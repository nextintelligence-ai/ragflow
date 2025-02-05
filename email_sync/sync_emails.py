#!/usr/bin/env python3
import os
import sys
import asyncio
import logging
from datetime import datetime
from api.db.services.knowledgebase_service import KnowledgebaseService
from api.db.services.file_service import FileService
from api.db.services.file2document_service import File2DocumentService
from api.db.services.task_service import queue_tasks
from api.db.services.document_service import DocumentService
import tempfile
import shutil
from pathlib import Path

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

from api.db.services.email_service import EmailAccountService
from email_sync.from_imap_to_eml import connect_to_imap, get_folder_list, process_folder, CheckpointManager, Timer, TotalStats, create_local_folders
from rag.utils.storage_factory import STORAGE_IMPL


def setup_logging():
    """로깅 설정"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('email_sync.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)

async def sync_account(account, temp_dir, logger):
    """단일 계정의 이메일 동기화"""
    try:
        # IMAP 서버 연결
        auth_params = {}
        if account["auth_type"] == "XOAUTH2":
            auth_params = {
                "oauth_refresh_token": account["oauth_refresh_token"],
                "oauth_client_id": account["oauth_client_id"],
                "oauth_client_secret": account["oauth_client_secret"]
            }
        
        imap = connect_to_imap(
            account["imap_host"],
            account["email"],
            account.get("password"),
            auth_type=account["auth_type"],
            **auth_params
        )
        
        # 체크포인트 매니저 초기화
        checkpoint_manager = CheckpointManager(account["email"])
        
        # 모든 폴더 목록 가져오기
        folder_paths = get_folder_list(imap)
        
        # 각 폴더 처리
        for folder_path in folder_paths:
            try:
                # 폴더별 임시 디렉토리 생성
                folder_temp_dir = os.path.join(temp_dir, account["email"], folder_path.replace('"', '').replace('/', os.path.sep))
                os.makedirs(folder_temp_dir, exist_ok=True)
                
                # 폴더 처리
                await process_folder(imap, folder_path, folder_temp_dir, 10, checkpoint_manager)
                
                # 처리된 이메일들을 스토리지에 업로드
                for root, _, files in os.walk(folder_temp_dir):
                    for file in files:
                        if not file.endswith('.eml'):
                            continue
                            
                        local_path = os.path.join(root, file)
                        relative_path = os.path.relpath(local_path, folder_temp_dir)
                        storage_path = f"data/{account['user_id']}/{folder_path}/{relative_path}"
                        
                        # 파일 업로드
                        with open(local_path, 'rb') as f:
                            STORAGE_IMPL.upload(storage_path, f)
                        logger.info(f"Uploaded: {storage_path}")
                
            except Exception as e:
                logger.error(f"폴더 처리 중 오류 발생: {folder_path}, {str(e)}")
                continue
                
        # 동기화 시간 업데이트
        EmailAccountService.update_sync_time(account["id"])
        imap.logout()
        
    except Exception as e:
        logger.error(f"계정 동기화 중 오류 발생: {account['email']}, {str(e)}")

async def main():
    """메인 동기화 함수"""
    logger = setup_logging()
    logger.info("이메일 동기화 시작")
    
    try:
        # 활성화된 모든 이메일 계정 조회
        accounts = EmailAccountService.get_active_accounts()
        if not accounts:
            logger.info("동기화할 계정이 없습니다.")
            return
            
        # 임시 디렉토리 생성
        with tempfile.TemporaryDirectory() as temp_dir:
            # 모든 계정 동기화
            tasks = [sync_account(account, temp_dir, logger) for account in accounts]
            await asyncio.gather(*tasks)
            
        logger.info("이메일 동기화 완료")
        
    except Exception as e:
        logger.error(f"동기화 중 오류 발생: {str(e)}")
        sys.exit(1)

async def sync_all_accounts(accounts):
    """모든 이메일 계정 동기화"""
    logger = logging.getLogger(__name__)
    total_stats = TotalStats()
    
    for account in accounts:
        try:
            logger.info(f"계정 동기화 시작: {account['email']}")
            
            # 체크포인트 매니저 초기화
            checkpoint_manager = CheckpointManager(account['email'])
            
            # IMAP 서버 연결
            with Timer(f"IMAP 서버 연결 ({account['email']})", logger):
                auth_params = {}
                if account['auth_type'] == "XOAUTH2":
                    auth_params['access_token'] = account['access_token']
                    
                    imap = connect_to_imap(
                        account['imap_host'],
                        account['email'],
                        auth_type=account['auth_type'],
                        **auth_params
                    )
                else:
                    auth_params['password'] = account['password']
                
                    imap = connect_to_imap(
                        account['imap_host'],
                        account['email'],
                        auth_type=account['auth_type'],
                        **auth_params
                    )
            
            # 출력 디렉토리 설정
            output_dir = os.path.join('eml_files', account['email'])
            
            # 폴더 목록 가져오기
            with Timer("폴더 목록 조회", logger):
                folder_paths = get_folder_list(imap)
                logger.info(f"처리할 폴더 수: {len(folder_paths)}")
            
            # 로컬 폴더 생성
            with Timer("로컬 폴더 구조 생성", logger):
                create_local_folders(output_dir, folder_paths)
                
            # kb id 얻어오기
            kb_name = "emails"
            e, kb = KnowledgebaseService.get_by_name(kb_name, account['user_id'])
            
            if kb is None:
                logger.error(f"KB {kb_name} 없음")
                continue
            
            kb_id = kb.id
            
            # 각 폴더 처리
            for folder_path in folder_paths:
                await process_folder(imap, folder_path, output_dir, 10, checkpoint_manager)
                if hasattr(process_folder, 'stats'):
                    folder_stats = process_folder.stats.get_stats()
                    total_stats.add_folder_stats(folder_stats, process_folder.message_count)
                
                # 처리된 이메일들을 스토리지에 업로드
                try:
                    folder_dir = os.path.join(output_dir, folder_path.replace('"', '').replace('/', os.path.sep))
                    if os.path.exists(folder_dir):
                        for root, _, files in os.walk(folder_dir):
                            for file in files:
                                if not file.endswith('.eml'):
                                    continue
                                    
                                file_path = os.path.join(root, file)
                                
                                # File 객체 생성
                                from werkzeug.datastructures import FileStorage
                                with open(file_path, 'rb') as f:
                                    file_obj = FileStorage(
                                        stream=f,
                                        filename=os.path.basename(file_path)
                                    )
                                    err, doc = FileService.upload_document(kb, [file_obj], account['user_id'])
                                    if err:
                                        logger.error(f"파일 업로드 실패: {kb_id}, 오류: {str(err)}")
                                        continue
                                    
                                    doc_id = doc[0][0].get('id')
                                        
                                    # 문서 정보 가져오기
                                    e, doc = DocumentService.get_by_id(doc_id)
                                    if not e:
                                        logger.error(f"문서를 찾을 수 없음: {doc_id}")
                                        continue
                                        
                                    # 문서 처리를 위한 태스크 추가
                                    doc_dict = doc.to_dict()
                                    doc_dict["tenant_id"] = account['user_id']
                                    bucket, name = File2DocumentService.get_storage_address(doc_id=doc.id)
                                    queue_tasks(doc_dict, bucket, name)
                                    logger.info(f"태스크 큐에 추가됨: {doc.id}")
                except Exception as e:
                    logger.error(f"폴더 업로드 중 오류 발생: {folder_path}, 오류: {str(e)}")
            
            imap.logout()
            
            # 동기화 시간 업데이트
            EmailAccountService.update_sync_time(account['id'])
            
            # 임시 디렉토리 정리
            # try:
            #     if os.path.exists(output_dir):
            #         shutil.rmtree(output_dir)
            #         logger.info(f"임시 디렉토리 삭제 완료: {output_dir}")
            # except Exception as cleanup_error:
            #     logger.error(f"임시 디렉토리 정리 실패: {output_dir}, 오류: {str(cleanup_error)}")
            
        except Exception as e:
            logger.error(f"계정 {account['email']} 동기화 중 오류 발생: {str(e)}")
            continue
    
    # 전체 통계 출력
    final_stats = total_stats.get_stats()
    logger.info(
        f"\n=== 전체 처리 결과 ===\n"
        f"- 총 이메일: {final_stats['total_emails']}통\n"
        f"- 성공: {final_stats['success_count']}통\n"
        f"- 실패: {final_stats['failed_count']}통\n"
        f"- 건너뜀: {final_stats['skipped_count']}통\n"
        f"- 성공률: {final_stats['success_rate']:.1f}%\n"
        f"- 총 처리 용량: {final_stats['total_size']}\n"
        f"- 총 첨부파일: {final_stats['attachment_count']}개 ({final_stats['attachment_size']})"
    )

if __name__ == '__main__':
    asyncio.run(main())