#!/usr/bin/env python3
import os
import sys
import logging
import time
import asyncio
import threading
import json
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

from api.db.services.email_service import EmailAccountService
from email_sync.sync_emails import sync_all_accounts

def setup_logging():
    """로깅 설정"""
    # logs 디렉토리 생성
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # 로그 파일명 설정 (현재 날짜 포함)
    current_date = datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(log_dir, f'email_sync_server_{current_date}.log')
    
    # 로그 포맷 설정
    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    formatter = logging.Formatter(log_format)
    
    # 파일 핸들러 설정
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # 콘솔 핸들러 설정
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # 로거 설정
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 기존 핸들러 제거
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 새 핸들러 추가
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"로그 파일 위치: {log_file}")
    return logger

# 로깅 설정
logger = setup_logging()

# Flask 앱 생성
app = Flask(__name__)

def acquire_sync_lock(account_id):
    """계정별 동기화 락 획득"""
    lock_dir = "email_sync_locks"
    os.makedirs(lock_dir, exist_ok=True)
    lock_file = os.path.join(lock_dir, f"{account_id}.lock")
    
    try:
        if os.path.exists(lock_file):
            try:
                with open(lock_file, 'r') as f:
                    lock_data = json.load(f)
                    start_time = datetime.fromisoformat(lock_data['start_time'])
                    if (datetime.now() - start_time).total_seconds() > 600:
                        os.remove(lock_file)
                    else:
                        return False
            except (json.JSONDecodeError, KeyError, ValueError):
                os.remove(lock_file)
        
        with open(lock_file, 'w') as f:
            lock_data = {
                'start_time': datetime.now().isoformat(),
                'account_id': account_id
            }
            json.dump(lock_data, f)
        return True
        
    except Exception as e:
        logger.error(f"락 획득 실패: {str(e)}")
        return False

def release_sync_lock(account_id):
    """계정별 동기화 락 해제"""
    lock_file = os.path.join("email_sync_locks", f"{account_id}.lock")
    try:
        if os.path.exists(lock_file):
            os.remove(lock_file)
    except Exception as e:
        logger.error(f"락 해제 실패: {str(e)}")

def run_email_sync():
    """이메일 동기화 스레드"""
    while True:
        try:
            accounts = EmailAccountService.get_active_accounts()
            if not accounts:
                logger.info("동기화할 계정이 없습니다.")
                time.sleep(60)
                continue
            
            for account in accounts:
                try:
                    if not acquire_sync_lock(account['id']):
                        logger.info(f"계정 {account['email']}의 동기화가 이미 진행 중입니다. 건너뜁니다.")
                        continue
                    
                    try:
                        logger.info(f"계정 동기화 시작: {account['email']}")
                        asyncio.run(sync_all_accounts([account]))
                    finally:
                        release_sync_lock(account['id'])
                    
                except Exception as e:
                    logger.error(f"계정 {account['email']} 동기화 중 오류 발생: {str(e)}")
                    release_sync_lock(account['id'])
            
        except Exception as e:
            logger.error(f"이메일 동기화 중 오류 발생: {str(e)}")
        
        time.sleep(60)

@app.route('/health')
def health_check():
    """헬스 체크 엔드포인트"""
    return jsonify({
        'status': 'healthy',
        'timestamp': time.time()
    }), 200

@app.route('/sync/status')
def sync_status():
    """동기화 상태 확인 엔드포인트"""
    lock_dir = "email_sync_locks"
    status = {}
    
    if os.path.exists(lock_dir):
        for lock_file in os.listdir(lock_dir):
            if lock_file.endswith('.lock'):
                try:
                    with open(os.path.join(lock_dir, lock_file), 'r') as f:
                        lock_data = json.load(f)
                        account_id = lock_data['account_id']
                        start_time = datetime.fromisoformat(lock_data['start_time'])
                        status[account_id] = {
                            'syncing': True,
                            'start_time': start_time.isoformat()
                        }
                except Exception as e:
                    logger.error(f"락 파일 읽기 실패: {str(e)}")
    
    return jsonify({
        'sync_status': status,
        'timestamp': time.time()
    }), 200

def main():
    """메인 함수"""
    # 데이터베이스 초기화
    from api.db.db_models import init_database_tables
    init_database_tables()
    
    # 동기화 스레드 시작
    sync_thread = threading.Thread(target=run_email_sync, daemon=True)
    sync_thread.start()
    
    # Flask 서버 시작
    logger.info("이메일 동기화 서버 시작...")
    app.run(
        host='0.0.0.0',
        port=5001,  # RAGFlow 서버와 다른 포트 사용
        threaded=True
    )

if __name__ == '__main__':
    main() 