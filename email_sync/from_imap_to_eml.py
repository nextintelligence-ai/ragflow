#!/usr/bin/env python3
import traceback
import imaplib
import email
import os
from datetime import datetime
import argparse
import asyncio
import aiofiles
import re
import logging
import sys
from dotenv import load_dotenv
import binascii
import time
import json
import signal
import atexit
import fcntl
import threading

# .env 파일 로드
load_dotenv()

class Timer:
    """작업 시간 측정을 위한 컨텍스트 매니저"""
    def __init__(self, description, logger):
        self.description = description
        self.logger = logger

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.duration = self.end - self.start
        self.logger.info(f"{self.description} 완료: {self.duration:.2f}초 소요")


def setup_logging(debug=False):
    """로깅 설정"""
    level = logging.DEBUG if debug else logging.INFO
    
    # logs 디렉토리 생성
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # 로그 파일명 설정 (현재 날짜 포함)
    current_date = datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(log_dir, f'imap_to_eml_{current_date}.log')
    
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
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    
    # 기존 핸들러 제거
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 새 핸들러 추가
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"로그 파일 위치: {log_file}")
    return logger

def connect_to_imap(host, username, auth_type="BASIC", **auth_params):
    """IMAP 서버에 연결"""
    logger = logging.getLogger(__name__)
    logger.info(f"IMAP 서버 연결 시도: {host}")
    
    try:
        imap = imaplib.IMAP4_SSL(host)
        
        if auth_type == "XOAUTH2":
            access_token = auth_params.get("access_token")
            if not access_token:
                raise Exception("OAuth2 인증에 필요한 access token이 없습니다.")
            
            auth_string = f"user={username}\x01auth=Bearer {access_token}\x01\x01"
            imap.authenticate('XOAUTH2', lambda x: auth_string)
            
        else:  # BASIC 인증
            password = auth_params.get("password")
            if not password:
                raise Exception("BASIC 인증에 필요한 password가 없습니다.")
            imap.login(username, password)
        
        # 연결 정보 저장
        imap.host = host
        imap._username = username
        imap._auth_params = auth_params
        imap._auth_type = auth_type
        
        logger.info("IMAP 서버 연결 성공")
        return imap
        
    except Exception as e:
        logger.error(f"IMAP 서버 연결 실패: {str(e)}")
        raise

def reconnect_imap(imap, folder_path):
    """IMAP 서버 재연결 및 폴더 선택"""
    logger = logging.getLogger(__name__)
    try:
        # SSL 연결
        new_imap = imaplib.IMAP4_SSL(imap.host)
        
        # 인증
        if imap._auth_type == "XOAUTH2":
            new_imap = connect_to_imap(
                host=imap.host,
                username=imap._username,
                auth_type="XOAUTH2",
                **imap._auth_params
            )
        else:
            new_imap = connect_to_imap(
                host=imap.host,
                username=imap._username,
                auth_type="BASIC",
                **imap._auth_params
            )
        
        # 폴더 선택
        encoded_folder_path = encode_imap_path(folder_path)
        status, _ = new_imap.select(encoded_folder_path)
        if status != 'OK':
            raise Exception(f"폴더 선택 실패: {folder_path}")
        return new_imap
        
    except Exception as e:
        logger.error(f"IMAP 서버 재연결 실패: {str(e)}")
        raise

def modified_utf7_decode(s):
    """IMAP modified UTF-7 디코딩"""
    logger = logging.getLogger(__name__)
    
    if not s:
        return s
    
    try:
        result = []
        i = 0
        while i < len(s):
            if s[i] == '&' and i + 1 < len(s):
                # 다음 '-' 찾기
                end = s.find('-', i + 1)
                if end == -1:
                    # '-'를 찾지 못한 경우 나머지 문자열을 그대로 추가
                    result.append(s[i:])
                    break
                
                if end == i + 1:
                    # '&-'는 '&'로 변환
                    result.append('&')
                else:
                    # Base64 디코딩
                    b64 = s[i+1:end]
                    # 패딩 추가
                    padding_length = len(b64) % 4
                    if padding_length:
                        b64 += '=' * (4 - padding_length)
                    try:
                        # Base64 디코딩 후 UTF-16BE로 디코딩
                        decoded = binascii.a2b_base64(b64).decode('utf-16be')
                        result.append(decoded)
                    except Exception as e:
                        logger.warning(f"부분 디코딩 실패 (무시됨): {b64}, 오류: {str(e)}")
                        result.append(s[i:end+1])
                i = end + 1
            else:
                result.append(s[i])
                i += 1
        
        decoded = ''.join(result)
        logger.debug(f"디코딩: {s} -> {decoded}")
        return decoded
    except Exception as e:
        logger.error(f"디코딩 실패: {s}, 오류: {str(e)}")
        return s

def modified_utf7_encode(s):
    """문자열을 IMAP modified UTF-7로 인코딩"""
    logger = logging.getLogger(__name__)
    
    if not s:
        return s
    
    # ASCII 범위 내의 문자만 있는지 확인 (공백 제외)
    if all(ord(c) < 128 for c in s):
        return s
    
    try:
        # 문자열을 UTF-16BE로 인코딩
        result = []
        buffer = []
        
        for c in s:
            # ASCII 문자나 공백은 그대로 사용
            if ord(c) < 128:
                if buffer:
                    # 버퍼에 있는 문자들을 인코딩
                    utf16be = ''.join(buffer).encode('utf-16be')
                    b64 = binascii.b2a_base64(utf16be).decode('ascii').rstrip('=\n')
                    result.append(f"&{b64}-")
                    buffer = []
                result.append(c)
            else:
                buffer.append(c)
        
        # 남은 버퍼 처리
        if buffer:
            utf16be = ''.join(buffer).encode('utf-16be')
            b64 = binascii.b2a_base64(utf16be).decode('ascii').rstrip('=\n')
            result.append(f"&{b64}-")
        
        encoded = ''.join(result)
        logger.debug(f"인코딩: {s} -> {encoded}")
        return encoded
        
    except Exception as e:
        logger.error(f"인코딩 실패: {s}, 오류: {str(e)}")
        raise

def encode_imap_path(path):
    """IMAP 경로의 각 부분을 UTF-7로 인코딩"""
    logger = logging.getLogger(__name__)
    try:
        if not path:
            return path
        
        parts = path.split('/')
        encoded_parts = []
        for part in parts:
            if not part:  # 빈 문자열 처리
                continue
            
            # 한글이나 특수문자가 포함된 경우에만 인코딩
            needs_encoding = any(ord(c) > 127 for c in part)
            
            if needs_encoding:
                try:
                    encoded_part = modified_utf7_encode(part)
                    logger.debug(f"폴더명 인코딩: {part} -> {encoded_part}")
                except Exception as e:
                    logger.error(f"폴더명 인코딩 실패: {part}, 오류: {str(e)}")
                    raise
            else:
                encoded_part = part
            
            encoded_parts.append(encoded_part)
        
        # 전체 경로를 따옴표로 묶음
        result = f'"{"/".join(encoded_parts)}"'
        logger.debug(f"IMAP 경로 인코딩 결과: {result} (원본: {path})")
        return result
    except Exception as e:
        logger.error(f"IMAP 경로 인코딩 실패: {path}, 오류: {str(e)}")
        raise

def decode_imap_path(path):
    """IMAP 경로 전체를 디코딩"""
    logger = logging.getLogger(__name__)
    
    if not path:
        return path
    
    try:
        # 경로를 부분으로 분리
        parts = path.split('/')
        decoded_parts = []
        
        for part in parts:
            if not part:  # 빈 문자열 처리
                continue
            
            # 각 부분을 디코딩
            decoded = modified_utf7_decode(part)
            decoded_parts.append(decoded)
            logger.debug(f"경로 부분 디코딩: {part} -> {decoded}")
        
        # 디코딩된 부분들을 결합
        result = '/'.join(decoded_parts)
        logger.debug(f"전체 경로 디코딩: {path} -> {result}")
        return result
    except Exception as e:
        logger.error(f"경로 디코딩 실패: {path}, 오류: {str(e)}")
        return path

def get_folder_list(imap):
    """IMAP 서버의 모든 폴더 목록 가져오기"""
    logger = logging.getLogger(__name__)
    logger.info("IMAP 폴더 목록 조회 시작")
    _, folders = imap.list()
    folder_paths = []
    
    logger.debug(f"발견된 전체 폴더 수: {len(folders)}")
    for folder in folders:
        try:
            # IMAP 응답 파싱
            decoded = folder.decode('utf-8')
            logger.debug(f"원본 IMAP 폴더 응답: {decoded}")
            
            # 폴더 플래그, 구분자, 폴더명 분리
            match = re.match(r'\((.*?)\) "(.*?)" (.+)', decoded)
            if match:
                flags, delimiter, folder_name = match.groups()
                logger.debug(f"파싱된 폴더 정보 - 플래그: {flags}, 구분자: {delimiter}, 폴더명: {folder_name}")
                
                # Special-use 플래그 확인
                is_trash = '\\Trash' in flags
                
                # 따옴표 제거 및 이스케이프된 문자 처리
                folder_name = folder_name.strip('"').replace('\\"', '"').replace('\\\\', '\\')
                
                # IMAP modified UTF-7 디코딩
                try:
                    decoded_name = decode_imap_path(folder_name)
                    logger.debug(f"디코딩된 폴더명: {decoded_name} (원본: {folder_name})")
                    
                    # Trash 폴더와 IMAP 특수 폴더 제외
                    if not is_trash and not decoded_name.startswith('['):
                        folder_paths.append(decoded_name)
                        logger.info(f"폴더 추가됨: {decoded_name}")
                    else:
                        logger.debug(f"제외된 폴더: {decoded_name} (플래그: {flags})")
                except Exception as e:
                    logger.error(f"폴더명 처리 실패: {folder_name}, 오류: {str(e)}")
                    continue
        except Exception as e:
            logger.error(f"폴더 처리 중 오류 발생: {str(e)}")
            continue
    
    logger.info(f"처리 가능한 폴더 수: {len(folder_paths)}")
    return folder_paths

def create_local_folders(base_dir, folder_paths):
    """로컬에 폴더 구조 생성"""
    logger = logging.getLogger(__name__)
    logger.info(f"로컬 폴더 구조 생성 시작: {base_dir}")
    
    created_folders = []
    skipped_folders = []
    for folder_path in folder_paths:
        try:
            # IMAP 구분자를 시스템 구분자로 변환
            local_path = folder_path.replace('"', '').replace('/', os.path.sep)
            full_path = os.path.join(base_dir, local_path)
            
            # 폴더가 이미 존재하는지 확인
            if os.path.exists(full_path):
                skipped_folders.append(folder_path)
                logger.info(f"이미 존재하는 폴더 건너뜀: {full_path}")
                continue
                
            os.makedirs(full_path)
            created_folders.append(folder_path)
            logger.info(f"폴더 생성됨: {full_path}")
        except Exception as e:
            logger.error(f"폴더 생성 실패: {full_path}, 오류: {str(e)}")
    
    logger.info(f"생성된 폴더 수: {len(created_folders)}, 건너뛴 폴더 수: {len(skipped_folders)}")
    return created_folders

def format_size(size_bytes):
    """바이트 크기를 사람이 읽기 쉬운 형식으로 변환"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f}TB"

class FolderStats:
    """폴더별 통계 정보"""
    def __init__(self):
        self.email_count = 0      # 실제 저장된 이메일 수
        self.total_size = 0
        self.attachment_count = 0
        self.attachment_size = 0
        self.failed_count = 0     # 실패한 이메일 수
        self.skipped_count = 0    # 건너뛴 이메일 수
        self.processed_count = 0  # 총 처리 시도한 이메일 수
        self.start_time = time.time()
    
    def add_email(self, size, attachment_count=0, attachment_size=0):
        self.email_count += 1
        self.total_size += size
        self.attachment_count += attachment_count
        self.attachment_size += attachment_size
        self.processed_count += 1
    
    def add_failure(self):
        self.failed_count += 1
        self.processed_count += 1
    
    def add_skipped(self):
        self.skipped_count += 1
        self.processed_count += 1
    
    def get_stats(self):
        elapsed = time.time() - self.start_time
        emails_per_second = self.email_count / elapsed if elapsed > 0 else 0
        return {
            'email_count': self.email_count,          # 실제 저장된 이메일 수
            'failed_count': self.failed_count,        # 실패한 이메일 수
            'skipped_count': self.skipped_count,      # 건너뛴 이메일 수
            'processed_count': self.processed_count,  # 총 처리 시도한 이메일 수
            'total_size': format_size(self.total_size),
            'attachment_count': self.attachment_count,
            'attachment_size': format_size(self.attachment_size),
            'elapsed': elapsed,
            'speed': emails_per_second
        }

def decode_header(header):
    """이메일 헤더를 안전하게 디코딩"""
    logger = logging.getLogger(__name__)
    
    if header is None:
        return 'no_subject'
    
    try:
        # email.header.Header 객체인 경우
        if isinstance(header, email.header.Header):
            try:
                # Header 객체를 직접 디코딩
                decoded_parts = []
                # Header 객체를 문자열로 변환하지 않고 직접 디코딩
                for part, charset in email.header.decode_header(header):
                    try:
                        if isinstance(part, bytes):
                            # 한국어 인코딩 처리
                            if charset and charset.lower() in ['cseuckr', 'euc-kr', 'ks_c_5601-1987']:
                                decoded = part.decode('euc-kr')
                            elif charset:
                                decoded = part.decode(charset)
                            else:
                                try:
                                    decoded = part.decode('utf-8')
                                except UnicodeDecodeError:
                                    try:
                                        decoded = part.decode('euc-kr')
                                    except UnicodeDecodeError:
                                        decoded = part.decode('utf-8', errors='replace')
                        else:
                            decoded = str(part)
                        decoded_parts.append(decoded)
                    except Exception as e:
                        logger.warning(f"헤더 부분 디코딩 실패 (무시됨): {str(e)}, charset: {charset}")
                        decoded_parts.append(str(part))
                return ''.join(decoded_parts)
            except Exception as e:
                # Header 객체 디코딩 실패 시 문자열로 변환
                logger.warning(f"Header 객체 디코딩 실패, 문자열로 처리: {str(e)}")
                return str(header)
        
        # 일반 문자열인 경우
        try:
            logger.debug(f"일반 문자열 디코딩 시작: {header}")
            decoded_parts = []
            
            # email.header.decode_header 호출 시도
            try:
                parts = email.header.decode_header(header)
                logger.debug(f"디코딩된 파트 수: {len(parts)}")
            except Exception as e:
                logger.error(f"decode_header 호출 실패: {str(e)}")
                return str(header)
            
            # 각 파트 처리
            for i, (part, charset) in enumerate(parts):
                try:
                    logger.debug(f"파트 {i+1} 처리 - 타입: {type(part)}, charset: {charset}")
                    if isinstance(part, bytes):
                        # 한국어 인코딩 처리
                        if charset and charset.lower() in ['cseuckr', 'euc-kr', 'ks_c_5601-1987']:
                            decoded = part.decode('euc-kr')
                            logger.debug(f"EUC-KR로 디코딩: {decoded}")
                        elif charset:
                            decoded = part.decode(charset)
                            logger.debug(f"{charset}로 디코딩: {decoded}")
                        else:
                            try:
                                decoded = part.decode('utf-8')
                                logger.debug("UTF-8로 디코딩 성공")
                            except UnicodeDecodeError:
                                try:
                                    decoded = part.decode('euc-kr')
                                    logger.debug("EUC-KR로 디코딩 성공")
                                except UnicodeDecodeError:
                                    decoded = part.decode('utf-8', errors='replace')
                                    logger.debug("UTF-8 (errors='replace')로 디코딩")
                    else:
                        decoded = str(part)
                        logger.debug(f"문자열 변환: {decoded}")
                    decoded_parts.append(decoded)
                except Exception as e:
                    logger.warning(f"파트 {i+1} 디코딩 실패: {str(e)}, charset: {charset}")
                    decoded_parts.append(str(part))
            
            result = ''.join(decoded_parts)
            logger.debug(f"최종 디코딩 결과: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"일반 문자열 처리 중 예외 발생: {str(e)}")
            return str(header)
            
    except Exception as e:
        logger.error(f"헤더 디코딩 실패: {str(e)}")
        return str(header)

async def save_email_to_eml(email_message, output_dir, folder_path, uid):
    """이메일을 EML 파일로 저장 (비동기)"""
    logger = logging.getLogger(__name__)
    temp_filepath = None
    folder_dir = None
    
    with Timer("이메일 저장", logger):
        try:
            # 이메일 날짜 추출
            date_str = 'unknown_date'
            date_tuple = email.utils.parsedate_tz(email_message['Date'])
            if date_tuple:
                try:
                    date = datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
                    date_str = date.strftime('%Y%m%d_%H%M%S')
                except Exception as e:
                    logger.warning(f"날짜 변환 실패: {str(e)}")
            
            # 제목 처리
            raw_subject = email_message.get('Subject', 'no_subject')
            safe_subject = 'no_subject'
            try:
                subject = decode_header(raw_subject)
                if subject:
                    safe_subject = "".join(x for x in subject if x.isalnum() or x in (' ', '-', '_'))[:50]
                    if not safe_subject.strip():
                        safe_subject = 'no_subject'
            except Exception as e:
                logger.error(f"제목 처리 실패: {str(e)}")
            
            # 파일 경로 설정
            filename = f"{date_str}_{safe_subject}_uid{uid}.eml"
            folder_path = folder_path.replace('"', '').replace('/', os.path.sep)
            folder_dir = os.path.join(output_dir, folder_path)
            filepath = os.path.join(folder_dir, filename)

            # 이미 파일이 존재하는지 확인
            if os.path.exists(filepath):
                logger.debug(f"이미 존재하는 파일 건너뜀: {filepath}")
                return filepath, 0, 0, 0, True

            # 이메일 크기 및 첨부파일 정보 계산
            email_bytes = email_message.as_bytes()
            email_size = len(email_bytes)
            attachment_count = 0
            attachment_size = 0
            
            # 첨부파일 정보 수집
            if email_message.is_multipart():
                for part in email_message.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    
                    try:
                        content_disp = part.get('Content-Disposition')
                        if isinstance(content_disp, email.header.Header):
                            content_disp = str(content_disp)
                        
                        if content_disp and 'attachment' in content_disp:
                            attachment_count += 1
                            try:
                                payload = part.get_payload(decode=True)
                                if payload is not None:
                                    attachment_size += len(payload)
                            except Exception as e:
                                logger.warning(f"첨부파일 크기 계산 실패: {str(e)}")
                    except Exception as e:
                        logger.warning(f"첨부파일 정보 처리 중 오류 발생: {str(e)}")
                    continue
            
            # 폴더 생성 시도 (최대 3번)
            for attempt in range(3):
                try:
                    os.makedirs(folder_dir, exist_ok=True)
                    if os.path.exists(folder_dir):
                        break
                except Exception as e:
                    if attempt == 2:  # 마지막 시도에서 실패
                        logger.error(f"폴더 생성 실패: {folder_dir}, 오류: {str(e)}")
                        raise
                    await asyncio.sleep(0.1)  # 잠시 대기 후 재시도
            
            temp_filepath = f"{filepath}.tmp"
            
            # 임시 파일로 저장 (최대 3번 시도)
            save_success = False
            for attempt in range(3):
                try:
                    # 임시 파일이 이미 존재하면 삭제
                    if os.path.exists(temp_filepath):
                        os.remove(temp_filepath)
                    
                    # 비동기로 파일 저장
                    async with aiofiles.open(temp_filepath, mode='wb') as f:
                        await f.write(email_bytes)
                        await f.flush()  # 버퍼 비우기
                    
                    # 저장된 파일 크기 확인
                    if not os.path.exists(temp_filepath):
                        logger.warning(f"임시 파일이 생성되지 않음 (시도 {attempt + 1})")
                        if attempt < 2:
                            await asyncio.sleep(0.1)
                            continue
                        else:
                            raise Exception("임시 파일 생성 실패")
                    
                    saved_size = os.path.getsize(temp_filepath)
                    if saved_size != email_size:
                        # 파일 내용 검증
                        async with aiofiles.open(temp_filepath, 'rb') as f:
                            saved_content = await f.read()
                        
                        # 줄바꿈 문자 차이로 인한 크기 불일치 확인
                        saved_content_normalized = saved_content.replace(b'\r\n', b'\n')
                        email_bytes_normalized = email_bytes.replace(b'\r\n', b'\n')
                        
                        if saved_content_normalized == email_bytes_normalized:
                            logger.info(f"줄바꿈 문자 차이로 인한 크기 불일치 무시: 예상={email_size}, 실제={saved_size}")
                            save_success = True
                            break
                        elif len(saved_content) >= len(email_bytes):
                            # 저장된 크기가 더 크지만 원본 내용을 모두 포함하고 있는 경우
                            if email_bytes in saved_content:
                                logger.info(f"추가 메타데이터로 인한 크기 차이 무시: 예상={email_size}, 실제={saved_size}")
                                save_success = True
                                break
                        
                        logger.warning(f"파일 크기 불일치 (시도 {attempt + 1}): 예상={email_size}, 실제={saved_size}")
                        if attempt < 2:
                            await asyncio.sleep(0.1)
                            continue
                    else:
                        save_success = True
                        break
                    
                except Exception as e:
                    logger.error(f"임시 파일 저장 실패 (시도 {attempt + 1}): {str(e)}")
                    if attempt < 2:
                        await asyncio.sleep(0.1)
                    else:
                        raise
            
            if not save_success:
                raise Exception(f"파일 저장 실패: 크기 불일치 또는 저장 오류 (최종 시도: 예상={email_size}, 실제={saved_size if 'saved_size' in locals() else 'unknown'})")
            
            # 임시 파일을 실제 파일로 이동
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
                os.rename(temp_filepath, filepath)
                logger.info(f"새로운 이메일 저장 성공: {filepath}")
                return filepath, email_size, attachment_count, attachment_size, False
            except Exception as move_error:
                logger.error(f"파일 이동 실패, 복사 시도: {str(move_error)}")
                # rename 실패 시 복사 후 삭제 시도
                async with aiofiles.open(temp_filepath, 'rb') as src, \
                          aiofiles.open(filepath, 'wb') as dst:
                    content = await src.read()
                    await dst.write(content)
                    await dst.flush()
                os.remove(temp_filepath)
                return filepath, email_size, attachment_count, attachment_size, False
                
        except Exception as e:
            logger.error(f"이메일 처리 중 오류 발생: {str(e)}")
            if temp_filepath and os.path.exists(temp_filepath):
                try:
                    os.remove(temp_filepath)
                except Exception:
                    pass
            raise

async def process_folder(imap, folder_path, output_dir, concurrent_limit, checkpoint_manager):
    """폴더 내 이메일 처리"""
    logger = logging.getLogger(__name__)
    stats = FolderStats()
    
    with Timer(f"폴더 처리: {folder_path}", logger):
        try:
            # 체크포인트 정보 로드
            checkpoint = checkpoint_manager.get_folder_checkpoint(folder_path)
            logger.debug(f"폴더 체크포인트 로드: {folder_path} -> {checkpoint}")
            
            if checkpoint["status"] == "completed":
                # 마지막 동기화 시간 이후의 이메일만 처리
                last_sync_time = checkpoint_manager.get_folder_sync_time(folder_path)
                logger.info(f"이전 동기화 완료됨, {last_sync_time} 이후의 새 이메일만 처리: {folder_path}")
            else:
                last_sync_time = None
                logger.info(f"처음부터 동기화 시작: {folder_path}")
            
            # 폴더명을 IMAP UTF-7로 인코딩
            encoded_folder_path = encode_imap_path(folder_path)
            logger.debug(f"인코딩된 폴더 경로: {encoded_folder_path} (원본: {folder_path})")
            
            # 폴더 선택
            status, folder_info = imap.select(encoded_folder_path)
            if status != 'OK':
                logger.error(f"폴더 선택 실패: {folder_path}, 상태: {status}, 정보: {folder_info}")
                process_folder.stats = stats
                process_folder.message_count = 0
                return
            
            try:
                # 이메일 검색 조건 설정
                if last_sync_time:
                    # 이미 존재하는 EML 파일 확인
                    existing_files = set()
                    folder_dir = os.path.join(output_dir, folder_path.replace('"', '').replace('/', os.path.sep))
                    if os.path.exists(folder_dir):
                        for file in os.listdir(folder_dir):
                            if file.endswith('.eml'):
                                uid_match = re.search(r'_uid(\d+)\.eml$', file)
                                if uid_match:
                                    existing_files.add(int(uid_match.group(1)))
                    
                    # RFC3501 날짜 형식으로 변환 (DD-Mon-YYYY)
                    search_date = last_sync_time.strftime("%d-%b-%Y")
                    search_cmd = f'(SINCE {search_date})'
                    logger.info(f"검색 조건: {search_cmd}")
                    status, message_data = imap.uid('search', None, search_cmd)
                    
                    if status == 'OK' and message_data[0]:
                        # 시간까지 정확하게 필터링하기 위해 각 메시지의 날짜 확인
                        message_uids = message_data[0].split()
                        filtered_uids = []
                        new_message_count = 0  # 실제 새로운 메일 수를 추적
                        
                        for uid in message_uids:
                            uid_int = int(uid.decode())
                            # 이미 처리된 UID이거나 EML 파일이 존재하는 경우 건너뜀
                            if uid_int <= checkpoint["last_uid"] or uid_int in existing_files:
                                logger.debug(f"이미 처리된 UID 제외: {uid_int}")
                                continue
                            
                            try:
                                # 메시지 헤더만 가져오기
                                _, msg_data = imap.uid('fetch', uid, '(BODY.PEEK[HEADER.FIELDS (DATE)])')
                                if msg_data and msg_data[0]:
                                    email_date_str = msg_data[0][1].decode()
                                    # 이메일 날짜 파싱
                                    email_date_match = re.search(r'Date: (.*?)\r\n', email_date_str)
                                    if email_date_match:
                                        email_date_str = email_date_match.group(1)
                                        try:
                                            # 이메일 날짜를 datetime으로 변환
                                            email_date_tuple = email.utils.parsedate_tz(email_date_str)
                                            if email_date_tuple:
                                                email_date = datetime.fromtimestamp(
                                                    email.utils.mktime_tz(email_date_tuple)
                                                )
                                                # 동기화 시작 시간 이후의 메시지만 포함
                                                if email_date >= last_sync_time:
                                                    filtered_uids.append(uid)
                                                    new_message_count += 1  # 실제 새로운 메일 수 증가
                                                    logger.debug(f"새로운 메일 발견 - UID: {uid_int}, 날짜: {email_date}")
                                                else:
                                                    logger.debug(f"시간 필터링으로 제외된 메시지 - UID: {uid_int}, 날짜: {email_date}")
                                        except Exception as e:
                                            logger.warning(f"이메일 날짜 파싱 실패 (UID: {uid_int}): {str(e)}")
                            except Exception as e:
                                logger.warning(f"메시지 헤더 조회 실패 (UID: {uid_int}): {str(e)}")
                        
                        message_uids = filtered_uids
                        logger.info(f"새로운 메일 발견: {new_message_count}통 (전체 검색 결과: {len(message_data[0].split())}통)")
                        
                        if new_message_count > 0:
                            # total_count는 실제 새로운 메일 수만큼만 증가
                            total_count = checkpoint["total_count"] + new_message_count
                            processed_count = 0
                        else:
                            logger.info(f"처리할 새 이메일 없음: {folder_path}")
                            checkpoint_manager.update_folder_sync_time(folder_path)
                            checkpoint_manager.mark_folder_complete(folder_path)
                            process_folder.stats = stats
                            process_folder.message_count = 0
                            return
                else:
                    status, message_data = imap.uid('search', None, 'ALL')
                
                if status != 'OK':
                    logger.error(f"이메일 검색 실패: {folder_path}, 상태: {status}")
                    process_folder.stats = stats
                    process_folder.message_count = 0
                    return
                
                if not message_data[0]:
                    logger.info(f"처리할 새 이메일 없음: {folder_path}")
                    checkpoint_manager.update_folder_sync_time(folder_path)
                    checkpoint_manager.mark_folder_complete(folder_path)
                    process_folder.stats = stats
                    process_folder.message_count = 0
                    return
                
                message_uids = message_data[0].split()
                message_count = len(message_uids)
                logger.info(f"처리할 이메일 수: {message_count} (폴더: {folder_path})")
                
                # 마지막으로 처리된 UID 이후부터 처리
                last_uid = checkpoint["last_uid"]
                
                # 새로운 동기화인 경우 processed_count는 0부터 시작
                if last_sync_time:
                    processed_count = 0
                    # total_count는 실제 새로운 메일 수만큼만 증가
                    total_count = checkpoint["total_count"] + message_count
                else:
                    # 초기 동기화 중인 경우
                    processed_count = checkpoint["processed_count"]
                    total_count = message_count
                
                if last_uid and not last_sync_time:  # 초기 동기화 중일 때만 UID 기반 필터링
                    try:
                        start_idx = message_uids.index(str(last_uid).encode())
                        message_uids = message_uids[start_idx + 1:]
                        logger.info(f"체크포인트에서 재시작: UID {last_uid} 이후부터 처리 (폴더: {folder_path})")
                    except ValueError:
                        logger.warning(f"마지막 UID {last_uid}를 찾을 수 없음, 처음부터 시작 (폴더: {folder_path})")
                        processed_count = 0  # UID를 찾지 못한 경우 처음부터 다시 시작
                
                tasks = []
                
                for uid in message_uids:
                    uid_str = uid.decode()
                    task = asyncio.create_task(process_email(uid_str, imap, output_dir, folder_path, stats, use_uid=True))
                    tasks.append((uid_str, task))  # UID와 task를 함께 저장
                    processed_count += 1
                    
                    # 체크포인트 주기적 업데이트
                    if len(tasks) >= concurrent_limit:
                        try:
                            # 각 작업의 결과 처리
                            for uid_str, task in tasks:
                                try:
                                    result = await task
                                    if result == "skipped":
                                        checkpoint_manager.update_folder_checkpoint(
                                            folder_path,
                                            int(uid_str),
                                            processed_count,
                                            total_count,
                                            skipped_uid=int(uid_str)
                                        )
                                    elif result == "failed":
                                        checkpoint_manager.update_folder_checkpoint(
                                            folder_path,
                                            int(uid_str),
                                            processed_count,
                                            total_count,
                                            failed_uid=int(uid_str)
                                        )
                                except Exception as e:
                                    logger.error(f"이메일 처리 실패 (UID: {uid_str}): {str(e)}")
                                    checkpoint_manager.update_folder_checkpoint(
                                        folder_path,
                                        int(uid_str),
                                        processed_count,
                                        total_count,
                                        failed_uid=int(uid_str)
                                    )
                            
                            # 마지막 처리된 UID로 체크포인트 업데이트
                            checkpoint_manager.update_folder_checkpoint(
                                folder_path,
                                int(tasks[-1][0]),  # 마지막 UID
                                processed_count,
                                total_count
                            )
                            logger.debug(f"체크포인트 업데이트 완료 - 폴더: {folder_path}, UID: {tasks[-1][0]}")
                        except Exception as e:
                            logger.error(f"일부 이메일 처리 실패: {str(e)}")
                        
                        folder_stats = stats.get_stats()
                        failed_uids = checkpoint_manager.get_failed_uids(folder_path)
                        skipped_uids = checkpoint_manager.get_skipped_uids(folder_path)
                        logger.info(
                            f"진행 상황 (폴더: {folder_path}): {processed_count}/{total_count} "
                            f"({(processed_count/total_count)*100:.1f}%) - "
                            f"처리 속도: {folder_stats['speed']:.1f}통/초\n"
                            f"실패한 UID 수: {len(failed_uids)}, 건너뛴 UID 수: {len(skipped_uids)}"
                        )
                        tasks = []
                
                if tasks:
                    try:
                        # 남은 작업 처리
                        for uid_str, task in tasks:
                            try:
                                result = await task
                                if result == "skipped":
                                    checkpoint_manager.update_folder_checkpoint(
                                        folder_path,
                                        int(uid_str),
                                        processed_count,
                                        total_count,
                                        skipped_uid=int(uid_str)
                                    )
                                elif result == "failed":
                                    checkpoint_manager.update_folder_checkpoint(
                                        folder_path,
                                        int(uid_str),
                                        processed_count,
                                        total_count,
                                        failed_uid=int(uid_str)
                                    )
                            except Exception as e:
                                logger.error(f"이메일 처리 실패 (UID: {uid_str}): {str(e)}")
                                checkpoint_manager.update_folder_checkpoint(
                                    folder_path,
                                    int(uid_str),
                                    processed_count,
                                    total_count,
                                    failed_uid=int(uid_str)
                                )
                        
                        # 마지막 UID로 체크포인트 업데이트
                        if tasks:
                            last_uid = int(tasks[-1][0])
                            checkpoint_manager.update_folder_checkpoint(
                                folder_path,
                                last_uid,
                                processed_count,
                                total_count
                            )
                    except Exception as e:
                        logger.error(f"일부 이메일 처리 실패: {str(e)}")
                
                # 폴더 처리 완료 표시
                checkpoint_manager.mark_folder_complete(folder_path)
                checkpoint_manager.update_folder_sync_time(folder_path)  # 동기화 시간 업데이트
                logger.debug(f"폴더 처리 완료 표시됨: {folder_path}")
                
                final_stats = stats.get_stats()
                failed_uids = checkpoint_manager.get_failed_uids(folder_path)
                skipped_uids = checkpoint_manager.get_skipped_uids(folder_path)
                success_rate = (final_stats['email_count'] / final_stats['processed_count'] * 100) if final_stats['processed_count'] > 0 else 0
                
                # 동기화 히스토리 출력
                sync_history = checkpoint_manager.get_folder_sync_history(folder_path)
                if sync_history:
                    logger.info("\n=== 동기화 히스토리 ===")
                    for idx, history in enumerate(sync_history, 1):
                        logger.info(
                            f"동기화 #{idx} ({history['timestamp']})\n"
                            f"- 이전 총 메일 수: {history['previous_total']}통\n"
                            f"- 새로운 메일 수: {history['new_messages']}통\n"
                            f"- 처리된 메일 수: {history['processed_count']}통\n"
                            f"- 마지막 UID: {history['last_uid']}\n"
                            f"- 실패한 UID 수: {len(history['failed_uids'])}\n"
                            f"- 건너뛴 UID 수: {len(history['skipped_uids'])}\n"
                        )
                
                logger.info(
                    f"\n=== 현재 동기화 결과 ===\n"
                    f"폴더 처리 완료: {folder_path}\n"
                    f"- 총 이메일: {total_count}통\n"
                    f"- 성공: {total_count - final_stats['failed_count'] - final_stats['skipped_count']}통\n"
                    f"- 실패: {final_stats['failed_count']}통 (성공률: {success_rate:.1f}%)\n"
                    f"- 건너뜀: {final_stats['skipped_count']}통\n"
                    f"- 총 용량: {final_stats['total_size']}\n"
                    f"- 첨부파일: {final_stats['attachment_count']}개 ({final_stats['attachment_size']})\n"
                    f"- 평균 처리 속도: {final_stats['speed']:.1f}통/초\n"
                    f"- 실패한 UID 목록: {failed_uids}\n"
                    f"- 건너뛴 UID 목록: {skipped_uids}"
                )
                
                process_folder.stats = stats
                process_folder.message_count = message_count
                
            finally:
                try:
                    imap.close()
                except Exception as e:
                    logger.debug(f"폴더 선택 해제 중 오류 (무시됨): {str(e)}")
                
        except Exception as e:
            logger.error(f"폴더 처리 실패: {folder_path}, 오류: {str(e)}")
            process_folder.stats = stats
            process_folder.message_count = 0

async def process_email(num, imap, output_dir, folder_path, stats, use_uid=False):
    """단일 이메일 처리 (비동기)"""
    logger = logging.getLogger(__name__)
    max_retries = 3
    retry_count = 0
    current_imap = imap
    
    while retry_count < max_retries:
        try:
            # FLAGS도 함께 가져오도록 수정
            if use_uid:
                _, msg_data = current_imap.uid('fetch', num, '(RFC822.PEEK FLAGS)')
            else:
                _, msg_data = current_imap.fetch(num, '(RFC822.PEEK FLAGS)')
                
            if not msg_data or not msg_data[0]:
                logger.error(f"이메일 데이터 없음: {num}")
                stats.add_failure()
                return "failed"
                
            email_body = msg_data[0][1]
            
            # FLAGS 정보 추출
            flags = None
            for item in msg_data:
                if isinstance(item, tuple) and b'FLAGS' in item[0]:
                    flag_match = re.search(rb'FLAGS \((.*?)\)', item[0])
                    if flag_match:
                        flags = flag_match.group(1).decode('utf-8')
                        logger.debug(f"추출된 FLAGS: {flags}")
                    break
            
            email_message = email.message_from_bytes(email_body)
            
            # FLAGS 정보가 있다면 X-IMAP-Flags 헤더에 추가
            if flags:
                email_message.add_header('X-IMAP-Flags', flags)
                logger.debug(f"X-IMAP-Flags 헤더 추가됨: {flags}")
            
            filepath, email_size, attachment_count, attachment_size, already_exists = await save_email_to_eml(
                email_message, output_dir, folder_path, num
            )
            
            if not already_exists:
                stats.add_email(email_size, attachment_count, attachment_size)
                logger.debug(f"이메일 처리 완료: {filepath}")
                return "success"
            else:
                stats.add_skipped()
                return "skipped"
                
        except Exception as e:
            error_str = str(e)
            if "command FETCH illegal in state LOGOUT" in error_str and retry_count < max_retries - 1:
                logger.warning(f"IMAP 연결이 끊김. 재연결 시도 ({retry_count + 1}/{max_retries})...")
                try:
                    current_imap = reconnect_imap(current_imap, folder_path)
                    retry_count += 1
                    continue
                except Exception as reconnect_error:
                    logger.error(f"재연결 실패: {str(reconnect_error)}")
            
            stats.add_failure()
            logger.error(f"이메일 처리 실패 (최대 재시도 횟수 초과) - 폴더: {folder_path}, {'UID' if use_uid else '번호'}: {num}, 오류: {error_str}")
        
        retry_count += 1
    
    return "failed"

class TotalStats:
    """전체 통계 정보"""
    def __init__(self):
        self.total_emails = 0        # 처리 시도한 총 이메일 수
        self.success_count = 0       # 실제 저장된 이메일 수
        self.failed_count = 0        # 실패한 이메일 수
        self.skipped_count = 0       # 건너뛴 이메일 수
        self.total_size = 0
        self.attachment_count = 0
        self.attachment_size = 0
    
    def add_folder_stats(self, folder_stats, message_count):
        """폴더 통계 정보를 전체 통계에 추가"""
        self.total_emails += folder_stats['processed_count']  # 처리 시도한 이메일 수로 변경
        self.success_count += folder_stats['email_count']     # 실제 저장된 이메일 수
        self.failed_count += folder_stats['failed_count']
        self.skipped_count += folder_stats['skipped_count']
        
        # 크기 정보는 문자열로 되어있으므로 숫자로 변환
        def parse_size(size_str):
            try:
                value = float(size_str[:-2])
                unit = size_str[-2:]
                multiplier = {
                    'B ': 1,
                    'KB': 1024,
                    'MB': 1024 * 1024,
                    'GB': 1024 * 1024 * 1024,
                    'TB': 1024 * 1024 * 1024 * 1024
                }.get(unit, 1)
                return int(value * multiplier)
            except:
                return 0
        
        self.total_size += parse_size(folder_stats['total_size'])
        self.attachment_count += folder_stats['attachment_count']
        self.attachment_size += parse_size(folder_stats['attachment_size'])
    
    def get_stats(self):
        """전체 통계 정보 반환"""
        # 성공률은 실제 저장된 이메일 수 기준으로 계산
        success_rate = (self.success_count / self.total_emails * 100) if self.total_emails > 0 else 0
        return {
            'total_emails': self.total_emails,
            'success_count': self.success_count,
            'failed_count': self.failed_count,
            'skipped_count': self.skipped_count,
            'success_rate': success_rate,
            'total_size': format_size(self.total_size),
            'attachment_count': self.attachment_count,
            'attachment_size': format_size(self.attachment_size)
        }

class CheckpointManager:
    """체크포인트 관리 클래스"""
    def __init__(self, account, checkpoint_file='imap_checkpoint.json'):
        self.account = account
        self.checkpoint_file = checkpoint_file
        self.lock_file = f"{self.checkpoint_file}.lock"
        self._lock_file = None
        self._remove_stale_lock()  # 오래된 락 파일 제거
        self.checkpoint_data = self._load_checkpoint()
        # 메인 스레드에서만 signal 핸들러 등록
        if threading.current_thread() is threading.main_thread():
            self._register_handlers()
        
        # 동기화 시작 시간 설정 (없으면 현재 시간으로)
        if "sync_start_time" not in self.checkpoint_data[self.account]:
            self.checkpoint_data[self.account]["sync_start_time"] = datetime.now().isoformat()
            self._save_checkpoint()
    
    def _remove_stale_lock(self):
        """오래된 락 파일 제거"""
        try:
            if os.path.exists(self.lock_file):
                # 락 파일이 존재하면 삭제
                os.remove(self.lock_file)
                logging.warning(f"오래된 락 파일 제거됨: {self.lock_file}")
        except Exception as e:
            logging.error(f"락 파일 제거 실패: {str(e)}")
    
    def _acquire_lock(self):
        """파일 잠금 획득"""
        try:
            self._lock_file = open(self.lock_file, 'w')
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (IOError, OSError) as e:
            if self._lock_file:
                self._lock_file.close()
                self._lock_file = None
            logging.error(f"락 획득 실패: {str(e)}")
            return False
    
    def _release_lock(self):
        """파일 잠금 해제"""
        try:
            if self._lock_file:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                self._lock_file.close()
                self._lock_file = None
                if os.path.exists(self.lock_file):
                    os.remove(self.lock_file)
        except Exception as e:
            logging.error(f"락 해제 실패: {str(e)}")
    
    def __del__(self):
        """소멸자에서 락 해제 보장"""
        self._release_lock()
    
    def _load_checkpoint(self):
        """체크포인트 파일 로드"""
        try:
            self._acquire_lock()
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
            else:
                data = {}
            
            # 계정이 없으면 초기화
            if self.account not in data:
                data[self.account] = {
                    "last_update": datetime.now().isoformat(),
                    "sync_start_time": datetime.now().isoformat(),
                    "folders": {}
                }
            
            # 각 폴더에 필드가 없으면 추가
            for folder in data[self.account]["folders"].values():
                if "failed_uids" not in folder:
                    folder["failed_uids"] = []
                if "skipped_uids" not in folder:
                    folder["skipped_uids"] = []
                if "last_sync_time" not in folder:
                    folder["last_sync_time"] = data[self.account].get("sync_start_time", datetime.now().isoformat())
                if "sync_history" not in folder:
                    folder["sync_history"] = []
            
            return data
            
        except Exception as e:
            logging.error(f"체크포인트 로드 실패: {str(e)}")
            current_time = datetime.now().isoformat()
            return {
                self.account: {
                    "last_update": current_time,
                    "sync_start_time": current_time,
                    "folders": {}
                }
            }
        finally:
            self._release_lock()
    
    def _save_checkpoint(self):
        """체크포인트 파일 저장"""
        logger = logging.getLogger(__name__)
        temp_file = None
        
        try:
            if not self._acquire_lock():
                logger.error("체크포인트 저장을 위한 락 획득 실패")
                return
            
            # 현재 계정의 last_update 갱신
            self.checkpoint_data[self.account]["last_update"] = datetime.now().isoformat()
            
            # 임시 파일에 먼저 저장
            temp_file = f"{self.checkpoint_file}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.checkpoint_data, f, indent=2, ensure_ascii=False)
            
            # 임시 파일을 실제 파일로 이동 (atomic operation)
            os.replace(temp_file, self.checkpoint_file)
            logger.debug(f"체크포인트 저장 완료: {self.checkpoint_file}")
            
        except Exception as e:
            logger.error(f"체크포인트 저장 실패: {str(e)}")
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as cleanup_error:
                    logger.error(f"임시 파일 정리 실패: {str(cleanup_error)}")
        finally:
            self._release_lock()
    
    def _register_handlers(self):
        """시그널 핸들러 등록"""
        def save_handler(signum, frame):
            self._save_checkpoint()
            if signum != 0:  # 0은 정상 종료
                sys.exit(1)
        
        # 정상 종료 시
        atexit.register(lambda: save_handler(0, None))
        # 비정상 종료 시
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, save_handler)
            signal.signal(signal.SIGTERM, save_handler)
    
    def _normalize_folder_path(self, folder_path):
        """폴더 경로 정규화"""
        # 따옴표 제거
        path = folder_path.strip('"')
        # 경로 구분자 통일
        path = path.replace(os.path.sep, '/')
        return path
    
    def get_folder_checkpoint(self, folder_path):
        """폴더의 체크포인트 정보 조회"""
        normalized_path = self._normalize_folder_path(folder_path)
        logger = logging.getLogger(__name__)
        logger.debug(f"체크포인트 조회 - 계정: {self.account}, 원본 경로: {folder_path}, 정규화된 경로: {normalized_path}")
        
        checkpoint = self.checkpoint_data[self.account]["folders"].get(normalized_path, {
            "last_uid": 0,
            "processed_count": 0,
            "total_count": 0,
            "status": "not_started",
            "failed_uids": [],  # 실패한 UID 목록
            "skipped_uids": [],  # 건너뛴 UID 목록
            "last_sync_time": self.checkpoint_data[self.account]["sync_start_time"]
        })
        logger.debug(f"체크포인트 데이터: {checkpoint}")
        return checkpoint
    
    def update_folder_checkpoint(self, folder_path, uid, processed_count, total_count, status="in_progress", failed_uid=None, skipped_uid=None):
        """폴더의 체크포인트 정보 업데이트"""
        normalized_path = self._normalize_folder_path(folder_path)
        logger = logging.getLogger(__name__)
        logger.debug(f"체크포인트 업데이트 - 계정: {self.account}, 경로: {normalized_path}, UID: {uid}, 처리: {processed_count}/{total_count}")
        
        # 폴더 정보가 없으면 초기화
        if normalized_path not in self.checkpoint_data[self.account]["folders"]:
            self.checkpoint_data[self.account]["folders"][normalized_path] = {
                "last_uid": 0,
                "processed_count": 0,
                "total_count": 0,
                "status": "not_started",
                "failed_uids": [],
                "skipped_uids": [],
                "last_sync_time": self.checkpoint_data[self.account]["sync_start_time"],
                "sync_history": []  # 동기화 히스토리 초기화
            }
        
        folder_data = self.checkpoint_data[self.account]["folders"][normalized_path]
        
        # 이전 상태가 completed이고 새로운 동기화가 시작되면 히스토리에 기록
        if folder_data["status"] == "completed" and status == "in_progress":
            # 실제 새로운 메일 수 계산
            new_messages = total_count - folder_data["total_count"]
            if new_messages > 0:
                # 이전 동기화 정보를 히스토리에 추가
                history_entry = {
                    "timestamp": datetime.now().isoformat(),
                    "previous_total": folder_data["total_count"],
                    "new_messages": new_messages,  # 실제 새로운 메일 수
                    "last_uid": folder_data["last_uid"],
                    "processed_count": folder_data["processed_count"],
                    "failed_uids": folder_data["failed_uids"].copy(),
                    "skipped_uids": folder_data["skipped_uids"].copy()
                }
                folder_data["sync_history"].append(history_entry)
                logger.info(f"동기화 히스토리 추가 - 폴더: {normalized_path}\n"
                           f"- 이전 총 메일 수: {history_entry['previous_total']}\n"
                           f"- 새로운 메일 수: {history_entry['new_messages']}\n"
                           f"- 처리된 메일 수: {history_entry['processed_count']}\n"
                           f"- 마지막 UID: {history_entry['last_uid']}\n"
                           f"- 실패한 UID 수: {len(history_entry['failed_uids'])}\n"
                           f"- 건너뛴 UID 수: {len(history_entry['skipped_uids'])}\n"
                )
            
            folder_data["processed_count"] = processed_count
            folder_data["total_count"] = total_count
        else:
            # 기존 처리 방식 유지
            folder_data.update({
                "last_uid": uid,
                "processed_count": processed_count,
                "total_count": total_count,
                "status": status
            })
        
        # 실패한 UID 추가
        if failed_uid is not None and failed_uid not in folder_data["failed_uids"]:
            folder_data["failed_uids"].append(failed_uid)
            logger.debug(f"실패한 UID 추가: {failed_uid} (폴더: {normalized_path})")
        
        # 건너뛴 UID 추가
        if skipped_uid is not None and skipped_uid not in folder_data["skipped_uids"]:
            folder_data["skipped_uids"].append(skipped_uid)
            logger.debug(f"건너뛴 UID 추가: {skipped_uid} (폴더: {normalized_path})")
        
        self._save_checkpoint()
        logger.debug(f"체크포인트 저장됨: {folder_data}")
    
    def mark_folder_complete(self, folder_path):
        """폴더 처리 완료 표시"""
        normalized_path = self._normalize_folder_path(folder_path)
        logger = logging.getLogger(__name__)
        
        if normalized_path in self.checkpoint_data[self.account]["folders"]:
            folder_data = self.checkpoint_data[self.account]["folders"][normalized_path]
            folder_data["status"] = "completed"
            failed_count = len(folder_data.get("failed_uids", []))
            logger.debug(f"폴더 완료 표시 - 계정: {self.account}, 폴더: {normalized_path}, 실패 UID 수: {failed_count}")
            self._save_checkpoint()
        else:
            logger.warning(f"완료 표시 실패: 폴더를 찾을 수 없음 - 계정: {self.account}, 폴더: {normalized_path}")
    
    def get_failed_uids(self, folder_path):
        """폴더의 실패한 UID 목록 조회"""
        normalized_path = self._normalize_folder_path(folder_path)
        folder_data = self.checkpoint_data[self.account]["folders"].get(normalized_path, {})
        return folder_data.get("failed_uids", [])

    def get_skipped_uids(self, folder_path):
        """폴더의 건너뛴 UID 목록 조회"""
        normalized_path = self._normalize_folder_path(folder_path)
        folder_data = self.checkpoint_data[self.account]["folders"].get(normalized_path, {})
        return folder_data.get("skipped_uids", [])

    def get_sync_start_time(self):
        """동기화 시작 시간 조회"""
        return datetime.fromisoformat(self.checkpoint_data[self.account]["sync_start_time"])
    
    def update_folder_sync_time(self, folder_path):
        """폴더의 마지막 동기화 시간 업데이트"""
        normalized_path = self._normalize_folder_path(folder_path)
        folder_data = self.checkpoint_data[self.account]["folders"].get(normalized_path, {})
        folder_data["last_sync_time"] = datetime.now().isoformat()
        self._save_checkpoint()
    
    def get_folder_sync_time(self, folder_path):
        """폴더의 마지막 동기화 시간 조회"""
        normalized_path = self._normalize_folder_path(folder_path)
        folder_data = self.checkpoint_data[self.account]["folders"].get(normalized_path, {})
        sync_time = folder_data.get("last_sync_time", self.checkpoint_data[self.account]["sync_start_time"])
        return datetime.fromisoformat(sync_time)

    def get_folder_sync_history(self, folder_path):
        """폴더의 동기화 히스토리 조회"""
        normalized_path = self._normalize_folder_path(folder_path)
        folder_data = self.checkpoint_data[self.account]["folders"].get(normalized_path, {})
        return folder_data.get("sync_history", [])

    def has_checkpoint(self):
        """체크포인트 파일 존재 여부와 계정 데이터 존재 여부 확인"""
        # 1. 체크포인트 파일이 존재하는지 확인
        if not os.path.exists(self.checkpoint_file):
            return False
            
        # 2. 체크포인트 데이터가 비어있지 않은지 확인
        if not self.checkpoint_data:
            return False
            
        # 3. 해당 이메일 계정의 데이터가 존재하는지 확인
        if self.account not in self.checkpoint_data:
            return False
            
        # 4. 계정의 폴더 데이터가 존재하는지 확인
        account_data = self.checkpoint_data[self.account]
        if not account_data.get("folders"):
            return False
            
        # 모든 조건을 만족하면 True 반환
        return True


async def main():
    parser = argparse.ArgumentParser(description='IMAP 서버에서 이메일을 EML 파일로 다운로드')
    parser.add_argument('--host', default=os.getenv('IMAP_HOST'), help='IMAP 서버 주소')
    parser.add_argument('--username', default=os.getenv('IMAP_USERNAME'), help='이메일 계정')
    parser.add_argument('--password', default=os.getenv('IMAP_PASSWORD'), help='비밀번호')
    parser.add_argument('--output-dir', default='eml_files', help='EML 파일 저장 디렉토리 (기본값: eml_files)')
    parser.add_argument('--concurrent-limit', type=int, default=10, help='동시 처리할 최대 이메일 수 (기본값: 10)')
    parser.add_argument('--debug', action='store_true', help='디버그 모드 활성화')
    parser.add_argument('--folders', nargs='+', help='처리할 특정 폴더 목록 (지정하지 않으면 모든 폴더 처리)')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logger = setup_logging(args.debug)
    logger.info("프로그램 시작")
    
    # 필수 인자 확인
    if not all([args.host, args.username, args.password]):
        logger.error("IMAP 서버 정보가 필요합니다. 명령행 인자나 .env 파일을 통해 제공해주세요.")
        sys.exit(1)
    
    # 사용자명을 기반으로 출력 디렉토리 설정
    output_dir = os.path.join(args.output_dir, args.username)
    logger.info(f"이메일 저장 경로: {output_dir}")
    
    total_start_time = time.time()
    total_stats = TotalStats()
    
    try:
        # 체크포인트 매니저 초기화 (한 번만 생성)
        checkpoint_manager = CheckpointManager(args.username)
        
        # IMAP 서버 연결
        with Timer("IMAP 서버 연결", logger):
            imap = connect_to_imap(args.host, args.username, args.password)
        
        # 모든 폴더 목록 가져오기
        with Timer("폴더 목록 조회", logger):
            all_folder_paths = get_folder_list(imap)
            
        # 지정된 폴더만 처리
        if args.folders:
            folder_paths = []
            for folder in args.folders:
                matching_folders = [f for f in all_folder_paths if f.lower() == folder.lower()]
                if matching_folders:
                    folder_paths.extend(matching_folders)
                else:
                    logger.warning(f"지정한 폴더를 찾을 수 없음: {folder}")
            if not folder_paths:
                logger.error("처리할 폴더가 없습니다.")
                sys.exit(1)
            logger.info(f"처리할 폴더: {folder_paths}")
        else:
            folder_paths = all_folder_paths
            logger.info("모든 폴더를 처리합니다.")
        
        # 로컬에 폴더 구조 생성
        with Timer("로컬 폴더 구조 생성", logger):
            create_local_folders(output_dir, folder_paths)
        
        # 각 폴더 처리
        for folder_path in folder_paths:
            await process_folder(imap, folder_path, output_dir, args.concurrent_limit, checkpoint_manager)
            if hasattr(process_folder, 'stats'):
                folder_stats = process_folder.stats.get_stats()
                total_stats.add_folder_stats(folder_stats, process_folder.message_count)
        
        imap.logout()
        
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
        
        total_time = time.time() - total_start_time
        logger.info(f"프로그램 정상 종료 - 총 소요 시간: {total_time:.2f}초")
        
    except Exception as e:
        total_time = time.time() - total_start_time
        logger.error(f"프로그램 실행 중 오류 발생: {str(e)}")
        logger.error(f"중단까지 소요 시간: {total_time:.2f}초")
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main()) 