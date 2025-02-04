from flask import Blueprint, request
from flask_login import login_required, current_user
from api.db.services.email_service import EmailAccountService
from api.db.services.user_service import UserService
from api.utils.api_utils import get_json_result, get_data_error_result, server_error_response, get_uuid, validate_request
from werkzeug.security import generate_password_hash
import base64
import re
import imaplib
import requests
from datetime import datetime, timedelta


def validate_imap_connection(host, port, email, auth_type, **auth_params):
    """IMAP 연결 및 인증 검증"""
    try:
        imap = imaplib.IMAP4_SSL(host, port)
        
        if auth_type == "XOAUTH2":
            access_token = auth_params.get("access_token")
            if not access_token:
                return False, "OAuth2 인증에 필요한 access token이 없습니다."
            
            auth_string = f"user={email}\x01auth=Bearer {access_token}\x01\x01"
            imap.authenticate('XOAUTH2', lambda x: auth_string)
            
        else:  # BASIC 인증
            password = auth_params.get("password")
            if not password:
                return False, "비밀번호가 필요합니다."
            imap.login(email, password)
        
        imap.logout()
        return True, "연결 성공"
        
    except imaplib.IMAP4.error as e:
        return False, f"IMAP 인증 실패: {str(e)}"
    except Exception as e:
        return False, f"연결 실패: {str(e)}"

@manager.route('/account/create', methods=['POST'])
@validate_request("email", "imap_host", "auth_type")
def create_email_account():
    """이메일 계정 생성 API"""
    try:
        data = request.json
        email = data["email"]
        imap_host = data["imap_host"]
        imap_port = data.get("imap_port", 993)
        auth_type = data.get("auth_type", "BASIC").upper()

        # 이메일 형식 검증
        if not re.match(r"^[\w\._-]+@([\w_-]+\.)+[\w-]{2,4}$", email):
            return get_data_error_result(message="유효하지 않은 이메일 주소입니다.")

        # 인증 타입 검증
        if auth_type not in ["BASIC", "XOAUTH2"]:
            return get_data_error_result(message="지원하지 않는 인증 방식입니다.")

        # 이미 등록된 이메일인지 확인
        existing_account = EmailAccountService.query(email=email)
        if existing_account:
            return get_data_error_result(message="이미 등록된 이메일 계정입니다.")

        # 인증 정보 준비
        auth_params = {}
        if auth_type == "BASIC":
            if "password" not in data:
                return get_data_error_result(message="비밀번호가 필요합니다.")
            auth_params["password"] = data["password"]
        else:  # XOAUTH2
            if "access_token" not in data:
                return get_data_error_result(message="OAuth2 인증에 필요한 access token이 없습니다.")
            auth_params["access_token"] = data["access_token"]

        # IMAP 연결 테스트
        success, message = validate_imap_connection(
            imap_host, imap_port, email, auth_type, **auth_params
        )
        if not success:
            return get_data_error_result(message=message)

        # 이메일 주소로 사용자 조회
        user = UserService.query(email=email)
        
        # 사용자가 없으면 새로 생성
        if not user:
            nickname = email.split('@')[0]
            password = auth_params.get("password", get_uuid())  # OAuth2의 경우 임의 비밀번호 생성
            encoded_password = base64.b64encode(password.encode('utf-8')).decode('utf-8')
            password_hash = generate_password_hash(encoded_password)
            
            user_id = get_uuid()
            user_data = {
                "id": user_id,
                "email": email,
                "nickname": nickname,
                "password": password_hash,
                "status": "1"
            }
            UserService.save(**user_data)
            user = [type('User', (), {'id': user_id})]

        # 이메일 계정 생성
        account_data = {
            "id": get_uuid(),
            "user_id": user[0].id,
            "email": email,
            "imap_host": imap_host,
            "imap_port": imap_port,
            "auth_type": auth_type,
            "status": "1"
        }
        account_data.update(auth_params)  # 인증 관련 파라미터 추가

        EmailAccountService.save(**account_data)
        return get_json_result(data=True, message="이메일 계정이 성공적으로 등록되었습니다.")

    except Exception as e:
        return server_error_response(e)

@manager.route('/account/list', methods=['GET'])
def list_email_accounts():
    """사용자의 이메일 계정 목록 조회 API"""
    try:
        accounts = EmailAccountService.get_user_accounts(current_user.id)
        # 보안을 위해 민감한 정보 제거
        for account in accounts:
            account.pop("password", None)
            account.pop("oauth_refresh_token", None)
            account.pop("oauth_client_secret", None)
        return get_json_result(data=accounts)
    except Exception as e:
        return server_error_response(e)

@manager.route('/account/<account_id>', methods=['DELETE'])
def delete_email_account(account_id):
    """이메일 계정 삭제 API"""
    try:
        account = EmailAccountService.get_by_id(account_id)
        if not account:
            return get_data_error_result(message="존재하지 않는 계정입니다.")

        if account.user_id != current_user.id:
            return get_data_error_result(message="해당 계정을 삭제할 권한이 없습니다.")

        EmailAccountService.update_by_id(account_id, {"status": "0"})
        return get_json_result(data=True, message="계정이 성공적으로 삭제되었습니다.")

    except Exception as e:
        return server_error_response(e)

@manager.route('/account/<account_id>', methods=['PUT'])
def update_email_account(account_id):
    """이메일 계정 정보 업데이트 API"""
    try:
        data = request.json
        account = EmailAccountService.get_by_id(account_id)
        if not account:
            return get_data_error_result(message="존재하지 않는 계정입니다.")

        if account.user_id != current_user.id:
            return get_data_error_result(message="해당 계정을 수정할 권한이 없습니다.")

        # 업데이트 가능한 필드
        allowed_fields = {
            "password", "imap_host", "imap_port", "status",
            "auth_type", "access_token"
        }
        update_data = {k: v for k, v in data.items() if k in allowed_fields}

        if "auth_type" in update_data:
            auth_type = update_data["auth_type"].upper()
            if auth_type not in ["BASIC", "XOAUTH2"]:
                return get_data_error_result(message="지원하지 않는 인증 방식입니다.")
            update_data["auth_type"] = auth_type

        if update_data:
            # IMAP 연결 테스트
            test_data = account.to_dict()
            test_data.update(update_data)
            
            success, message = validate_imap_connection(
                test_data["imap_host"],
                test_data["imap_port"],
                test_data["email"],
                test_data["auth_type"],
                **test_data
            )
            if not success:
                return get_data_error_result(message=message)

            EmailAccountService.update_by_id(account_id, update_data)
            return get_json_result(data=True, message="계정 정보가 성공적으로 업데이트되었습니다.")
        else:
            return get_data_error_result(message="업데이트할 정보가 없습니다.")

    except Exception as e:
        return server_error_response(e) 