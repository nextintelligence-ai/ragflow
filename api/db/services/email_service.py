from api.db.db_models import EmailAccount, DB
from api.db.services.common_service import CommonService
from datetime import datetime

class EmailAccountService(CommonService):
    model = EmailAccount

    @classmethod
    @DB.connection_context()
    def get_active_accounts(cls):
        """활성화된 모든 이메일 계정 조회"""
        accounts = cls.model.select().where(
            cls.model.status == "1"
        )
        return [account.to_dict() for account in accounts]

    @classmethod
    @DB.connection_context()
    def get_user_accounts(cls, user_id):
        """사용자의 이메일 계정 조회"""
        return list(cls.model.select().where(
            (cls.model.user_id == user_id) &
            (cls.model.status == "1")
        ).dicts())

    @classmethod
    @DB.connection_context()
    def update_sync_time(cls, account_id):
        """동기화 시간 업데이트"""
        return cls.model.update(
            last_sync_time=datetime.now(),
            updated_at=datetime.now()
        ).where(cls.model.id == account_id).execute()