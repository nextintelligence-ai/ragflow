#!/bin/bash

# 프로세스 ID를 저장할 파일
PID_FILE=".email_sync.pid"

# SIGTERM 시그널 처리 함수
cleanup() {
    echo "서버를 종료합니다..."
    if [ -f "$PID_FILE" ]; then
        pid=$(cat "$PID_FILE")
        kill -TERM "$pid" 2>/dev/null
        rm -f "$PID_FILE"
    fi
    exit 0
}

# SIGTERM 시그널 트랩
trap cleanup SIGTERM SIGINT

# 가상환경 활성화
source .venv/bin/activate

# 환경변수 설정
export PYTHONPATH=${PWD}
export POETRY_VIRTUALENVS_CREATE=true
export POETRY_VIRTUALENVS_IN_PROJECT=true

# 이메일 동기화 서버 실행
python email_sync/sync_server.py &

# 백그라운드 프로세스의 PID 저장
echo $! > "$PID_FILE"

# 백그라운드 프로세스가 종료될 때까지 대기
wait