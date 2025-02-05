#!/bin/bash

# 프로세스 ID를 저장할 변수
SERVER_PID=""

# 종료 시그널 처리 함수
cleanup() {
    echo "서버를 안전하게 종료합니다..."
    if [ ! -z "$SERVER_PID" ]; then
        kill -TERM "$SERVER_PID"
        wait "$SERVER_PID"
    fi
    exit 0
}

# SIGTERM과 SIGINT(Ctrl+C) 시그널 처리
trap cleanup SIGTERM SIGINT

# 가상환경 활성화
source .venv/bin/activate

# Poetry 설정
export POETRY_VIRTUALENVS_CREATE=true
export POETRY_VIRTUALENVS_IN_PROJECT=true

# Python 경로 설정
export PYTHONPATH=${PWD}

# 서버 실행 (백그라운드에서)
python api/ragflow_server.py &
SERVER_PID=$!

# 서버 프로세스가 종료될 때까지 대기
wait "$SERVER_PID"