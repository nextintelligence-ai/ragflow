#!/bin/bash

# 가상환경 활성화
source .venv/bin/activate

# 환경 변수 설정
export PYTHONPATH=${PWD}
export POETRY_VIRTUALENVS_CREATE=true
export POETRY_VIRTUALENVS_IN_PROJECT=true
export CONSUMER_NO=1

# 프로세스 ID를 저장할 배열
declare -a worker_pids

# SIGTERM 시그널 핸들러 함수
cleanup() {
    echo "Gracefully shutting down workers..."
    # 모든 worker 프로세스에 SIGTERM 전달
    for pid in "${worker_pids[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            echo "Sending SIGTERM to worker process $pid"
            kill -TERM $pid
        fi
    done
    
    # worker들이 종료될 때까지 대기 (최대 30초)
    timeout=30
    while [ $timeout -gt 0 ] && [ ${#worker_pids[@]} -gt 0 ]; do
        for pid in "${worker_pids[@]}"; do
            if ! kill -0 $pid 2>/dev/null; then
                # 종료된 프로세스는 배열에서 제거
                worker_pids=("${worker_pids[@]/$pid}")
            fi
        done
        [ ${#worker_pids[@]} -eq 0 ] && break
        sleep 1
        ((timeout--))
    done

    # 시간 초과 후에도 남아있는 프로세스는 강제 종료
    if [ ${#worker_pids[@]} -gt 0 ]; then
        echo "Force killing remaining processes..."
        for pid in "${worker_pids[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                kill -9 $pid
            fi
        done
    fi

    echo "Shutdown complete"
    exit 0
}

# SIGTERM과 SIGINT(Ctrl+C) 시그널 처리 등록
trap cleanup SIGTERM SIGINT

# 실행할 worker의 수를 인자로 받습니다
if [ $# -eq 0 ]; then
    echo "Usage: $0 <number_of_workers>"
    exit 1
fi

num_workers=$1

# 각 worker를 백그라운드에서 실행
for ((i=0; i<num_workers; i++))
do
    echo "Starting worker $i"
    python rag/svr/task_executor.py $i &
    worker_pids+=($!)
done

echo "All workers started. Press Ctrl+C to gracefully shutdown."
echo "Worker PIDs: ${worker_pids[*]}"

# 무한 대기 (시그널이 오기를 기다림)
while true; do
    sleep 1
done 