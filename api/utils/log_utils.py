#
#  Copyright 2024 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import os
import os.path
import logging
import logging.handlers

def get_project_base_directory():
    PROJECT_BASE = os.path.abspath(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            os.pardir,
            os.pardir,
        )
    )
    return PROJECT_BASE

def initRootLogger(name):
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    log_file = os.path.join(log_dir, f"{name}.log")
    
    # 로그 로테이션 설정
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    
    # 로그 포맷 설정
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # 기존 핸들러 제거
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 새 핸들러 추가
    root_logger.addHandler(file_handler)
    
    # 콘솔 출력용 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    logging.captureWarnings(True)

    LOG_LEVELS = os.environ.get("LOG_LEVELS", "")
    pkg_levels = {}
    for pkg_name_level in LOG_LEVELS.split(","):
        terms = pkg_name_level.split("=")
        if len(terms)!= 2:
            continue
        pkg_name, pkg_level = terms[0], terms[1]
        pkg_name = pkg_name.strip()
        pkg_level = logging.getLevelName(pkg_level.strip().upper())
        if not isinstance(pkg_level, int):
            pkg_level = logging.INFO
        pkg_levels[pkg_name] = logging.getLevelName(pkg_level)

    for pkg_name in ['peewee', 'pdfminer']:
        if pkg_name not in pkg_levels:
            pkg_levels[pkg_name] = logging.getLevelName(logging.WARNING)
    if 'root' not in pkg_levels:
        pkg_levels['root'] = logging.getLevelName(logging.INFO)

    for pkg_name, pkg_level in pkg_levels.items():
        pkg_logger = logging.getLogger(pkg_name)
        pkg_logger.setLevel(pkg_level)

    msg = f"{name} log path: {log_file}, log levels: {pkg_levels}"
    logging.getLogger().info(msg)