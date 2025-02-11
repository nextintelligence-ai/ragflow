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

# from beartype import BeartypeConf
# from beartype.claw import beartype_all  # <-- you didn't sign up for this
# beartype_all(conf=BeartypeConf(violation_type=UserWarning))    # <-- emit warnings from all code
import random
import sys
from api.utils.log_utils import initRootLogger
from graphrag.utils import get_llm_cache, set_llm_cache, get_tags_from_cache, set_tags_to_cache

CONSUMER_NO = "0" if len(sys.argv) < 2 else sys.argv[1]
CONSUMER_NAME = "task_executor_" + CONSUMER_NO
initRootLogger(CONSUMER_NAME)

import logging
import os
from datetime import datetime
import json
import xxhash
import copy
import re
import time
import threading
from functools import partial
from io import BytesIO
from multiprocessing.context import TimeoutError
from timeit import default_timer as timer
import tracemalloc
import resource
import psutil
from functools import lru_cache
import gc
import signal

import numpy as np
from peewee import DoesNotExist

from api.db import LLMType, ParserType, TaskStatus
from api.db.services.dialog_service import keyword_extraction, question_proposal, content_tagging
from api.db.services.document_service import DocumentService
from api.db.services.llm_service import LLMBundle
from api.db.services.task_service import TaskService
from api.db.services.file2document_service import File2DocumentService
from api import settings
from api.versions import get_ragflow_version
from api.db.db_models import close_connection
from rag.app import laws, paper, presentation, manual, qa, table, book, resume, picture, naive, one, audio, \
    knowledge_graph, email, tag
from rag.nlp import search, rag_tokenizer
from rag.raptor import RecursiveAbstractiveProcessing4TreeOrganizedRetrieval as Raptor
from rag.settings import DOC_MAXIMUM_SIZE, SVR_QUEUE_NAME, print_rag_settings, TAG_FLD, PAGERANK_FLD
from rag.utils import num_tokens_from_string
from rag.utils.redis_conn import REDIS_CONN, Payload
from rag.utils.storage_factory import STORAGE_IMPL

BATCH_SIZE = 64

FACTORY = {
    "general": naive,
    ParserType.NAIVE.value: naive,
    ParserType.PAPER.value: paper,
    ParserType.BOOK.value: book,
    ParserType.PRESENTATION.value: presentation,
    ParserType.MANUAL.value: manual,
    ParserType.LAWS.value: laws,
    ParserType.QA.value: qa,
    ParserType.TABLE.value: table,
    ParserType.RESUME.value: resume,
    ParserType.PICTURE.value: picture,
    ParserType.ONE.value: one,
    ParserType.AUDIO.value: audio,
    ParserType.EMAIL.value: email,
    ParserType.KG.value: knowledge_graph,
    ParserType.TAG.value: tag
}

CONSUMER_NAME = "task_consumer_" + CONSUMER_NO
PAYLOAD: Payload | None = None
BOOT_AT = datetime.now().astimezone().isoformat(timespec="milliseconds")
PENDING_TASKS = 0
LAG_TASKS = 0

mt_lock = threading.Lock()
DONE_TASKS = 0
FAILED_TASKS = 0
CURRENT_TASK = None

# 전역 변수로 스레드 객체 저장
background_thread = None

class TaskCanceledException(Exception):
    def __init__(self, msg):
        self.msg = msg


def set_progress(task_id, from_page=0, to_page=-1, prog=None, msg="Processing..."):
    global PAYLOAD
    if prog is not None and prog < 0:
        msg = "[ERROR]" + msg
    try:
        cancel = TaskService.do_cancel(task_id)
    except DoesNotExist:
        logging.warning(f"set_progress task {task_id} is unknown")
        if PAYLOAD:
            PAYLOAD.ack()
            PAYLOAD = None
        return

    if cancel:
        msg += " [Canceled]"
        prog = -1

    if to_page > 0:
        if msg:
            msg = f"Page({from_page + 1}~{to_page + 1}): " + msg
    if msg:
        msg = datetime.now().strftime("%H:%M:%S") + " " + msg
    d = {"progress_msg": msg}
    if prog is not None:
        d["progress"] = prog

    logging.info(f"set_progress({task_id}), progress: {prog}, progress_msg: {msg}")
    try:
        TaskService.update_progress(task_id, d)
    except DoesNotExist:
        logging.warning(f"set_progress task {task_id} is unknown")
        if PAYLOAD:
            PAYLOAD.ack()
            PAYLOAD = None
        return

    close_connection()
    if cancel and PAYLOAD:
        PAYLOAD.ack()
        PAYLOAD = None
        raise TaskCanceledException(msg)


def collect():
    global CONSUMER_NAME, PAYLOAD, DONE_TASKS, FAILED_TASKS
    try:
        PAYLOAD = REDIS_CONN.get_unacked_for(CONSUMER_NAME, SVR_QUEUE_NAME, "rag_flow_svr_task_broker")
        if not PAYLOAD:
            PAYLOAD = REDIS_CONN.queue_consumer(SVR_QUEUE_NAME, "rag_flow_svr_task_broker", CONSUMER_NAME)
        if not PAYLOAD:
            time.sleep(1)
            return None
    except Exception:
        logging.exception("Get task event from queue exception")
        return None

    msg = PAYLOAD.get_message()
    if not msg:
        return None

    task = None
    canceled = False
    try:
        task = TaskService.get_task(msg["id"])
        if task:
            _, doc = DocumentService.get_by_id(task["doc_id"])
            canceled = doc.run == TaskStatus.CANCEL.value or doc.progress < 0
    except DoesNotExist:
        pass
    except Exception:
        logging.exception("collect get_task exception")
    if not task or canceled:
        state = "is unknown" if not task else "has been cancelled"
        with mt_lock:
            DONE_TASKS += 1
        logging.info(f"collect task {msg['id']} {state}")
        return None

    if msg.get("type", "") == "raptor":
        task["task_type"] = "raptor"
    return task


class StreamingBuffer:
    """진정한 스트리밍 방식으로 파일을 처리하는 버퍼 클래스"""
    
    def __init__(self, bucket, name, chunk_size=1024*1024):
        self.bucket = bucket
        self.name = name
        self.chunk_size = chunk_size
        self._buffer = BytesIO()
        self._position = 0
        self._file_size = None
        self._eof = False
        self._init_file_size()
        self._load_chunk()
        self._all_data = None  # 전체 데이터를 저장할 변수
    
    def _init_file_size(self):
        """파일 크기를 초기화합니다."""
        try:
            # 먼저 get_size 메서드 존재 여부 확인
            if hasattr(STORAGE_IMPL, 'get_size'):
                self._file_size = STORAGE_IMPL.get_size(self.bucket, self.name)
                return
                
            # get_size가 없는 경우 stat 또는 head 메서드 시도
            if hasattr(STORAGE_IMPL, 'stat'):
                stat = STORAGE_IMPL.stat(self.bucket, self.name)
                if hasattr(stat, 'size'):
                    self._file_size = stat.size
                    return
                    
            if hasattr(STORAGE_IMPL, 'head'):
                head = STORAGE_IMPL.head(self.bucket, self.name)
                if hasattr(head, 'content_length'):
                    self._file_size = head.content_length
                    return
                    
            # 위 방법들이 모두 실패하면 전체 파일을 한 번 읽어서 크기 확인
            logging.warning("File size detection methods not available, calculating size manually")
            temp_data = STORAGE_IMPL.get(self.bucket, self.name)
            if isinstance(temp_data, bytes):
                self._file_size = len(temp_data)
            elif isinstance(temp_data, BytesIO):
                temp_data.seek(0, 2)  # 끝으로 이동
                self._file_size = temp_data.tell()
            else:
                self._file_size = 0
                logging.error(f"Unexpected data type: {type(temp_data)}")
                
        except Exception as e:
            logging.error(f"Error getting file size: {str(e)}")
            self._file_size = 0
    
    def _load_chunk(self):
        """청크 단위로 파일을 로드합니다."""
        try:
            if self._eof or (self._file_size is not None and self._position >= self._file_size):
                return False
                
            try:
                # offset과 length 파라미터 지원 확인
                chunk = STORAGE_IMPL.get(self.bucket, self.name, 
                                       offset=self._position, 
                                       length=self.chunk_size)
            except TypeError:
                # offset과 length를 지원하지 않는 경우
                if self._position == 0:
                    chunk = STORAGE_IMPL.get(self.bucket, self.name)
                else:
                    return False
                
            if not chunk:
                self._eof = True
                return False
                
            if isinstance(chunk, bytes):
                self._buffer = BytesIO(chunk)
            elif isinstance(chunk, BytesIO):
                self._buffer = chunk
            else:
                raise TypeError(f"Unexpected chunk type: {type(chunk)}")
                
            self._position += self.chunk_size
            return True
            
        except Exception as e:
            logging.error(f"Error loading chunk: {str(e)}")
            self._eof = True
            return False
    
    def read(self, size=None):
        """스트리밍 방식으로 데이터를 읽습니다."""
        if size is None:
            # 전체 파일을 읽어야 하는 경우
            if self._all_data is None:
                all_data = BytesIO()
                while True:
                    chunk = self._buffer.read()
                    if chunk:
                        all_data.write(chunk)
                    if not self._load_chunk():
                        break
                self._all_data = all_data.getvalue()
            return self._all_data
            
        data = self._buffer.read(size)
        if not data and not self._eof and self._load_chunk():
            data = self._buffer.read(size)
        return data
    
    def seek(self, offset, whence=0):
        """파일 포인터를 이동합니다."""
        if whence == 0:
            self._position = offset
        elif whence == 1:
            self._position += offset
        elif whence == 2:
            if self._file_size is not None:
                self._position = self._file_size + offset
            else:
                raise IOError("File size unknown, cannot seek from end")
        
        self._eof = False
        self._load_chunk()
        return self._position
    
    def tell(self):
        """현재 파일 포인터 위치를 반환합니다."""
        return self._position + self._buffer.tell()
    
    def getvalue(self):
        """전체 데이터를 bytes로 반환합니다."""
        return self.read()
        
    def get_size(self):
        """파일 크기를 반환합니다."""
        return self._file_size if self._file_size is not None else 0
        
    # bytes-like 객체 인터페이스 구현
    def __bytes__(self):
        """bytes 타입으로 변환"""
        return self.read()


def get_storage_binary(bucket, name):
    return STORAGE_IMPL.get(bucket, name)


def get_storage_binary_stream(bucket, name, chunk_size=1024*1024):
    """스트리밍 방식으로 스토리지에서 파일을 읽습니다.
    
    Args:
        bucket: 버킷 이름
        name: 파일 이름
        chunk_size: 청크 크기 (기본값: 1MB)
        
    Yields:
        bytes: 파일의 청크 데이터
    """
    try:
        binary = STORAGE_IMPL.get(bucket, name)
        if isinstance(binary, bytes):
            for i in range(0, len(binary), chunk_size):
                yield binary[i:i + chunk_size]
        elif isinstance(binary, BytesIO):
            while True:
                chunk = binary.read(chunk_size)
                if not chunk:
                    break
                yield chunk
        else:
            raise TypeError(f"Unexpected binary type: {type(binary)}")
    except Exception as e:
        logging.error(f"Error reading file {name} from bucket {bucket}: {str(e)}")
        raise


def build_chunks(task, progress_callback):
    if task["size"] > DOC_MAXIMUM_SIZE:
        set_progress(task["id"], prog=-1, msg="File size exceeds( <= %dMb )" %
                                              (int(DOC_MAXIMUM_SIZE / 1024 / 1024)))
        return []

    chunker = FACTORY[task["parser_id"].lower()]
    try:
        st = timer()
        bucket, name = File2DocumentService.get_storage_address(doc_id=task["doc_id"])
        
        # 메모리 모니터링 시작
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024
        logging.info(f"Initial memory usage before processing: {initial_memory:.2f}MB")
        
        # 이메일 파서인 경우 전체 데이터를 한 번에 읽습니다
        if task["parser_id"].lower() == ParserType.EMAIL.value.lower():
            binary_data = STORAGE_IMPL.get(bucket, name)
            if isinstance(binary_data, BytesIO):
                binary_data = binary_data.getvalue()
        else:
            # 다른 파서의 경우 스트리밍 버퍼 사용
            binary_data = StreamingBuffer(bucket, name, chunk_size=256*1024)  # 256KB 청크
        
        logging.info("Starting file processing from minio: {}/{}".format(task["location"], task["name"]))
        
        # 기본 문서 정보 설정
        base_doc = {
            "doc_id": task["doc_id"],
            "kb_id": str(task["kb_id"]),
            "docnm_kwd": task["name"],
            "title_tks": rag_tokenizer.tokenize(task["name"])
        }
        if task["pagerank"]:
            base_doc[PAGERANK_FLD] = int(task["pagerank"])
        
        # 청크 생성 및 처리
        for chunk in chunker.chunk(task["name"], binary=binary_data, from_page=task["from_page"],
                            to_page=task["to_page"], lang=task["language"], callback=progress_callback,
                            kb_id=task["kb_id"], parser_config=task["parser_config"], tenant_id=task["tenant_id"]):
            # 필수 필드 추가 (shallow copy 사용)
            chunk_doc = dict(base_doc)
            chunk_doc.update(chunk)
            
            # id 필드가 없는 경우 생성
            if "id" not in chunk_doc:
                chunk_doc["id"] = xxhash.xxh64(
                    (chunk_doc.get("content_with_weight", "") + str(chunk_doc["doc_id"])).encode("utf-8")
                ).hexdigest()
            
            # 생성 시간 필드 추가
            now = datetime.now()
            chunk_doc["create_time"] = now.strftime("%Y-%m-%d %H:%M:%S").strip()
            chunk_doc["create_timestamp_flt"] = now.timestamp()
            
            yield chunk_doc
            
            # 주기적으로 메모리 사용량 체크 및 정리
            current_memory = process.memory_info().rss / 1024 / 1024
            if current_memory - initial_memory > 500:  # 500MB 이상 증가시
                logging.warning(f"High memory increase detected: {current_memory-initial_memory:.2f}MB")
                gc.collect()  # 가비지 컬렉션 강제 실행
                
                # 메모리 상태 로깅
                gc_stats = gc.get_stats()
                logging.info(f"GC stats after collection: {gc_stats}")
            
        processing_time = timer() - st
        final_memory = process.memory_info().rss / 1024 / 1024
        memory_increase = final_memory - initial_memory
        
        logging.info(
            "File processing completed ({:.2f}s), Memory usage: {:.2f}MB ({:+.2f}MB) {}/{}".format(
                processing_time, final_memory, memory_increase,
                task["location"], task["name"]
            )
        )
            
    except TimeoutError:
        progress_callback(-1, "Internal server error: Fetch file from minio timeout. Could you try it again.")
        logging.exception(
            "Minio {}/{} got timeout: Fetch file from minio timeout.".format(task["location"], task["name"]))
        raise
    except Exception as e:
        if re.search("(No such file|not found)", str(e)):
            progress_callback(-1, "Can not find file <%s> from minio. Could you try it again?" % task["name"])
        else:
            progress_callback(-1, "Get file from minio: %s" % str(e).replace("'", ""))
        logging.exception("Chunking {}/{} got exception".format(task["location"], task["name"]))
        raise


def embedding(docs, mdl, parser_config=None, callback=None):
    if parser_config is None:
        parser_config = {}
    batch_size = min(8, max(1, len(docs) // 8))  # 배치 크기 축소
    
    def batch_generator(items, batch_size):
        batch = []
        for item in items:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
    
    tk_count = 0
    vector_size = 0
    
    try:
        # 메모리 최적화: 스트리밍 방식으로 처리
        for doc_batch in batch_generator(docs, batch_size):
            # 타이틀 임베딩
            titles = [d.get("docnm_kwd", "Title") for d in doc_batch]
            try:
                title_embeddings, title_tokens = mdl.encode(titles)
                tk_count += title_tokens if title_tokens else 0
            except Exception as e:
                logging.warning(f"타이틀 임베딩 중 오류 발생: {str(e)}")
                title_embeddings = None
            
            # 컨텐츠 임베딩
            contents = []
            for d in doc_batch:
                c = "\n".join(d.get("question_kwd", []))
                if not c:
                    c = d["content_with_weight"]
                c = re.sub(r"</?(table|td|caption|tr|th)( [^<>]{0,12})?>", " ", c)
                if not c:
                    c = "None"
                contents.append(c)
            
            try:
                content_embeddings, content_tokens = mdl.encode(contents)
                tk_count += content_tokens if content_tokens else 0
            except Exception as e:
                logging.warning(f"컨텐츠 임베딩 중 오류 발생: {str(e)}")
                content_embeddings = None
            
            # 임베딩이 실패한 경우 건너뛰기
            if title_embeddings is None or content_embeddings is None:
                continue
            
            # 가중치 적용 및 벡터 저장
            title_w = float(parser_config.get("filename_embd_weight", 0.1))
            for i, doc in enumerate(doc_batch):
                try:
                    vec = (title_w * title_embeddings[i] + (1 - title_w) * content_embeddings[i]).tolist()
                    vector_size = len(vec)
                    doc["q_%d_vec" % vector_size] = vec
                except Exception as e:
                    logging.warning(f"벡터 결합 중 오류 발생: {str(e)}")
                    continue
            
            # 메모리 해제
            del title_embeddings
            del content_embeddings
            gc.collect()
    except Exception as e:
        logging.error(f"임베딩 처리 중 오류 발생: {str(e)}")
        raise
        
    return tk_count, vector_size


def run_raptor(row, chat_mdl, embd_mdl, callback=None):
    vts, _ = embd_mdl.encode(["ok"])
    vector_size = len(vts[0])
    vctr_nm = "q_%d_vec" % vector_size
    chunks = []
    for d in settings.retrievaler.chunk_list(row["doc_id"], row["tenant_id"], [str(row["kb_id"])],
                                             fields=["content_with_weight", vctr_nm]):
        chunks.append((d["content_with_weight"], np.array(d[vctr_nm])))

    raptor = Raptor(
        row["parser_config"]["raptor"].get("max_cluster", 64),
        chat_mdl,
        embd_mdl,
        row["parser_config"]["raptor"]["prompt"],
        row["parser_config"]["raptor"]["max_token"],
        row["parser_config"]["raptor"]["threshold"]
    )
    original_length = len(chunks)
    chunks = raptor(chunks, row["parser_config"]["raptor"]["random_seed"], callback)
    doc = {
        "doc_id": row["doc_id"],
        "kb_id": [str(row["kb_id"])],
        "docnm_kwd": row["name"],
        "title_tks": rag_tokenizer.tokenize(row["name"])
    }
    if row["pagerank"]:
        doc[PAGERANK_FLD] = int(row["pagerank"])
    res = []
    tk_count = 0
    for content, vctr in chunks[original_length:]:
        d = copy.deepcopy(doc)
        d["id"] = xxhash.xxh64((content + str(d["doc_id"])).encode("utf-8")).hexdigest()
        d["create_time"] = str(datetime.now()).replace("T", " ")[:19]
        d["create_timestamp_flt"] = datetime.now().timestamp()
        d[vctr_nm] = vctr.tolist()
        d["content_with_weight"] = content
        d["content_ltks"] = rag_tokenizer.tokenize(content)
        d["content_sm_ltks"] = rag_tokenizer.fine_grained_tokenize(d["content_ltks"])
        res.append(d)
        tk_count += num_tokens_from_string(content)
    return res, tk_count, vector_size


def log_document_structure(chunks):
    """문서 구조 로깅
    Args:
        chunks: 청크 리스트
    """
    for i, chunk in enumerate(chunks):
        try:
            log_chunk = {
                'chunk_index': i,
                'content_type': chunk.get('content_type', 'unknown'),
                'chunk_type': chunk.get('chunk_type', 'unknown'),
                'content_length': len(chunk.get('content', '')),
                'content_preview': chunk.get('content', '')[:100] + '...' if len(chunk.get('content', '')) > 100 else chunk.get('content', ''),
                'tokenized_fields': {
                    'content_ltks': chunk.get('content_ltks', '')[:100] + '...' if len(chunk.get('content_ltks', '')) > 100 else chunk.get('content_ltks', ''),
                    'content_sm_tks': chunk.get('content_sm_tks', '')[:100] + '...' if len(chunk.get('content_sm_tks', '')) > 100 else chunk.get('content_sm_tks', ''),
                    'searchable_text_length': len(chunk.get('searchable_text', '')),
                },
                'metadata': {
                    'title_tks': chunk.get('title_tks', ''),
                    'title_sm_tks': chunk.get('title_sm_tks', ''),
                    'important_kwd': chunk.get('important_kwd', []),
                    'question_kwd': chunk.get('question_kwd', []),
                    'participants': chunk.get('participants', []),
                    'thread_depth': chunk.get('thread_depth', 0),
                    'importance': chunk.get('importance', 0)
                }
            }
            logging.info(f"ES 문서 구조 (청크 {i}):\n{json.dumps(log_chunk, ensure_ascii=False, indent=2)}")
        except Exception as e:
            logging.warning(f"문서 구조 로깅 중 오류 발생: {str(e)}")


def do_handle_task(task):
    global DONE_TASKS, FAILED_TASKS, CURRENT_TASK
    
    try:
        # 현재 작업 상태 업데이트 및 heartbeat 전송
        with mt_lock:
            CURRENT_TASK = copy.deepcopy(task)
        
        now = datetime.now()
        heartbeat = json.dumps({
            "name": CONSUMER_NAME,
            "now": now.astimezone().isoformat(timespec="milliseconds"),
            "boot_at": BOOT_AT,
            "pending": PENDING_TASKS,
            "lag": LAG_TASKS,
            "done": DONE_TASKS,
            "failed": FAILED_TASKS,
            "current": CURRENT_TASK,
        })
        REDIS_CONN.zadd(CONSUMER_NAME, heartbeat, now.timestamp())
        
        # 기존 task 처리 로직
        task_id = task["id"]
        task_from_page = task["from_page"]
        task_to_page = task["to_page"]
        task_tenant_id = task["tenant_id"]
        task_embedding_id = task["embd_id"]
        task_language = task["language"]
        task_llm_id = task["llm_id"]
        task_dataset_id = task["kb_id"]
        task_doc_id = task["doc_id"]
        task_document_name = task["name"]
        task_parser_config = task["parser_config"]

        # 토큰 카운트 초기화
        token_count = 0
        vector_size = 0

        # prepare the progress callback function
        progress_callback = partial(set_progress, task_id, task_from_page, task_to_page)

        # FIXME: workaround, Infinity doesn't support table parsing method, this check is to notify user
        lower_case_doc_engine = settings.DOC_ENGINE.lower()
        if lower_case_doc_engine == 'infinity' and task['parser_id'].lower() == 'table':
            error_message = "Table parsing method is not supported by Infinity, please use other parsing methods or use Elasticsearch as the document engine."
            progress_callback(-1, msg=error_message)
            raise Exception(error_message)

        try:
            task_canceled = TaskService.do_cancel(task_id)
        except DoesNotExist:
            logging.warning(f"task {task_id} is unknown")
            return
        if task_canceled:
            progress_callback(-1, msg="Task has been canceled.")
            return

        try:
            # bind embedding model
            embedding_model = LLMBundle(task_tenant_id, LLMType.EMBEDDING, llm_name=task_embedding_id, lang=task_language)
        except Exception as e:
            error_message = f'Fail to bind embedding model: {str(e)}'
            progress_callback(-1, msg=error_message)
            logging.exception(error_message)
            raise

        # Either using RAPTOR or Standard chunking methods
        if task.get("task_type", "") == "raptor":
            try:
                # bind LLM for raptor
                chat_model = LLMBundle(task_tenant_id, LLMType.CHAT, llm_name=task_llm_id, lang=task_language)

                # run RAPTOR
                chunks, token_count, vector_size = run_raptor(task, chat_model, embedding_model, progress_callback)
                
                # 문서 구조 로깅 추가
                progress_callback(msg="문서 구조 로깅 중...")
                log_document_structure(chunks)
                
            except TaskCanceledException:
                raise
            except Exception as e:
                error_message = f'Fail to bind LLM used by RAPTOR: {str(e)}'
                progress_callback(-1, msg=error_message)
                logging.exception(error_message)
                raise
        else:
            # Standard chunking methods
            start_ts = timer()
            chunks = []
            chunk_generator = build_chunks(task, progress_callback)
            for chunk in chunk_generator:
                chunks.append(chunk)
                # 메모리 모니터링
                if len(chunks) % 100 == 0:
                    current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                    logging.info(f"Current memory usage after {len(chunks)} chunks: {current_memory:.2f}MB")
            
            logging.info("Build document {}: {:.2f}s".format(task_document_name, timer() - start_ts))
            if not chunks:
                progress_callback(1., msg=f"No chunk built from {task_document_name}")
                return

            # 문서 구조 로깅 최적화
            progress_callback(msg="문서 구조 로깅 중...")
            for i in range(0, len(chunks), 10):  # 10개씩 나눠서 로깅
                log_document_structure(chunks[i:i+10])
            
            # 임베딩 처리
            try:
                progress_callback(msg="임베딩 처리 중...")
                token_count, vector_size = embedding(chunks, embedding_model, task_parser_config, progress_callback)
            except Exception as e:
                error_message = f"임베딩 생성 중 오류 발생: {str(e)}"
                progress_callback(-1, msg=error_message)
                logging.exception(error_message)
                raise
            
            # ES 저장 최적화
            es_bulk_size = min(2, max(1, len(chunks) // 200))  # 더 작은 배치 사이즈
            chunk_ids = []
            
            for b in range(0, len(chunks), es_bulk_size):
                current_chunks = chunks[b:b + es_bulk_size]
                doc_store_result = settings.docStoreConn.insert(current_chunks, search.index_name(task_tenant_id),
                                                              task_dataset_id)
                if doc_store_result:
                    error_message = f"Insert chunk error: {doc_store_result}"
                    progress_callback(-1, msg=error_message)
                    if chunk_ids:
                        settings.docStoreConn.delete({"id": chunk_ids}, search.index_name(task_tenant_id),
                                                  task_dataset_id)
                    raise Exception(error_message)
                
                chunk_ids.extend([chunk["id"] for chunk in current_chunks])
                if b % 64 == 0:  # 더 자주 저장
                    progress_callback(prog=0.8 + 0.1 * (b + 1) / len(chunks), msg="")
                    try:
                        TaskService.update_chunk_ids(task["id"], " ".join(chunk_ids))
                    except DoesNotExist:
                        logging.warning(f"do_handle_task update_chunk_ids failed since task {task['id']} is unknown.")
                        settings.docStoreConn.delete({"id": chunk_ids}, search.index_name(task_tenant_id),
                                                  task_dataset_id)
                        return
                
                # 메모리 해제
                del current_chunks
                if b % 100 == 0:
                    gc.collect()

            DocumentService.increment_chunk_num(task_doc_id, task_dataset_id, token_count, len(chunks), 0)

            time_cost = timer() - start_ts
            progress_callback(prog=1.0, msg="Done ({:.2f}s)".format(time_cost))
            logging.info(
                "Chunk doc({}), page({}-{}), chunks({}), token({}), elapsed:{:.2f}".format(task_document_name, task_from_page,
                                                                                           task_to_page, len(chunks),
                                                                                           token_count, time_cost))

    finally:
        # 작업 완료 후 상태 업데이트
        with mt_lock:
            CURRENT_TASK = None


def handle_task():
    global PAYLOAD, mt_lock, DONE_TASKS, FAILED_TASKS, CURRENT_TASK
    task = collect()
    if task:
        try:
            logging.info(f"handle_task begin for task {json.dumps(task)}")
            with mt_lock:
                CURRENT_TASK = copy.deepcopy(task)
            do_handle_task(task)
            with mt_lock:
                DONE_TASKS += 1
                CURRENT_TASK = None
            logging.info(f"handle_task done for task {json.dumps(task)}")
        except TaskCanceledException:
            with mt_lock:
                DONE_TASKS += 1
                CURRENT_TASK = None
            try:
                set_progress(task["id"], prog=-1, msg="handle_task got TaskCanceledException")
            except Exception:
                pass
            logging.debug("handle_task got TaskCanceledException", exc_info=True)
        except Exception as e:
            with mt_lock:
                FAILED_TASKS += 1
                CURRENT_TASK = None
            try:
                set_progress(task["id"], prog=-1, msg=f"[Exception]: {e}")
            except Exception:
                pass
            logging.exception(f"handle_task got exception for task {json.dumps(task)}")
    if PAYLOAD:
        PAYLOAD.ack()
        PAYLOAD = None


def report_status():
    global CONSUMER_NAME, BOOT_AT, PENDING_TASKS, LAG_TASKS, mt_lock, DONE_TASKS, FAILED_TASKS, CURRENT_TASK
    REDIS_CONN.sadd("TASKEXE", CONSUMER_NAME)
    last_report_time = 0
    
    while True:
        try:
            now = time.time()
            # 30초마다만 상태 보고
            if now - last_report_time < 30:
                time.sleep(1)
                continue
                
            last_report_time = now
            now_dt = datetime.now()
            
            # Redis 연결 재사용
            try:
                group_info = REDIS_CONN.queue_info(SVR_QUEUE_NAME, "rag_flow_svr_task_broker")
                if group_info is not None:
                    PENDING_TASKS = int(group_info.get("pending", 0))
                    LAG_TASKS = int(group_info.get("lag", 0))

                with mt_lock:
                    heartbeat = json.dumps({
                        "name": CONSUMER_NAME,
                        "now": now_dt.astimezone().isoformat(timespec="milliseconds"),
                        "boot_at": BOOT_AT,
                        "pending": PENDING_TASKS,
                        "lag": LAG_TASKS,
                        "done": DONE_TASKS,
                        "failed": FAILED_TASKS,
                        "current": CURRENT_TASK,
                    })
                REDIS_CONN.zadd(CONSUMER_NAME, heartbeat, now)
                
                # 30분 이상 된 데이터 정리
                expired = REDIS_CONN.zcount(CONSUMER_NAME, 0, now - 1800)  # 30분
                if expired > 0:
                    REDIS_CONN.zpopmin(CONSUMER_NAME, expired)
                    
                logging.info(f"{CONSUMER_NAME} reported heartbeat: {heartbeat}")
            except Exception as e:
                logging.error(f"Redis operation failed: {e}")
                
        except Exception as e:
            logging.exception("report_status got exception")
            time.sleep(5)  # 에러 발생시 좀 더 긴 대기


def analyze_heap(snapshot1: tracemalloc.Snapshot, snapshot2: tracemalloc.Snapshot, snapshot_id: int, dump_full: bool):
    msg = ""
    if dump_full:
        stats2 = snapshot2.statistics('lineno')
        msg += f"{CONSUMER_NAME} memory usage of snapshot {snapshot_id}:\n"
        for stat in stats2[:10]:
            msg += f"{stat}\n"
    stats1_vs_2 = snapshot2.compare_to(snapshot1, 'lineno')
    msg += f"{CONSUMER_NAME} memory usage increase from snapshot {snapshot_id - 1} to snapshot {snapshot_id}:\n"
    for stat in stats1_vs_2[:10]:
        msg += f"{stat}\n"
    msg += f"{CONSUMER_NAME} detailed traceback for the top memory consumers:\n"
    for stat in stats1_vs_2[:3]:
        msg += '\n'.join(stat.traceback.format())
    logging.info(msg)


# 메모리 제한 설정
def set_memory_limit():
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        logging.info(f"Current memory limits - soft: {soft/(1024*1024*1024):.2f}GB, hard: {hard/(1024*1024*1024):.2f}GB")
        
        # 2GB 또는 현재 하드 리밋 중 작은 값으로 설정
        target_limit = min(2 * 1024 * 1024 * 1024, hard)
        if target_limit < soft:
            # 현재 소프트 리밋이 더 크면 조정하지 않음
            logging.warning(f"Current soft limit ({soft/(1024*1024*1024):.2f}GB) is higher than target limit ({target_limit/(1024*1024*1024):.2f}GB). Keeping current limit.")
            return
            
        resource.setrlimit(resource.RLIMIT_AS, (target_limit, hard))
        logging.info(f"Memory limit set to {target_limit/(1024*1024*1024):.2f}GB")
    except ValueError as e:
        logging.warning(f"Failed to set memory limit: {e}. Using system defaults.")
    except Exception as e:
        logging.warning(f"Unexpected error while setting memory limit: {e}. Using system defaults.")


# LLM 모델 캐싱
@lru_cache(maxsize=1)
def get_cached_model(tenant_id, model_type, model_name, lang):
    return LLMBundle(tenant_id, model_type, llm_name=model_name, lang=lang)


def cleanup_resources():
    global background_thread
    logging.info("Cleaning up resources...")
    if background_thread and background_thread.is_alive():
        logging.info("Stopping background thread...")
        background_thread.join(timeout=5)
    logging.info("Cleanup complete")


def signal_handler(signum, frame):
    logging.info(f"Received signal {signum}")
    cleanup_resources()
    sys.exit(0)


def main():
    global background_thread
    
    # 시그널 핸들러 등록
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logging.info(r"""
  ______           __      ______                     __            
 /_  __/___ ______/ /__   / ____/  _____  _______  __/ /_____  _____
  / / / __ `/ ___/ //_/  / __/ | |/_/ _ \/ ___/ / / / __/ __ \/ ___/
 / / / /_/ (__  ) ,<    / /____>  </  __/ /__/ /_/ / /_/ /_/ / /    
/_/  \__,_/____/_/|_|  /_____/_/|_|\___/\___/\__,_/\__/\____/_/                               
    """)
    logging.info(f'TaskExecutor: RAGFlow version: {get_ragflow_version()}')
    
    try:
        # 메모리 제한 설정
        set_memory_limit()
        
        # 프로세스 우선순위 설정
        try:
            os.nice(10)
        except Exception as e:
            logging.warning(f"Failed to set process priority: {e}")
        
        settings.init_settings()
        print_rag_settings()
        
        # 메모리 모니터링 시작
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        logging.info(f"Initial memory usage: {initial_memory:.2f}MB")
        
        # 기존 스레드가 있다면 정리
        if background_thread and background_thread.is_alive():
            background_thread.join(timeout=5)
        
        # 새 스레드 시작 (필요한 경우에만)
        if os.environ.get('ENABLE_STATUS_THREAD', '1') == '1':
            background_thread = threading.Thread(target=report_status)
            background_thread.daemon = True
            try:
                background_thread.start()
            except RuntimeError as e:
                logging.error(f"Failed to start background thread: {e}")
                logging.warning("Continuing without background thread...")
        else:
            logging.info("Status reporting thread disabled by environment variable")
        
        TRACE_MALLOC_DELTA = int(os.environ.get('TRACE_MALLOC_DELTA', "0"))
        TRACE_MALLOC_FULL = int(os.environ.get('TRACE_MALLOC_FULL', "0"))
        if TRACE_MALLOC_DELTA > 0:
            if TRACE_MALLOC_FULL < TRACE_MALLOC_DELTA:
                TRACE_MALLOC_FULL = TRACE_MALLOC_DELTA
            tracemalloc.start()
            snapshot1 = tracemalloc.take_snapshot()
        
        task_count = 0
        while True:
            try:
                handle_task()
                task_count += 1
                
                # 주기적으로 메모리 사용량 체크 및 로깅
                if task_count % 10 == 0:  # 10개 태스크마다
                    current_memory = process.memory_info().rss / 1024 / 1024
                    logging.info(f"Current memory usage: {current_memory:.2f}MB")
                    
                    # 메모리 임계치 초과시 경고
                    if current_memory > 1800:  # 1.8GB
                        logging.warning(f"High memory usage detected: {current_memory:.2f}MB")
                        gc.collect()  # 가비지 컬렉션 강제 실행
                
                num_tasks = DONE_TASKS + FAILED_TASKS
                if TRACE_MALLOC_DELTA > 0 and num_tasks > 0 and num_tasks % TRACE_MALLOC_DELTA == 0:
                    snapshot2 = tracemalloc.take_snapshot()
                    analyze_heap(snapshot1, snapshot2, int(num_tasks / TRACE_MALLOC_DELTA), 
                               num_tasks % TRACE_MALLOC_FULL == 0)
                    snapshot1 = snapshot2
                    snapshot2 = None
                    
            except Exception as e:
                logging.exception("Error in main loop")
                time.sleep(1)  # 에러 발생시 잠시 대기
                
    except Exception as e:
        logging.exception("Fatal error in main")
        cleanup_resources()
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt")
        cleanup_resources()
    except Exception as e:
        logging.exception("Unhandled exception")
        cleanup_resources()
        sys.exit(1)
