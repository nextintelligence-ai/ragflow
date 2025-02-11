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

import logging
from email import policy
from email.parser import BytesParser
from rag.app.naive import chunk as naive_chunk
import re
from rag.nlp import rag_tokenizer, naive_merge, tokenize_chunks
from deepdoc.parser import HtmlParser, TxtParser
from timeit import default_timer as timer
import io
from langdetect import detect
from bs4 import BeautifulSoup
import email.utils
from datetime import datetime
import nltk
from nltk.corpus import wordnet
import json

# NLTK 데이터 초기화
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/wordnet')
    nltk.data.find('corpora/omw-1.4')
except LookupError:
    nltk.download('punkt')
    nltk.download('wordnet')
    nltk.download('omw-1.4')

# WordNet 초기화
try:
    wordnet._wordnet  # WordNet 초기화 강제
except:
    pass

def get_language_specific_delimiters(detected_lang):
    """언어별 적절한 구분자 반환"""
    delimiters = {
        'ko': {
            'sentence': ['。', '！', '？', '.', '!', '?'],
            'clause': ['，', '；', ',', ';', '：', ':', '」', '』'],
            'context': ['그러나', '하지만', '따라서', '그래서', '또한', '그리고'],
            'quote': ['>', '▶', '▷']
        },
        'en': {
            'sentence': ['.', '!', '?'],
            'clause': [',', ';', ':'],
            'context': ['however', 'but', 'therefore', 'thus', 'moreover', 'and'],
            'quote': ['>', '▶', '▷']
        },
        'ja': {
            'sentence': ['。', '！', '？', '.', '!', '?'],
            'clause': ['、', '，', '；', ',', ';', '：', ':', '」', '』'],
            'context': ['しかし', 'だが', 'そのため', 'また', 'そして'],
            'quote': ['>', '▶', '▷']
        }
    }
    return delimiters.get(detected_lang, delimiters['en'])

def extract_header_metadata(msg):
    """이메일 헤더 메타데이터 추출"""
    headers = []
    important_headers = ['From', 'To', 'Cc', 'Subject', 'Date']
    
    for header in important_headers:
        value = msg.get(header)
        if value:
            if header == 'Date':
                try:
                    date_tuple = email.utils.parsedate_tz(value)
                    if date_tuple:
                        dt = datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
                        value = dt.strftime('%Y-%m-%d %H:%M:%S %z')
                except Exception:
                    pass
            headers.append(f"{header}: {value}")
    
    return "\n".join(headers)

def process_html_content(html_content):
    """HTML 컨텐츠 처리 및 정제"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 스타일, 스크립트 제거
        for tag in soup(['style', 'script']):
            tag.decompose()
            
        # 인용문 분리
        quotes = []
        for quote in soup.find_all(['blockquote', 'div.quote']):
            quotes.append(quote.get_text(strip=True))
            quote.decompose()
            
        # 남은 텍스트 추출
        main_text = soup.get_text(separator='\n', strip=True)
        
        return main_text, quotes
    except Exception as e:
        logging.warning(f"HTML 처리 중 오류 발생: {e}")
        return html_content, []

def extract_important_keywords(text, content_type="body"):
    """중요 키워드 추출
    Args:
        text: 텍스트 내용
        content_type: 컨텐츠 타입 (header, body, quote, attachment)
    """
    important_words = set()  # 중복 방지를 위해 set 사용
    
    # 이메일 관련 키워드
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    emails = re.findall(email_pattern, text)
    important_words.update(emails)
    
    # 날짜/시간 패턴
    date_patterns = [
        r'\d{4}[-/]\d{1,2}[-/]\d{1,2}',  # YYYY-MM-DD
        r'\d{1,2}[-/]\d{1,2}[-/]\d{4}',  # DD-MM-YYYY
        r'\d{1,2}:\d{2}(?::\d{2})?'      # HH:MM:SS
    ]
    for pattern in date_patterns:
        dates = re.findall(pattern, text)
        important_words.update(dates)
    
    # 헤더 관련 키워드
    if content_type == "header":
        header_fields = ["Subject:", "From:", "To:", "Cc:", "Bcc:", "Date:"]
        for field in header_fields:
            if field in text:
                value = text.split(field)[1].split('\n')[0].strip()
                if value:
                    important_words.add(value)
    
    # 본문 관련 키워드
    if content_type in ["body", "quote"]:
        # 인사말 패턴
        greetings = [
            r'안녕하[세습]요',
            r'(?:좋은|수고하신|안녕하신) ?\w{2,3} 되[세습]요',
            r'감사합니다',
            r'Dear\s+\w+',
            r'Hello\s+\w+',
            r'Hi\s+\w+'
        ]
        for pattern in greetings:
            matches = re.findall(pattern, text)
            important_words.update(matches)
        
        # 중요 표시 단어
        important_markers = [
            "중요", "긴급", "필독", "요청", "문의", "답변", "회신", "전달", "공지", "안내",
            "Important", "Urgent", "Action", "Required", "Deadline", "ASAP", "Priority"
        ]
        words = text.split()
        marked_words = [word for word in words if word in important_markers]
        important_words.update(marked_words)
        
        # 조직/부서명 패턴
        org_patterns = [
            r'\w+[팀부과처청국실]',
            r'[A-Za-z\s]+\s+Team',
            r'[A-Za-z\s]+\s+Department',
            r'[A-Za-z\s]+\s+Division'
        ]
        for pattern in org_patterns:
            orgs = re.findall(pattern, text)
            important_words.update(orgs)
        
        # 연락처 패턴
        contact_patterns = [
            r'(?:전화|연락처|Tel|TEL|Phone)[\s:]+\d[\d\s-]+\d',
            r'\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}'
        ]
        for pattern in contact_patterns:
            contacts = re.findall(pattern, text)
            important_words.update(contacts)
    
    # 첨부파일 관련 키워드
    elif content_type == "attachment":
        # 파일 확장자
        ext_pattern = r'\.[A-Za-z0-9]+$'
        extensions = re.findall(ext_pattern, text)
        important_words.update(extensions)
        
        # 파일명에서 의미있는 단어 추출
        words = re.split(r'[_\-\s.]', text)
        meaningful_words = [word for word in words if len(word) > 2]
        important_words.update(meaningful_words)
    
    # 특수문자 제거 및 공백 정리
    cleaned_words = set()
    for word in important_words:
        cleaned = re.sub(r'[^\w\s@.-]', '', word).strip()
        if cleaned and len(cleaned) > 1:  # 빈 문자열과 한 글자 단어 제외
            cleaned_words.add(cleaned)
    
    return list(cleaned_words)  # set을 list로 변환하여 반환

def add_tokenized_fields(chunk_doc, text, content_type="body"):
    """각 청크에 토큰화된 필드 추가"""
    try:
        # 기본 토큰화
        tokens = rag_tokenizer.tokenize(text)
        if not tokens:
            tokens = text  # 토큰화 실패시 원본 텍스트 사용
        chunk_doc["content_ltks"] = tokens if isinstance(tokens, str) else " ".join(tokens)
        
        # 세밀한 토큰화
        try:
            fine_tokens = rag_tokenizer.fine_grained_tokenize(tokens)
            if not fine_tokens:
                fine_tokens = tokens  # 세밀한 토큰화 실패시 기본 토큰 사용
        except:
            fine_tokens = tokens  # 에러 발생시 기본 토큰 사용
        chunk_doc["content_sm_tks"] = fine_tokens if isinstance(fine_tokens, str) else " ".join(fine_tokens)
        
        # 검색 가능성을 높이기 위한 추가 필드
        chunk_doc["content_with_weight"] = text
        chunk_doc["text"] = text
        
        # 검색 가능한 모든 텍스트 결합
        searchable_parts = [
            text,
            chunk_doc['content_ltks'],
            chunk_doc['content_sm_tks']
        ]
        
        chunk_doc["searchable_text"] = ' '.join(searchable_parts)
        chunk_doc["searchable_text_length"] = len(chunk_doc["searchable_text"])
        
    except Exception as e:
        logging.warning(f"토큰화 중 오류 발생: {str(e)}")
        # 기본값 설정 - 검색 가능성 유지
        chunk_doc["content_ltks"] = text
        chunk_doc["content_sm_tks"] = text
        chunk_doc["content_with_weight"] = text
        chunk_doc["text"] = text
        chunk_doc["searchable_text"] = text
        chunk_doc["searchable_text_length"] = len(text)

def split_into_sentences(text, lang="auto"):
    """텍스트를 문장 단위로 분리"""
    if lang == "auto":
        try:
            lang = detect(text)
        except:
            lang = "en"
    
    delimiters = get_language_specific_delimiters(lang)
    
    # 줄바꿈으로 먼저 분리
    paragraphs = text.split('\n')
    sentences = []
    
    for paragraph in paragraphs:
        if not paragraph.strip():
            continue
            
        # 인용문 체크
        is_quote = any(paragraph.strip().startswith(q) for q in delimiters['quote'])
        
        # 현재 문장 버퍼
        current_sentence = ""
        
        for char in paragraph:
            current_sentence += char
            
            # 문장 종결 확인
            is_end = False
            for end in delimiters['sentence']:
                if current_sentence.strip().endswith(end):
                    is_end = True
                    break
            
            # 문장이 완성되면 추가
            if is_end:
                if current_sentence.strip():
                    sentences.append({
                        'text': current_sentence.strip(),
                        'is_quote': is_quote
                    })
                current_sentence = ""
        
        # 남은 문장 처리
        if current_sentence.strip():
            sentences.append({
                'text': current_sentence.strip(),
                'is_quote': is_quote
            })
    
    return sentences

def group_sentences(sentences, max_chunk_size=1024):
    """문장들을 적절한 크기의 청크로 그룹화"""
    chunks = []
    current_chunk = []
    current_size = 0
    
    for sentence in sentences:
        # 문장 토큰 수 계산
        sentence_tokens = len(rag_tokenizer.tokenize(sentence['text']))
        
        # 단일 문장이 최대 크기를 초과하는 경우
        if sentence_tokens > max_chunk_size:
            # 기존 청크가 있다면 저장
            if current_chunk:
                chunks.append('\n'.join([s['text'] for s in current_chunk]))
                current_chunk = []
                current_size = 0
            
            # 긴 문장을 적절히 분할
            words = sentence['text'].split()
            temp_sentence = []
            temp_size = 0
            
            for word in words:
                word_tokens = len(rag_tokenizer.tokenize(word))
                if temp_size + word_tokens > max_chunk_size and temp_sentence:
                    chunks.append(' '.join(temp_sentence))
                    temp_sentence = [word]
                    temp_size = word_tokens
                else:
                    temp_sentence.append(word)
                    temp_size += word_tokens
            
            if temp_sentence:
                chunks.append(' '.join(temp_sentence))
            continue
        
        # 현재 청크에 문장을 추가했을 때 최대 크기를 초과하는 경우
        if current_size + sentence_tokens > max_chunk_size:
            if current_chunk:
                chunks.append('\n'.join([s['text'] for s in current_chunk]))
            current_chunk = [sentence]
            current_size = sentence_tokens
        else:
            # 인용문이 시작되거나 끝날 때 새로운 청크 시작
            if current_chunk and current_chunk[-1]['is_quote'] != sentence['is_quote']:
                chunks.append('\n'.join([s['text'] for s in current_chunk]))
                current_chunk = []
                current_size = 0
            
            current_chunk.append(sentence)
            current_size += sentence_tokens
    
    # 마지막 청크 처리
    if current_chunk:
        chunks.append('\n'.join([s['text'] for s in current_chunk]))
    
    return chunks

def smart_email_chunking(text, lang="auto", max_chunk_size=1024):
    """스마트 이메일 청킹 - 문장 단위 처리
    - 문장 단위로 분리
    - 문맥을 고려한 그룹화
    - 인용문 구분
    - 언어별 최적화
    """
    # 문장 단위로 분리
    sentences = split_into_sentences(text, lang)
    
    # 빈 문장 제거
    sentences = [s for s in sentences if s['text'].strip()]
    
    # 문장 그룹화하여 청크 생성
    chunks = group_sentences(sentences, max_chunk_size)
    
    # 빈 청크 제거
    chunks = [chunk.strip() for chunk in chunks if chunk.strip()]
    
    return chunks

def extract_email_metadata(msg):
    """향상된 이메일 메타데이터 추출"""
    metadata = {
        'headers': {},
        'thread_info': {},
        'importance': 0,
        'participants': set(),
        'references': set(),
    }
    
    # 기본 헤더 처리
    important_headers = [
        'From', 'To', 'Cc', 'Bcc', 'Subject', 'Date',
        'Message-ID', 'In-Reply-To', 'References',
        'Thread-Index', 'Thread-Topic', 'Importance',
        'X-Priority', 'X-MSMail-Priority'
    ]
    
    for header in important_headers:
        value = msg.get(header)
        if value:
            if header == 'Date':
                try:
                    date_tuple = email.utils.parsedate_tz(value)
                    if date_tuple:
                        dt = datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
                        metadata['headers'][header] = dt.strftime('%Y-%m-%d %H:%M:%S %z')
                        continue
                except Exception:
                    pass
            metadata['headers'][header] = value
    
    # 참여자 추출
    for header in ['From', 'To', 'Cc', 'Bcc']:
        if header in metadata['headers']:
            addresses = email.utils.getaddresses([metadata['headers'][header]])
            for name, addr in addresses:
                if addr:
                    metadata['participants'].add(addr.lower())
    
    # 스레드 정보 처리
    if 'References' in metadata['headers']:
        refs = metadata['headers']['References'].split()
        metadata['references'].update(refs)
        metadata['thread_info']['depth'] = len(refs)
    else:
        metadata['thread_info']['depth'] = 0
    
    # 중요도 계산
    importance_indicators = {
        'X-Priority': {'1': 2, '2': 1},
        'X-MSMail-Priority': {'High': 2, 'Normal': 1, 'Low': 0},
        'Importance': {'high': 2, 'normal': 1, 'low': 0}
    }
    
    for header, values in importance_indicators.items():
        if header in metadata['headers']:
            value = metadata['headers'][header].lower()
            if value in values:
                metadata['importance'] = max(metadata['importance'], values[value])
    
    # 집합을 리스트로 변환
    metadata['participants'] = list(metadata['participants'])
    metadata['references'] = list(metadata['references'])
    
    return metadata

def process_email_content(msg):
    """이메일 컨텐츠 고급 처리"""
    content_parts = {
        'text': [],
        'html': [],
        'attachments': [],
        'quotes': []
    }
    
    def process_part(part):
        """파트 처리"""
        content_type = part.get_content_type()
        
        if content_type == 'text/plain':
            text = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
            content_parts['text'].append(text)
            
        elif content_type == 'text/html':
            html = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
            main_text, quotes = process_html_content(html)
            content_parts['html'].append(main_text)
            content_parts['quotes'].extend(quotes)
            
        elif part.get_filename():  # 첨부파일
            content_parts['attachments'].append({
                'filename': part.get_filename(),
                'content_type': content_type,
                'size': len(part.get_payload(decode=True))
            })
    
    if msg.is_multipart():
        for part in msg.walk():
            if part.is_multipart():
                continue
            process_part(part)
    else:
        process_part(msg)
    
    return content_parts

def chunk(
    filename,
    binary=None,
    from_page=0,
    to_page=100000,
    lang="auto",
    callback=None,
    max_chunk_size=1024,  # 이메일의 경우 더 큰 청크 크기 사용
    **kwargs
):
    """
    개선된 이메일 청킹 프로세스
    - 구조적 특성 (헤더, 본문, 인용문, 첨부파일) 고려
    - 언어 자동 감지 및 언어별 최적화
    - 문맥 기반 청킹
    - 메타데이터 강화
    """
    if callback is None:
        callback = lambda prog=None, msg="": None

    if binary:
        msg = BytesParser(policy=policy.default).parse(io.BytesIO(binary))
    else:
        msg = BytesParser(policy=policy.default).parse(open(filename, "rb"))

    # 1. 메타데이터 추출
    metadata = extract_email_metadata(msg)
    
    # 기본 doc 구조 생성
    subject = metadata['headers'].get('Subject', '')
    doc = {
        "docnm_kwd": filename,
        "title_tks": rag_tokenizer.tokenize(subject) if subject else "",
        "title_sm_tks": rag_tokenizer.fine_grained_tokenize(rag_tokenizer.tokenize(subject)) if subject else "",
        "create_time": metadata['headers'].get('Date', str(datetime.now()).replace("T", " ")[:19]),
        "create_timestamp_flt": datetime.now().timestamp(),
        "participants": metadata['participants'],
        "thread_depth": metadata['thread_info']['depth'],
        "importance": metadata['importance']
    }
    
    # 2. 컨텐츠 처리
    content = process_email_content(msg)
    
    chunks = []
    
    # 3. 헤더 청크 생성
    header_text = "\n".join(f"{k}: {v}" for k, v in metadata['headers'].items())
    header_chunk = doc.copy()
    header_chunk.update({
        "content_type": "header",
        "content": header_text,
        "chunk_type": "header"
    })
    chunks.append(header_chunk)
    
    # 4. 본문 청크 생성
    if content['text'] or content['html']:
        main_text = "\n".join(content['text']) if content['text'] else "\n".join(content['html'])
        text_chunks = smart_email_chunking(main_text, lang, max_chunk_size)
        
        for i, chunk_text in enumerate(text_chunks):
            chunk_doc = doc.copy()
            chunk_doc.update({
                "content_type": "body",
                "content": chunk_text,
                "chunk_type": "body",
                "chunk_index": i,
                "total_chunks": len(text_chunks)
            })
            chunks.append(chunk_doc)
    
    # 5. 인용문 청크 생성
    if content['quotes']:
        for i, quote in enumerate(content['quotes']):
            quote_chunks = smart_email_chunking(quote, lang, max_chunk_size)
            for j, chunk_text in enumerate(quote_chunks):
                chunk_doc = doc.copy()
                chunk_doc.update({
                    "content_type": "quote",
                    "content": chunk_text,
                    "chunk_type": "quote",
                    "quote_index": i,
                    "chunk_index": j,
                    "total_chunks": len(quote_chunks)
                })
                chunks.append(chunk_doc)
    
    # 6. 첨부파일 메타데이터
    if content['attachments']:
        attach_chunk = doc.copy()
        attach_chunk.update({
            "content_type": "attachment_metadata",
            "content": json.dumps(content['attachments'], ensure_ascii=False),
            "chunk_type": "attachment_metadata"
        })
        chunks.append(attach_chunk)
    
    # 7. 각 청크에 토큰화 필드 추가
    for chunk in chunks:
        add_tokenized_fields(chunk, chunk['content'], chunk['content_type'])
    
    # ES 문서 구조 로깅
    for i, chunk in enumerate(chunks):
        try:
            log_chunk = {
                'chunk_index': i,
                'content_type': chunk['content_type'],
                'chunk_type': chunk['chunk_type'],
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
                    'participants': chunk.get('participants', []),
                    'thread_depth': chunk.get('thread_depth', 0),
                    'importance': chunk.get('importance', 0)
                }
            }
            logging.info(f"ES 문서 구조 (청크 {i}):\n{json.dumps(log_chunk, ensure_ascii=False, indent=2)}")
        except Exception as e:
            logging.warning(f"문서 구조 로깅 중 오류 발생: {str(e)}")
    
    callback(100, "이메일 청킹 완료")
    return chunks

if __name__ == "__main__":
    import sys

    def dummy(prog=None, msg=""):
        pass

    chunk(sys.argv[1], callback=dummy)
