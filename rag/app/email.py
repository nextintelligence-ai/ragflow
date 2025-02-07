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
        'ko': '\n!?。；！？.,:，：』」\n\n',  # 한국어
        'en': '\n!?.,:;}\n\n',  # 영어
        'ja': '\n!?。；！？.,:，：』」\n\n',  # 일본어
        'zh': '\n!?。；！？.,:，：』」\n\n',  # 중국어
    }
    return delimiters.get(detected_lang, '\n!?。；！？.,:，：』」\n\n')

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
    important_words = []
    
    # 헤더 관련 키워드
    if content_type == "header":
        header_keywords = ["Subject:", "From:", "To:", "Cc:", "Date:"]
        words = text.split()
        header_words = [word.strip(":") for word in words if any(keyword in word for keyword in header_keywords)]
        important_words.extend(header_words)
    
    # 본문 관련 키워드
    if content_type in ["body", "quote"]:
        # 대문자로 시작하는 단어 (이름, 고유명사 등)
        words = text.split()
        capitalized_words = [word for word in words if word and word[0].isupper()]
        important_words.extend(capitalized_words[:5])  # 상위 5개만 선택
        
        # 이메일 주소
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text)
        important_words.extend(emails)
        
        # 날짜/시간 패턴
        date_pattern = r'\d{4}[-/]\d{1,2}[-/]\d{1,2}'
        dates = re.findall(date_pattern, text)
        important_words.extend(dates)
        
        # 중요 표시 단어 ("Important", "Urgent", "Action Required" 등)
        important_markers = ["Important", "Urgent", "Action", "Required", "Deadline", "ASAP", "Priority"]
        marked_words = [word for word in words if word in important_markers]
        important_words.extend(marked_words)
        
        # 반복되는 단어 (2회 이상 등장하는 단어는 중요할 수 있음)
        word_counts = {}
        for word in words:
            if len(word) > 3:  # 짧은 단어 제외
                word_counts[word] = word_counts.get(word, 0) + 1
        repeated_words = [word for word, count in word_counts.items() if count > 1]
        important_words.extend(repeated_words[:5])  # 상위 5개만 선택
    
    # 첨부파일 관련 키워드
    elif content_type == "attachment":
        # 파일 확장자
        ext_pattern = r'\.[A-Za-z0-9]+$'
        extensions = re.findall(ext_pattern, text)
        important_words.extend(extensions)
        
        # 파일명에서 의미있는 단어 추출
        words = re.split(r'[_\-\s.]', text)
        meaningful_words = [word for word in words if len(word) > 2]
        important_words.extend(meaningful_words)
    
    # 중복 제거 및 정리
    important_words = list(set(important_words))
    # 특수문자 제거 및 공백 정리
    important_words = [re.sub(r'[^\w\s@.-]', '', word).strip() for word in important_words]
    # 빈 문자열 제거
    important_words = [word for word in important_words if word]
    
    return important_words

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
        chunk_doc["searchable_text"] = f"{text} {chunk_doc['content_ltks']} {chunk_doc['content_sm_tks']}"  # 검색 가능한 모든 텍스트 결합
        
        # 중요 키워드 추출 및 검색 가능성 향상
        keywords = extract_important_keywords(text, content_type)
        chunk_doc["important_kwd"] = keywords
        if keywords:
            chunk_doc["searchable_text"] += f" {' '.join(keywords)}"
        
    except Exception as e:
        logging.warning(f"토큰화 중 오류 발생: {str(e)}")
        # 기본값 설정 - 검색 가능성 유지
        chunk_doc["content_ltks"] = text
        chunk_doc["content_sm_tks"] = text
        chunk_doc["content_with_weight"] = text
        chunk_doc["text"] = text
        chunk_doc["searchable_text"] = text
        chunk_doc["important_kwd"] = []

def chunk(
    filename,
    binary=None,
    from_page=0,
    to_page=100000,
    lang="auto",
    callback=None,
    **kwargs,
):
    """
    EML 파일 처리를 위한 고급 chunking
    - 구조적 특성 (헤더, 본문, 인용문, 첨부파일) 고려
    - 언어 자동 감지 및 언어별 최적화
    - HTML/텍스트 컨텐츠 정제
    - 검색 가능성 향상을 위한 필드 추가
    """
    parser_config = kwargs.get(
        "parser_config",
        {"chunk_token_num": 256, "layout_recognize": True},
    )
    
    doc = {
        "docnm_kwd": filename,
        "title_tks": rag_tokenizer.tokenize(re.sub(r"\.[a-zA-Z]+$", "", filename)),
    }
    doc["title_sm_tks"] = rag_tokenizer.fine_grained_tokenize(doc["title_tks"])
    
    # 공통 필드 추가
    current_time = datetime.now()
    doc["create_time"] = str(current_time).replace("T", " ")[:19]
    doc["create_timestamp_flt"] = current_time.timestamp()
    doc["img_id"] = ""
    doc["question_tks"] = ""  # 이메일은 Q&A가 아니므로 빈 문자열
    doc["important_kwd"] = []  # 기본 빈 리스트로 초기화

    main_res = []
    attachment_res = []

    if binary:
        msg = BytesParser(policy=policy.default).parse(io.BytesIO(binary))
    else:
        msg = BytesParser(policy=policy.default).parse(open(filename, "rb"))

    # 1. 헤더 정보 처리
    header_chunk = extract_header_metadata(msg)
    header_doc = doc.copy()
    header_doc["content_type"] = "header"
    add_tokenized_fields(header_doc, header_chunk, "header")
    main_res.append(header_doc)

    text_contents = []
    html_contents = []
    quotes = []

    # 2. 이메일 본문 처리
    def _add_content(msg, content_type):
        if content_type == "text/plain":
            try:
                charset = msg.get_content_charset() or 'utf-8'
                payload = msg.get_payload(decode=True)
                text = payload.decode(charset, errors='replace')
                text_contents.append(text)
            except Exception as e:
                logging.warning(f"텍스트 디코딩 실패: {e}")
        elif content_type == "text/html":
            try:
                charset = msg.get_content_charset() or 'utf-8'
                payload = msg.get_payload(decode=True)
                html = payload.decode(charset, errors='replace')
                main_text, quote_parts = process_html_content(html)
                html_contents.append(main_text)
                quotes.extend(quote_parts)
            except Exception as e:
                logging.warning(f"HTML 디코딩 실패: {e}")
        elif "multipart" in content_type:
            if msg.is_multipart():
                for part in msg.iter_parts():
                    _add_content(part, part.get_content_type())

    _add_content(msg, msg.get_content_type())

    # 3. 언어 감지 및 chunk 생성
    all_text = "\n".join(text_contents + html_contents)
    try:
        detected_lang = detect(all_text[:1000]) if lang == "auto" else lang.lower()
    except:
        detected_lang = 'en'
    
    eng = detected_lang == "en"
    delimiters = get_language_specific_delimiters(detected_lang)
    
    # 4. 본문 chunking
    if all_text.strip():
        sections = [(text, "") for text in all_text.split("\n") if text.strip()]
        chunks = naive_merge(
            sections,
            int(parser_config.get("chunk_token_num", 256)),
            delimiters
        )
        for chunk in chunks:
            if chunk.strip():
                content_doc = doc.copy()
                content_doc["content_type"] = "body"
                # 본문 내용에 제목 정보 추가하여 검색 가능성 향상
                if "Subject" in header_chunk:
                    subject = header_chunk.split('Subject:')[1].split('\n')[0].strip()
                    chunk = subject + "\n\n" + chunk
                add_tokenized_fields(content_doc, chunk, "body")
                main_res.append(content_doc)

    # 5. 인용문 처리
    for quote in quotes:
        if quote.strip():
            quote_doc = doc.copy()
            quote_doc["content_type"] = "quote"
            add_tokenized_fields(quote_doc, quote, "quote")
            main_res.append(quote_doc)

    # 6. 첨부파일 처리
    for part in msg.iter_attachments():
        content_disposition = part.get("Content-Disposition")
        if content_disposition:
            dispositions = content_disposition.strip().split(";")
            if dispositions[0].lower() == "attachment":
                filename = part.get_filename()
                if filename:
                    attachment_doc = doc.copy()
                    attachment_doc["content_type"] = "attachment"
                    attachment_doc["attachment_name"] = filename
                    attachment_text = f"Attachment: {filename}"
                    add_tokenized_fields(attachment_doc, attachment_text, "attachment")
                    main_res.append(attachment_doc)
                
                payload = part.get_payload(decode=True)
                try:
                    attachment_chunks = naive_chunk(filename, payload, callback=callback, **kwargs)
                    # 첨부파일 청크에도 필요한 필드 추가
                    for chunk in attachment_chunks:
                        if "text" in chunk:
                            if "content_ltks" not in chunk:
                                add_tokenized_fields(chunk, chunk["text"], "attachment")
                    attachment_res.extend(attachment_chunks)
                except Exception as e:
                    logging.warning(f"첨부파일 처리 실패: {filename}, {str(e)}")

    return main_res + attachment_res

if __name__ == "__main__":
    import sys

    def dummy(prog=None, msg=""):
        pass

    chunk(sys.argv[1], callback=dummy)
