import os
import io
import json 
import fitz
import pandas as pd
import re
from datetime import datetime
from supabase import create_client, Client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ==========================================
# 🛑 1. Supabase (DB) 설정
# ==========================================
SUPABASE_URL = "https://cbvmxfklgeizoiidibdw.supabase.co"
SUPABASE_KEY = "sb_publishable_53SDy7w7iKYmEe1G4-yciA_cgqQKO5L"

# 클라우드(GitHub)에서는 환경 변수를, 로컬에서는 위 변수를 사용
SUPABASE_URL = os.environ.get("SUPABASE_URL") or SUPABASE_URL
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or SUPABASE_KEY

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# 🛑 2. Google Drive 설정
# ==========================================
SERVICE_ACCOUNT_FILE = 'credentials.json' 
SOURCE_FOLDER_ID = '1cxvRel_fCPVzJLAP7gd-JVDkaicPBWfn'     # 새 PDF가 올라오는 폴더
DONE_FOLDER_ID = '1J6K5Ko2nrKVBDCNfkyjGpjkXHHa1wyvW'       # 파싱이 끝난 PDF를 옮길 폴더

def get_drive_service():
    if "GCP_CREDENTIALS" in os.environ:
        creds_info = json.loads(os.environ["GCP_CREDENTIALS"])
        creds = service_account.Credentials.from_service_account_info(
            creds_info, scopes=['https://www.googleapis.com/auth/drive']
        )
    else:
        creds = service_account.Credentials.from_service_account_file(
            'credentials.json', scopes=['https://www.googleapis.com/auth/drive']
        )
    return build('drive', 'v3', credentials=creds)

# ==========================================
# 🆕 추가된 테마 추출 전용 함수
# ==========================================
def extract_theme_name(text):
    """
    텍스트 블록에서 꺾쇠괄호 < > 로 둘러싸인 테마 이름만 추출합니다.
    [ ] 형태나 일반 텍스트는 모두 None을 반환하여 무시합니다.
    """
    # 불필요한 특수 공백(\xa0)을 일반 공백으로 바꾸고, 양끝 줄바꿈(\n) 및 공백 제거
    clean_text = text.replace('\xa0', ' ').strip()
    
    # 정규식: 무조건 '<' 로 시작하고, '>' 로 끝나는 문자열만 찾기
    match = re.match(r'^\<(.*?)\>$', clean_text)
    
    if match:
        # 괄호 안의 실제 테마 이름만 빼오기 (양옆 공백 제거)
        return match.group(1).strip()
        
    return None

# ==========================================
# 🛑 3. 파싱 로직
# ==========================================
def extract_full_news_data(pdf_path):
    doc = fitz.open(pdf_path)
    filename = os.path.basename(pdf_path)
    match = re.search(r'^(\d{6})', filename)
    file_date = datetime.strptime(match.group(1), "%y%m%d").strftime("%Y-%m-%d") if match else "Unknown"
    file_day = filename[6:9] if "(" in filename else ""

    all_data = []
    current_theme = "경제 일반" 
    current_item = None
    start_marker = "< 경제 일반 >"
    end_marker = "< 기타 >"
    is_target_section = False
    main_margin_x0 = None 

    for page in doc:
        page_links = page.get_links()
        blocks = page.get_text("dict")["blocks"]
        for b in blocks:
            if "lines" not in b: continue
            
            for l in b["lines"]:
                # 🌟 [핵심 해결책] Span 쪼개짐 방지를 위해 한 줄의 텍스트를 미리 하나로 합칩니다.
                full_line_text = "".join([s["text"] for s in l["spans"]]).replace('\xa0', ' ').strip()
                if not full_line_text: continue

                # 1. 시작 마커 감지 (합친 텍스트로 검사)
                if start_marker in full_line_text and not is_target_section:
                    is_target_section = True
                    current_theme = "경제 일반" 
                    main_margin_x0 = l["bbox"][0] 
                    continue
                    
                # 2. 종료 마커 감지
                if end_marker in full_line_text:
                    if current_item: all_data.append(current_item)
                    is_target_section = False
                    break
                    
                if not is_target_section: continue

                # 3. 테마(카테고리) 변경 감지 (합친 텍스트로 검사)
                # 꺾쇠괄호 < > 로 시작하고 끝나는지 검사합니다.
                theme_match = re.match(r'^\<(.*?)\>$', full_line_text)
                if theme_match:
                    if current_item:
                        all_data.append(current_item)
                        current_item = None
                    current_theme = theme_match.group(1).strip()
                    main_margin_x0 = l["bbox"][0] 
                    continue

                # 4. 일반 뉴스 데이터 추출 (기존과 동일하게 조각(span) 단위로 위치와 링크 계산)
                for s in l["spans"]:
                    text = s["text"].strip()
                    if not text: continue
                    x0 = s["bbox"][0]
                    if text in ["관련주", "•"]: continue
                    
                    title_rect = fitz.Rect(s["bbox"])
                    link_url = ""
                    for link in page_links:
                        if title_rect.intersects(link["from"]):
                            link_url = link.get("uri", "")
                            break
                    
                    if link_url:
                        is_indented = False
                        if main_margin_x0 is not None:
                            if x0 > main_margin_x0 + 15: is_indented = True
                        elif x0 > 60: 
                            is_indented = True

                        if is_indented: continue 

                        if current_item: all_data.append(current_item)
                        current_item = {
                            "date": file_date, "day": file_day, "theme": current_theme,
                            "title": text, "url": link_url, "content": ""
                        }
                    else:
                        if current_item:
                            current_item["content"] += (" " + text if current_item["content"] else text)
                            
    if current_item: all_data.append(current_item)
    return pd.DataFrame(all_data)

def detect_keywords(text):
    keywords = ["상장", "공시", "M&A", "특허", "공급계약", "수주", "MOU", "임상"]
    found = [kw for kw in keywords if kw in text]
    return ", ".join(found) if found else ""

# --- Supabase 저장 로직 ---
def save_to_supabase(df):
    if df.empty: return 0
    df['important_keywords'] = df.apply(lambda row: detect_keywords(row['title'] + " " + row['content']), axis=1)
    records = df.to_dict(orient='records')
    inserted_count = 0
    for record in records:
        response = supabase.table("news_data").select("id").eq("date", record["date"]).eq("title", record["title"]).execute()
        if len(response.data) == 0:
            clean_record = {k: ("" if pd.isna(v) else v) for k, v in record.items()}
            supabase.table("news_data").insert(clean_record).execute()
            inserted_count += 1
    return inserted_count

# ==========================================
# 🆕 메인 실행 함수
# ==========================================
def process_drive_pdfs():
    service = get_drive_service()
    
    query = f"'{SOURCE_FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print("📭 구글 드라이브에 새로운 PDF 파일이 없습니다.")
        return

    os.makedirs("temp_downloads", exist_ok=True) 

    for item in items:
        file_id = item['id']
        file_name = item['name']
        print(f"\n📄 [{file_name}] 처리를 시작합니다...")

        request = service.files().get_media(fileId=file_id)
        file_path = os.path.join("temp_downloads", file_name)
        
        with io.FileIO(file_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        print("   - 다운로드 완료")

        try:
            news_df = extract_full_news_data(file_path)
            inserted = save_to_supabase(news_df)
            print(f"   - DB 저장 완료 (신규 데이터: {inserted}건)")
            
            service.files().update(
                fileId=file_id,
                addParents=DONE_FOLDER_ID,
                removeParents=SOURCE_FOLDER_ID
            ).execute()
            print("   - 드라이브 '완료 폴더'로 파일 이동 완료")
            
        except Exception as e:
            print(f"   ❌ 에러 발생: {e}")
        
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    print("\n✅ 모든 작업이 완료되었습니다!")

if __name__ == "__main__":
    process_drive_pdfs()
