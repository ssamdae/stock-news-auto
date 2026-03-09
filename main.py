import os
import io
import json # 추가됨
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
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# 🛑 2. Google Drive 설정
# ==========================================
# 다운받은 JSON 파일이 main.py와 같은 폴더에 있어야 합니다.
SERVICE_ACCOUNT_FILE = 'credentials.json' 
SOURCE_FOLDER_ID = '1cxvRel_fCPVzJLAP7gd-JVDkaicPBWfn'     # 새 PDF가 올라오는 폴더
DONE_FOLDER_ID = '1J6K5Ko2nrKVBDCNfkyjGpjkXHHa1wyvW'       # 파싱이 끝난 PDF를 옮길 폴더

# --- 구글 드라이브 인증 함수 ---
def get_drive_service():
    # 클라우드(GitHub) 실행 시: 환경 변수에 저장된 JSON 문자열을 읽어옴
    if "GCP_CREDENTIALS" in os.environ:
        creds_info = json.loads(os.environ["GCP_CREDENTIALS"])
        creds = service_account.Credentials.from_service_account_info(
            creds_info, scopes=['https://www.googleapis.com/auth/drive']
        )
    # 로컬(내 PC) 실행 시: credentials.json 파일을 읽어옴
    else:
        creds = service_account.Credentials.from_service_account_file(
            'credentials.json', scopes=['https://www.googleapis.com/auth/drive']
        )
    return build('drive', 'v3', credentials=creds)

# --- 기존 파싱 로직 (변경 없음) ---
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
                for s in l["spans"]:
                    text = s["text"].strip()
                    if not text: continue
                    x0 = s["bbox"][0]
                    if text in ["관련주", "•"]: continue
                    
                    if start_marker in text:
                        is_target_section = True
                        current_theme = "경제 일반" 
                        main_margin_x0 = x0 
                        continue
                    if end_marker in text:
                        if current_item: all_data.append(current_item)
                        is_target_section = False
                        break
                    if not is_target_section: continue

                    theme_match = re.match(r'< (.*?) >', text)
                    if theme_match:
                        if current_item:
                            all_data.append(current_item)
                            current_item = None
                        current_theme = theme_match.group(1).strip()
                        main_margin_x0 = x0 
                        continue

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
# 🆕 메인 실행 함수: 드라이브 감시 -> 다운로드 -> 파싱 -> 이동
# ==========================================
def process_drive_pdfs():
    service = get_drive_service()
    
    # 1. 소스 폴더에서 PDF 파일 목록 조회
    query = f"'{SOURCE_FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print("📭 구글 드라이브에 새로운 PDF 파일이 없습니다.")
        return

    os.makedirs("temp_downloads", exist_ok=True) # 임시 다운로드 폴더 생성

    for item in items:
        file_id = item['id']
        file_name = item['name']
        print(f"\n📄 [{file_name}] 처리를 시작합니다...")

        # 2. 파일 다운로드
        request = service.files().get_media(fileId=file_id)
        file_path = os.path.join("temp_downloads", file_name)
        
        with io.FileIO(file_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        print("   - 다운로드 완료")

        # 3. 파싱 및 DB 저장
        try:
            news_df = extract_full_news_data(file_path)
            inserted = save_to_supabase(news_df)
            print(f"   - DB 저장 완료 (신규 데이터: {inserted}건)")
            
            # 4. 처리가 끝난 파일을 '완료 폴더'로 이동 (중복 방지)
            service.files().update(
                fileId=file_id,
                addParents=DONE_FOLDER_ID,
                removeParents=SOURCE_FOLDER_ID
            ).execute()
            print("   - 드라이브 '완료 폴더'로 파일 이동 완료")
            
        except Exception as e:
            print(f"   ❌ 에러 발생: {e}")
        
        finally:
            # 5. 로컬에 남은 임시 PDF 파일 삭제
            if os.path.exists(file_path):
                os.remove(file_path)

    print("\n✅ 모든 작업이 완료되었습니다!")

if __name__ == "__main__":
    process_drive_pdfs()
