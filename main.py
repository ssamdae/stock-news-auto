import os
import io
import json 
import fitz
import pandas as pd
import re
import requests
from datetime import datetime
from supabase import create_client, Client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ==========================================
# 🛑 1. 환경 설정 (Secrets/Environment Variables)
# ==========================================
# Supabase 설정
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL else None

# 텔레그램 설정
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# 구글 드라이브 폴더 ID
SOURCE_FOLDER_ID = '1cxvRel_fCPVzJLAP7gd-JVDkaicPBWfn'     # 새 PDF 폴더
DONE_FOLDER_ID = '1J6K5Ko2nrKVBDCNfkyjGpjkXHHa1wyvW'       # 완료 폴더

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
# 🛑 2. 핵심 로직 (필터링 및 알림)
# ==========================================
def is_schedule_news(text):
    """일정/예상 키워드 및 날짜 패턴 감지"""
    schedule_keywords = ["예정", "계획", "개최", "발표", "출시", "추진", "예상", "전망", "일정", "앞두고", "임박", "목표", "기대", "본격화"]
    date_pattern = r'(\d{1,2}월\s*\d{1,2}일|\d{1,2}월|\d{1,2}분기|내년|내달|차주|상반기|하반기)'
    
    has_keyword = any(kw in text for kw in schedule_keywords)
    has_date = bool(re.search(date_pattern, text))
    return has_keyword or has_date

def send_telegram_message(message):
    """텔레그램 메시지 전송 (HTML 포맷)"""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ 텔레그램 설정이 누락되어 메시지를 보낼 수 없습니다.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": False}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"❌ 텔레그램 전송 실패: {e}")

def detect_keywords(text):
    keywords = ["상장", "공시", "M&A", "특허", "공급계약", "수주", "MOU", "임상"]
    found = [kw for kw in keywords if kw in text]
    return ", ".join(found) if found else ""

# ==========================================
# 🛑 3. 파싱 로직 (PDF 추출)
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
    start_marker, end_marker = "< 경제 일반 >", "< 기타 >"
    is_target_section = False
    main_margin_x0 = None 

    for page in doc:
        page_links = page.get_links()
        blocks = page.get_text("dict")["blocks"]
        for b in blocks:
            if "lines" not in b: continue
            for l in b["lines"]:
                full_line_text = "".join([s["text"] for s in l["spans"]]).replace('\xa0', ' ').strip()
                if not full_line_text: continue

                if start_marker in full_line_text and not is_target_section:
                    is_target_section, main_margin_x0 = True, l["bbox"][0]
                    continue
                if end_marker in full_line_text:
                    if current_item: all_data.append(current_item)
                    is_target_section = False
                    break
                if not is_target_section: continue

                theme_match = re.match(r'^\<(.*?)\>$', full_line_text)
                if theme_match:
                    if current_item: all_data.append(current_item)
                    current_item = None
                    current_theme, main_margin_x0 = theme_match.group(1).strip(), l["bbox"][0]
                    continue

                for s in l["spans"]:
                    text, x0 = s["text"].strip(), s["bbox"][0]
                    if not text or text in ["관련주", "•"]: continue
                    title_rect = fitz.Rect(s["bbox"])
                    link_url = next((link.get("uri", "") for link in page_links if title_rect.intersects(link["from"])), "")
                    
                    if link_url:
                        if main_margin_x0 and x0 > main_margin_x0 + 15: continue
                        if current_item: all_data.append(current_item)
                        current_item = {"date": file_date, "day": file_day, "theme": current_theme, "title": text, "url": link_url, "content": ""}
                    elif current_item:
                        current_item["content"] += (" " + text if current_item["content"] else text)
                            
    if current_item: all_data.append(current_item)
    return pd.DataFrame(all_data)

# ==========================================
# 🛑 4. DB 저장 로직
# ==========================================
def save_to_supabase(df):
    if df.empty or not supabase: return 0
    df['important_keywords'] = df.apply(lambda row: detect_keywords(str(row['title']) + " " + str(row['content'])), axis=1)
    records = df.to_dict(orient='records')
    inserted_count = 0
    for record in records:
        # 중복 체크
        res = supabase.table("news_data").select("id").eq("date", record["date"]).eq("title", record["title"]).execute()
        if len(res.data) == 0:
            clean_record = {k: ("" if pd.isna(v) else v) for k, v in record.items()}
            supabase.table("news_data").insert(clean_record).execute()
            inserted_count += 1
    return inserted_count

# ==========================================
# 🛑 5. 메인 실행 프로세스
# ==========================================
def main():
    service = get_drive_service()
    query = f"'{SOURCE_FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print("📭 처리할 신규 PDF 파일이 없습니다.")
        return

    os.makedirs("temp_downloads", exist_ok=True) 

    for item in items:
        file_id, file_name = item['id'], item['name']
        print(f"\n🚀 [{file_name}] 분석 시작")
        
        file_path = os.path.join("temp_downloads", file_name)
        request = service.files().get_media(fileId=file_id)
        with io.FileIO(file_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done: _, done = downloader.next_chunk()

        try:
            # 1. 데이터 추출
            df = extract_full_news_data(file_path)
            
            # 2. Supabase DB 저장 (전체 데이터)
            inserted = save_to_supabase(df)
            print(f"✅ DB 저장 완료 (신규 {inserted}건)")

            # 3. 일정 뉴스 필터링 및 텔레그램 전송
            schedule_df = df[df.apply(lambda r: is_schedule_news(str(r['title']) + " " + str(r['content'])), axis=1)]
            
            if not schedule_df.empty:
                msg_header = f"<b>📅 [일정 뉴스 알림] {file_name[:6]}</b>\n\n"
                msg = msg_header
                for i, row in enumerate(schedule_df.to_dict('records'), 1):
                    item_str = f"{i}. <b>[{row['theme']}]</b> {row['title']}\n🔗 <a href='{row['url']}'>기사보기</a>\n\n"
                    
                    # 텔레그램 메시지 길이 제한(4000자) 대응
                    if len(msg + item_str) > 4000:
                        send_telegram_message(msg)
                        msg = msg_header + "(계속)...\n\n"
                    msg += item_str
                
                send_telegram_message(msg)
                print(f"🔔 일정 뉴스 {len(schedule_df)}건 텔레그램 전송 완료")
            else:
                print("ℹ️ 선별된 일정 뉴스가 없습니다.")

            # 4. 완료 폴더로 이동
            service.files().update(fileId=file_id, addParents=DONE_FOLDER_ID, removeParents=SOURCE_FOLDER_ID).execute()
            print("📦 파일 이동 완료 (완료 폴더)")
            
        except Exception as e:
            print(f"❌ 에러 발생: {e}")
        finally:
            if os.path.exists(file_path): os.remove(file_path)

if __name__ == "__main__":
    main()
