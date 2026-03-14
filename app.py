import streamlit as st
from supabase import create_client, Client
import pandas as pd
from datetime import datetime, timedelta

st.set_page_config(page_title="뉴스 대시보드", page_icon="📈", layout="wide")
st.title("📈 뉴스 DB")

# --- 1. Supabase 연결 설정 ---
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# --- 2. 테마 목록만 가볍게 불러오기 (사이드바 선택용) ---
@st.cache_data(ttl=3600) # 테마 목록은 1시간마다 갱신
def get_unique_themes():
    # 테마 컬럼만 가져와서 중복을 제거합니다.
    response = supabase.table("news_data").select("theme").limit(10000).execute()
    themes = list(set([row['theme'] for row in response.data if row['theme']]))
    return sorted(themes)

# --- 3. UI 및 검색 필터 (먼저 배치) ---
st.sidebar.header("🔍 검색 필터")

# 기본 날짜 설정 (최근 한 달)
today = datetime.today()
default_start = today - timedelta(days=30)

start_date, end_date = st.sidebar.date_input("🗓️ 날짜 범위", [default_start.date(), today.date()])
available_themes = get_unique_themes()
selected_themes = st.sidebar.multiselect("🏷️ 테마 선택", options=available_themes, default=[])
search_keyword = st.sidebar.text_input("🔑 키워드 검색", placeholder="예: 로봇, 상장")

# --- 4. DB에서 조건에 맞는 데이터만 직접 불러오기 ---
@st.cache_data(ttl=600) # 10분마다 새로고침
def fetch_filtered_data(start_d, end_d, themes, keyword):
    # 기본 쿼리 시작
    query = supabase.table("news_data").select("date, theme, title, content, important_keywords, url")
    
    # [조건 1] 날짜 필터링 (gte: 크거나 같음, lte: 작거나 같음)
    query = query.gte("date", start_d.strftime("%Y-%m-%d"))
    query = query.lte("date", end_d.strftime("%Y-%m-%d"))
    
    # [조건 2] 테마 필터링 (in_: 리스트 안에 포함되는지)
    if themes:
        query = query.in_("theme", themes)
        
    # [조건 3] 키워드 검색 (or_ 조건과 ilike를 사용해 제목, 내용, 키워드 중 하나라도 포함되면 가져옴)
    if keyword:
        # DB 쿼리용 문자열 포맷팅 (%키워드% 형태로 부분 일치 검색)
        or_condition = f"title.ilike.%{keyword}%,content.ilike.%{keyword}%,important_keywords.ilike.%{keyword}%"
        query = query.or_(or_condition)
        
    # 최종적으로 최신순 정렬 후 최대 2000개(원하는 만큼 조절 가능)까지만 가져오기
    response = query.order("date", desc=True).limit(2000).execute()
    
    return pd.DataFrame(response.data)

# --- 5. 결과 출력 ---
if not selected_themes and not search_keyword:
    st.info("👈 왼쪽 사이드바에서 '테마'를 선택하거나 '검색어'를 입력해 주세요.")
else:
    with st.spinner('데이터를 불러오는 중입니다...'):
        try:
            # 위에서 입력받은 필터 값을 함수에 전달
            df = fetch_filtered_data(start_date, end_date, selected_themes, search_keyword)
            
            if df.empty:
                st.warning("선택하신 조건에 맞는 뉴스가 없습니다.")
            else:
                # 출력용 날짜 포맷 변경
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                
                st.subheader(f"총 {len(df)}개의 뉴스가 검색되었습니다.")
                st.dataframe(
                    df[['date', 'theme', 'title', 'important_keywords', 'content', 'url']], 
                    use_container_width=True, 
                    height=800, 
                    hide_index=True,
                    column_config={
                        "url": st.column_config.LinkColumn("기사 링크", display_text="🔗 기사 보기")
                    }
                )
        except Exception as e:
            st.error(f"데이터베이스 조회 중 오류가 발생했습니다: {e}")
