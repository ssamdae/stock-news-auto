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

# 💡 수정 1: 날짜 선택 시 발생하는 ValueError 방지 (길이 체크)
date_range = st.sidebar.date_input("🗓️ 날짜 범위", [default_start.date(), today.date()])

if len(date_range) == 2:
    start_date, end_date = date_range
else:
    # 사용자가 아직 날짜를 1개만 선택했을 때 (시작일 = 종료일로 임시 처리)
    start_date = date_range[0]
    end_date = date_range[0]

available_themes = get_unique_themes()
selected_themes = st.sidebar.multiselect("🏷️ 테마 선택", options=available_themes, default=[])
search_keyword = st.sidebar.text_input("🔑 키워드 검색", placeholder="예: 로봇, 상장")

# --- 4. DB에서 조건에 맞는 데이터만 직접 불러오기 ---
@st.cache_data(ttl=600) # 10분마다 새로고침
def fetch_filtered_data(start_d, end_d, themes, keyword):
    # 기본 쿼리 시작 (important_keywords는 내부 검색을 위해 함께 불러옵니다)
    query = supabase.table("news_data").select("date, theme, title, content, important_keywords, url")
    
    # [조건 1] 날짜 필터링
    query = query.gte("date", start_d.strftime("%Y-%m-%d"))
    query = query.lte("date", end_d.strftime("%Y-%m-%d"))
    
    # [조건 2] 테마 필터링
    if themes:
        query = query.in_("theme", themes)
        
    # [조건 3] 키워드 검색 (제목, 내용, 키워드 중 하나라도 포함되면 가져옴)
    if keyword:
        or_condition = f"title.ilike.%{keyword}%,content.ilike.%{keyword}%,important_keywords.ilike.%{keyword}%"
        query = query.or_(or_condition)
        
    # 최종적으로 최신순 정렬 후 최대 2000개까지만 가져오기
    response = query.order("date", desc=True).limit(2000).execute()
    
    return pd.DataFrame(response.data)

# --- 5. 결과 출력 ---
if not selected_themes and not search_keyword:
    st.info("👈 왼쪽 사이드바에서 '테마'를 선택하거나 '검색어'를 입력해 주세요.")
else:
    with st.spinner('데이터를 불러오는 중입니다...'):
        try:
            df = fetch_filtered_data(start_date, end_date, selected_themes, search_keyword)
            
            if df.empty:
                st.warning("선택하신 조건에 맞는 뉴스가 없습니다.")
            else:
                # 출력용 날짜 포맷 변경
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                
                st.subheader(f"총 {len(df)}개의 뉴스가 검색되었습니다.")
                
                # 💡 수정 2 & 3: 화면 표시 목록에서 'important_keywords' 제외, width='stretch' 사용
                st.dataframe(
                    df[['date', 'theme', 'title', 'content', 'url']], 
                    width='stretch', 
                    height=800, 
                    hide_index=True,
                    column_config={
                        "url": st.column_config.LinkColumn("기사 링크", display_text="🔗 기사 보기")
                    }
                )
        except Exception as e:
            st.error(f"데이터베이스 조회 중 오류가 발생했습니다: {e}")
