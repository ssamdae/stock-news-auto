import streamlit as st
from supabase import create_client, Client
import pandas as pd

st.set_page_config(page_title="뉴스 대시보드", page_icon="📈", layout="wide")
st.title("📈 뉴스 DB")

# --- 1. Supabase 연결 설정 ---
# Streamlit 클라우드의 금고(secrets)에서 URL과 KEY를 가져옵니다.
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# --- 2. 데이터 불러오기 ---
@st.cache_data(ttl=600) # 10분마다 새로고침 (DB 부하 방지)
def load_data():
    # Supabase의 news_data 테이블에서 데이터를 가져옵니다. (최신순으로 2000개)
    response = supabase.table("news_data").select("date, theme, title, content, important_keywords, url").order("date", desc=True).limit(5000).execute()
    
    # 가져온 데이터를 Pandas 데이터프레임으로 변환
    df = pd.DataFrame(response.data)
    return df

# 데이터 로드
try:
    df = load_data()
except Exception as e:
    st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")
    st.stop()

# --- 3. UI 및 검색 필터 (기존 로직과 동일) ---
if df.empty:
    st.info("데이터가 없습니다. 데이터 수집 봇이 작동했는지 확인해 주세요.")
else:
    st.sidebar.header("🔍 검색 필터")
    df['date'] = pd.to_datetime(df['date'])
    
    start_date, end_date = st.sidebar.date_input("🗓️ 날짜 범위", [df['date'].min().date(), df['date'].max().date()])
    selected_themes = st.sidebar.multiselect("🏷️ 테마 선택", options=sorted(df['theme'].unique().tolist()), default=[])
    search_keyword = st.sidebar.text_input("🔑 키워드 검색", placeholder="예: 로봇, 상장")

    if not selected_themes and not search_keyword:
        st.info("👈 왼쪽 사이드바에서 '테마'를 선택하거나 '검색어'를 입력해 주세요.")
    else:
        mask = (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)
        if selected_themes: mask &= df['theme'].isin(selected_themes)
        filtered_df = df[mask].copy()
        
        if search_keyword:
            kw_mask = (filtered_df['title'].str.contains(search_keyword, case=False, na=False) | 
                       filtered_df['content'].str.contains(search_keyword, case=False, na=False) |
                       filtered_df['important_keywords'].str.contains(search_keyword, case=False, na=False))
            filtered_df = filtered_df[kw_mask]
        
        filtered_df['date'] = filtered_df['date'].dt.strftime('%Y-%m-%d')
        st.subheader(f"총 {len(filtered_df)}개의 뉴스가 검색되었습니다.")
        st.dataframe(filtered_df[['date', 'theme', 'title', 'important_keywords', 'content', 'url']], 
                     use_container_width=True, height=800, hide_index=True,
                     column_config={"url": st.column_config.LinkColumn("기사 링크", display_text="🔗 기사 보기")})
