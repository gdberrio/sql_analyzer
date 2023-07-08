import streamlit as st

st.set_page_config(
    page_title="SQL Analyzer",
    layout="centered",
    initial_sidebar_state="auto",
    menu_items={
        "Get Help": "https://streamlit.io/",
        "Report a bug": "https://github.com",
        "About": "**SQL Analyzer**",
    },
)

st.sidebar.title("Main menu")
st.title("SQL Analyzer")

hide_footer_style = """
<style>
.main footer {visibility: hidden;}
</style>
"""

st.markdown(hide_footer_style, unsafe_allow_html=True)

hide_menu_style = """
<style>
#MainMenu {visibility: hidden;}
</style>
"""

st.markdown(hide_menu_style, unsafe_allow_html=True)

st.sidebar.markdown(
    """
<div class="markdown-text-container stText" style="width: 698px;">
<footer><p></p><div style="font-size: 12px;">
SQL Analyzer v 0.1</div>
<div style="font size: 12px;">widibeo.ai</div>
<div>
""",
    unsafe_allow_html=True,
)
