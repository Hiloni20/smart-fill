import streamlit as st
from PIL import Image

page_icon = Image.open("pages/favicon.ico")
st.set_page_config(page_title="Famiology", page_icon=page_icon, layout="wide", initial_sidebar_state="expanded")
logo = Image.open("pages/favicon.ico")

# Streamlit UI
st.sidebar.image("FamiologyTextLogo.png", use_column_width=True)

# st.sidebar.expander("Apps")

# st.sidebar.expander("Dashboards")

# st.sidebar.selectbox('Apps', options=['Document Detector', 'Smart Fill'], index=0)
# st.sidebar.selectbox('Dashboards', options=['Dashboard'], index=1)

st.markdown(
    """
    <style>
        section[data-testid="stSidebar"] {
            width: 400px !important; # Set the width to your desired value
        }
    </style>
    """,
    unsafe_allow_html=True,
)



# def main():
#     with st.sidebar.container():
#         # Section 1: Apps
#         with st.expander("Apps"):
#             st.markdown("Document Detector")
#             st.markdown('<a href="https://famiology-smart-fill.streamlit.app/" target="_self">Smart Fill</a>', unsafe_allow_html=True)
#         # Section 2: Dashboards
#         with st.expander("Dashboards"):
#             st.markdown("Dashboard")

# if __name__ == "__main__":
#     main()
