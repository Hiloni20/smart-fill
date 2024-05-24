# import streamlit as st
# from PIL import Image

# page_icon = Image.open("pages/favicon.ico")
# st.set_page_config(page_title="Famiology", page_icon=page_icon, layout="wide", initial_sidebar_state="expanded")
# logo = Image.open("pages/favicon.ico")

# # Streamlit UI
# st.sidebar.image("FamiologyTextLogo.png", use_column_width=True)

# # st.sidebar.expander("Apps")

# # st.sidebar.expander("Dashboards")

# # st.sidebar.selectbox('Apps', options=['Document Detector', 'Smart Fill'], index=0)
# # st.sidebar.selectbox('Dashboards', options=['Dashboard'], index=1)

# st.markdown(
#     """
#     <style>
#         section[data-testid="stSidebar"] {
#             width: 400px !important; # Set the width to your desired value
#         }
#     </style>
#     """,
#     unsafe_allow_html=True,
# )



# # def main():
# #     with st.sidebar.container():
# #         # Section 1: Apps
# #         with st.expander("Apps"):
# #             st.markdown("Document Detector")
# #             st.markdown('<a href="https://famiology-smart-fill.streamlit.app/" target="_self">Smart Fill</a>', unsafe_allow_html=True)
# #         # Section 2: Dashboards
# #         with st.expander("Dashboards"):
# #             st.markdown("Dashboard")

# # if __name__ == "__main__":
# #     main()



import streamlit as st
from streamlit_option_menu import option_menu
import importlib.util
import os

def load_page(page):
    spec = importlib.util.spec_from_file_location(page, os.path.join("pages", f"{page}.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

# Main layout
st.set_page_config(layout="wide")

# Sidebar layout with Famiology logo and dropdown menus
with st.sidebar:
    st.image("FamiologyTextLogo.png", width=150)
    
    apps = option_menu("Apps", ["App 1", "App 2", "App 3"],
                       icons=["app-indicator", "app-indicator", "app-indicator"],
                       menu_icon="cast", default_index=0)
    
    dashboards = option_menu("Dashboards", ["Dashboard 1", "Dashboard 2", "Dashboard 3"],
                             icons=["graph-up", "graph-up", "graph-up"],
                             menu_icon="cast", default_index=0)

# Page loading area
st.title("Page Loading Area")

# Conditional rendering based on selected menu items
if apps == "App 1":
    load_page("app1")
elif apps == "App 2":
    load_page("app2")
elif apps == "App 3":
    load_page("app3")

if dashboards == "Dashboard 1":
    load_page("dashboard1")
elif dashboards == "Dashboard 2":
    load_page("dashboard2")
elif dashboards == "Dashboard 3":
    load_page("dashboard3")
