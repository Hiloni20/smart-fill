import requests
import streamlit as st

def preprocess_excel_files(selected_files):
    if not selected_files:
        st.error("Please select at least one file to preprocess.")
        return
    
    airflow_api_url = 'http://localhost:8080/api/v1/dags/preprocess_extract_columns/dagRuns'
    username = "admin"
    password = "Hilonib20!"
    auth = (username, password)
    headers = {"Content-type": "application/json"}
    payload = {
        "conf": {},
        "note": "string"
    }
    
    try:
        response = requests.post(airflow_api_url, headers=headers, auth=auth, json=payload)
        response.raise_for_status() 
        st.success("Airflow DAG triggered successfully.")
    except requests.exceptions.ConnectionError as e:
        st.error("Failed to connect to the Airflow server. Make sure the server is running and accessible at http://localhost:8080.")
    except requests.exceptions.HTTPError as e:
        st.error(f"Failed to trigger Airflow DAG: {e}")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")

def main():
    st.title('Excel Preprocessing')

    client_profile = st.checkbox('Client Profile', value=False)
    family_members = st.checkbox('Family Members', value=False)
    financial_assets = st.checkbox('Financial Assets', value=False)

    selected_files = []
    if client_profile:
        selected_files.append('client_profile.xlsx')
    if family_members:
        selected_files.append('family_members.xlsx')
    if financial_assets:
        selected_files.append('financial_assets.xlsx')

    if st.button('Preprocess selected files'):
        preprocess_excel_files(selected_files)

if __name__ == '__main__':
    main()
