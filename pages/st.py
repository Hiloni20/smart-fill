import requests
import streamlit as st

def preprocess_selected_files(selected_files):
    if not selected_files:
        st.error("Please select at least one file to preprocess.")
        return
    
    # Make a POST request to trigger the Airflow DAG
    response = requests.post('http://localhost:8080/api/v1/dags/preprocess_extract_columns/dag_runs',
                             json={})
    if response.status_code == 200:
        st.success("Airflow DAG triggered successfully.")
    else:
        st.error("Failed to trigger Airflow DAG.")

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
        preprocess_selected_files(selected_files)

if __name__ == '__main__':
    main()
