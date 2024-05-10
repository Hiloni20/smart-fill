# import requests
# import streamlit as st

# def preprocess_excel_files(selected_files):
#     if not selected_files:
#         st.error("Please select at least one file to preprocess.")
#         return
    
#     api_url = 'http://localhost:8080/api/v1/dags/preprocess_extract_columns/dagRuns'
#     username = 'admin'
#     password = 'Hilonib20!'
#     response = requests.post(api_url, auth=(username, password), json={})

#     if response.status_code == 200:
#         st.success("Airflow DAG triggered successfully.")
#     else:
#         st.error("Failed to trigger Airflow DAG. Status code: {}".format(response.status_code))

# def main():
#     st.title('Excel Preprocessing')

#     client_profile = st.checkbox('Client Profile', value=False)
#     family_members = st.checkbox('Family Members', value=False)
#     financial_assets = st.checkbox('Financial Assets', value=False)

#     selected_files = []
#     if client_profile:
#         selected_files.append('client_profile.xlsx')
#     if family_members:
#         selected_files.append('family_members.xlsx')
#     if financial_assets:
#         selected_files.append('financial_assets.xlsx')

#     if st.button('Preprocess selected files'):
#         preprocess_excel_files(selected_files)

# if __name__ == '__main__':
#     main()




import pandas as pd
import streamlit as st
from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator import BashOperator

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

def preprocess_selected_files(selected_files):
    for file_name in selected_files:
        file_path = f'pages/{file_name}'
        try:
            df = pd.read_excel(file_path)
            original_indices = df.index

            numeric_columns = df.select_dtypes(include=[float, int]).columns

            for col in numeric_columns:
                if '%' in col:  
                    df[col] = df[col].str.rstrip('%').astype(float) / 100.0
                else:
                    df[col] = df[col].fillna(df[col].mean())

            if 'Date of Birth' in df.columns:
                df['Date of Birth'] = pd.to_datetime(df['Date of Birth'], errors='coerce')
                df = df.dropna(subset=['Date of Birth'])
                df['Age'] = datetime.now().year - df['Date of Birth'].dt.year
                df['Generation'] = df['Date of Birth'].dt.year.apply(categorize_generation)

            df = df.reindex(original_indices)

            preprocessed_file_path = file_path.replace('.xlsx', '_preprocessed.xlsx')
            df.to_excel(preprocessed_file_path, index=False)
            st.write(f"Preprocessed file saved: {preprocessed_file_path}")
        except Exception as e:
            st.error(f"Error preprocessing file {file_name}: {str(e)}")

def categorize_generation(year):
    if year >= 1997:
        return 'Gen Z'
    elif year >= 1981:
        return 'Millennials'
    elif year >= 1965:
        return 'Gen X'
    elif year >= 1946:
        return 'Baby Boomers'
    else:
        return 'Silent Generation'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 28),
    'retries': 1,
}

dag = DAG(
    'preprocess_extract_columns',
    default_args=default_args,
    description='A DAG to preprocess Excel files and extract columns',
    schedule=None,
)

preprocess_files_task = PythonOperator(
    task_id='preprocess_selected_files',
    python_callable=preprocess_selected_files,
    dag=dag,
)

streamlit_task = BashOperator(
    task_id='run_streamlit_app',
    bash_command='streamlit run /Users/hilonibhimani/airflow/dags/st.py',  
    dag=dag,
)

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
        if selected_files:
            with st.spinner('Preprocessing files...'):
                preprocess_selected_files(selected_files)
                st.success('Preprocessing completed successfully.')
        else:
            st.error("Please select at least one file to preprocess.")

if __name__ == '__main__':
    main()

