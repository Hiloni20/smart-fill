# import streamlit as st
# import subprocess

# def execute_airflow_dag():
#     command = "airflow dags trigger --conf '{}' preprocess_extract_columns"
#     subprocess.run(command, shell=True)

# def main():
#     def execute_tasks():
#         execute_airflow_dag()
#         streamlit_command = "streamlit run /Users/hilonibhimani/airflow/dags/st.py"
#         subprocess.Popen(streamlit_command, shell=True)
#     execute_tasks()

# if __name__ == '__main__':
#     main()


import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import subprocess
import streamlit as st

def preprocess_excel_files(selected_files):
    if not selected_files:
        st.error("Select files to preprocess.")
        return

    for file_name in selected_files:
        file_path = f'/Users/hilonibhimani/airflow/dags/{file_name}'
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

def execute_airflow_dag():
    command = "airflow dags trigger --conf '{}' preprocess_extract_columns"
    subprocess.run(command, shell=True)

def main():
    st.title('Excel Preprocessing')

    selected_files_client_profile = st.checkbox('Client Profile')
    selected_files_family_members = st.checkbox('Family Members')
    selected_files_financial_assets = st.checkbox('Financial Assets')

    selected_files = []
    if selected_files_client_profile:
        selected_files.append('client_profile.xlsx')
    if selected_files_family_members:
        selected_files.append('family_members.xlsx')
    if selected_files_financial_assets:
        selected_files.append('financial_assets.xlsx')

    if st.button('Preprocess selected files'):
        with st.spinner('Preprocessing files...'):
            preprocess_selected_files(selected_files)
            st.success('Preprocessing completed successfully.')
            st.write('Triggering Airflow DAG...')
            execute_airflow_dag()

if __name__ == '__main__':
    main()

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
    schedule_interval=None,
)

preprocess_files_task = PythonOperator(
    task_id='preprocess_excel_files',
    python_callable=preprocess_excel_files,
    dag=dag,
)

extract_columns_task = PythonOperator(
    task_id='extract_columns_and_save',
    python_callable=extract_columns_and_save,
    op_kwargs={
        'file_paths': ['/Users/hilonibhimani/airflow/dags/client_profile_preprocessed.xlsx',
                       '/Users/hilonibhimani/airflow/dags/family_members_preprocessed.xlsx',
                       '/Users/hilonibhimani/airflow/dags/financial_assets_preprocessed.xlsx'],
        'output_file_path': '/Users/hilonibhimani/airflow/dags/extracted_columns.xlsx',
        'columns_to_extract': ['ClientID', 'First Name', 'Last Name', 'Contact Information', 
                               'Generation', 'Asset Type', 'Asset Details', 'Value']
    },
    dag=dag,
)

streamlit_task = BashOperator(
    task_id='run_streamlit_app',
    bash_command='streamlit run /Users/hilonibhimani/Desktop/air.py',  
    dag=dag,
)

preprocess_files_task >> extract_columns_task >> streamlit_task
