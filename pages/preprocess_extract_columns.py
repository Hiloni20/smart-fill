import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime

def preprocess_excel_files():
    file_paths = ['/Users/hilonibhimani/airflow/dags/client_profile.xlsx',
                  '/Users/hilonibhimani/airflow/dags/family_members.xlsx',
                  '/Users/hilonibhimani/airflow/dags/financial_assets.xlsx']
    
    for file_path in file_paths:
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
        print(f"Preprocessed file saved: {preprocessed_file_path}")

    print("Excel files preprocessed successfully.")

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

def extract_columns_and_save(file_paths, output_file_path, columns_to_extract):
    dfs = []
    for file_path in file_paths:
        df = pd.read_excel(file_path)
        print("Column Names:", df.columns)
        
        present_columns = [col for col in columns_to_extract if col in df.columns]
        
        if present_columns:
            extracted_df = df[present_columns]
            dfs.append(extracted_df)
        else:
            print(f"No specified columns found in the DataFrame from file: {file_path}")
    
    if dfs:
        result_df = pd.concat(dfs, axis=1)
        result_df.to_excel(output_file_path, index=False)
        print(f"Extracted columns saved to: {output_file_path}")
    else:
        print("No valid dataframes to concatenate. Exiting task.")

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
    bash_command='streamlit run st.py',  
    dag=dag,
)

preprocess_files_task >> extract_columns_task
