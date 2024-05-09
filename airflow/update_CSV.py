from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

def calculate_age(birthdate):
    today = datetime.today()
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    return age

def read_and_update_excel():
    excel_file_path = '/Users/atharvabapat/airflow/FamilyOfficeEntityDataSampleV1.1.xlsx'
    updated_excel_file_path = '/Users/atharvabapat/airflow/updatedX_sheet_FamilyOfficeEntityDataSampleV1.1.xlsx'
    
    # Read Excel file using 'openpyxl' engine
    df = pd.read_excel(excel_file_path, sheet_name='Client Profile', engine='openpyxl')
    
    # Make a copy of the DataFrame
    df_updated = df.copy()
    
    # Convert 'Date of Birth' column to datetime
    df_updated['Date of Birth'] = pd.to_datetime(df_updated['Date of Birth'])
    
    # Calculate age
    df_updated['Age'] = df_updated['Date of Birth'].apply(calculate_age)
    
    # Save updated Excel file using 'openpyxl' engine
    with pd.ExcelWriter(updated_excel_file_path, engine='openpyxl') as writer:
        df_updated.to_excel(writer, index=False)
    
    print("Excel file updated successfully!")

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id="update_CSV",
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    task1 = PythonOperator(
        task_id="read_and_update_excel",
        python_callable=read_and_update_excel)

    task1
