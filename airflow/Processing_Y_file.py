import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def calculate_age(birthdate):
    today = datetime.today()
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    return age

def calculate_age_family_members():
    excel_file_path = '/Users/atharvabapat/airflow/FamilyOfficeEntityDataSampleV1.1.xlsx'
    updated_excel_file_path = '/Users/atharvabapat/airflow/Y_New_Processed.xlsx'
    
    df = pd.read_excel(excel_file_path, sheet_name='Family Members', engine='openpyxl')
    df_updated = df.copy()
    df_updated['Date of Birth'] = pd.to_datetime(df_updated['Date of Birth'])
    df_updated['Age'] = df_updated['Date of Birth'].apply(calculate_age)
    
    # Save the updated DataFrame to Excel
    df_updated.to_excel(updated_excel_file_path, index=False)
    print("")
    print("Family Members Excel file updated successfully!")

with DAG(dag_id="Processing_Y_file",
         start_date=datetime(2021,1,1),
         schedule_interval='@daily',
         catchup=False) as dag:
    
    task1 = PythonOperator(
        task_id="calculate_age_family_members",
        python_callable=calculate_age_family_members)
