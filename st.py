import pandas as pd
import streamlit as st
from datetime import datetime

def preprocess_selected_files(selected_files):
    if not selected_files:
        st.error("Please select at least one file to preprocess.")
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
