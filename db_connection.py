from sqlalchemy import create_engine
from ETL_for_database import ETL_Pipeline
import os
import pandas as pd
import numpy as np

path = "D:\\Data Analysis\\Projects\\Financial Transactions Dataset Analytics\\Data\\"
db_name = 'FinancialTransactions'
def connect_to_db():
    conn_string = f'mssql+pyodbc://sa:zezo450@localhost/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&timeout=0'
    engine = create_engine(conn_string)
    return engine


class Tran_transactions(ETL_Pipeline):
    def transform_data(self,df):
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date'] = df['date'].dt.strftime("%Y-%m-%d")
        return df
    
class Tran_users(ETL_Pipeline):
    def transform_data(self,df):
        # df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce', format='mixed')
        # if np.issubdtype(df['birth_date'].dtype, np.number):
        #     df['birth_date'] = pd.to_datetime(df['birth_date'].astype(str), format='%Y%m', errors='coerce')
        #df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce').dt.date
        birth_date_cleaned = df['birth_date'].astype(str).str.strip().replace(['nan', ''], np.nan)
        df['birth_date'] = pd.to_datetime(birth_date_cleaned, errors='coerce').dt.date
        return df
    
class Tran_cards(ETL_Pipeline):
    def transform_data(self,df):
        expires_cleaned = df['expires'].astype(str).str.strip().replace(['nan', ''], np.nan)
        df['expires'] = pd.to_datetime(expires_cleaned, errors='coerce').dt.date
        acct_open_date_cleaned = df['acct_open_date'].astype(str).str.strip().replace(['nan', ''], np.nan)
        df['acct_open_date'] = pd.to_datetime(acct_open_date_cleaned, errors='coerce').dt.date
        df['has_chip'] = df['has_chip'].astype(str).str.lower().str.capitalize()
        return df

def etl_pipeline():
    etl = ETL_Pipeline()
    t_users = Tran_users()
    t_transactions = Tran_transactions()
    t_cards = Tran_cards()
    engine = connect_to_db()
    
    table_map = {
    "cards_table.csv": (t_cards, "Cards"),
    "merchants_table.csv": (etl, "Merchants"),
    "transactions_table.csv": (t_transactions, "Transactions"),
    "users_table.csv": (t_users, "Users"),
    }
    table_cols = {
    "Cards": ['Id', 'Client_id', 'Card_brand', 'Card_type', 'Card_number', 'Expire_date', 'CVV', 'Has_chip',
              'Num_cards_issued', 'Credit_limit', 'Acct_open_date', 'Card_on_dark_web', 'Year_pin_last_changed'],
      
    "Merchants": ['Id', 'City', 'State', 'Zip', 'Business_type', 'Error'],
      
    "Transactions": ['Id', 'Date', 'Amount', 'Use_chip', 'Fraud_label', 'client_id', 'card_id', 'merchant_id'],
      
    "Users":['Id', 'Age', 'Retirement_age', 'Birth_date', 'Gender', 'Address', 'Latitude', 'Longitude',
             'Per_capita_income', 'Yearly_income', 'Total_dept', 'Credit_score', 'Num_credit_cards'],
    }
    for file_name in table_map.keys():
        transformer, table_name = table_map[file_name]
        file_path = os.path.join(path, file_name)
        df = etl.extract_data(file_path)
        print(f"Rows in {file_name}: {len(df)}")
        df_transformed = transformer.transform_data(df)
        df_transformed.columns = table_cols[table_name]
        etl.load_data_to_sql(df_transformed, table_name, 5000, engine)
        print(f"Processing: {file_name} → Loading into {table_name}")
        print(df_transformed.head())

# Run the ETL pipeline
if __name__ == "__main__":
    etl_pipeline()