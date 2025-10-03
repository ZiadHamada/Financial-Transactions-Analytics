# -*- coding: utf-8 -*-
"""
Created on Fri Aug 22 15:33:54 2025

@author: Ziad Hamada
"""
import numpy as np
import pandas as pd
import json
import os

# Define the data directory
data_path = "D:\\Data Analysis\\Projects\\Financial Transactions Dataset Analytics\\Data\\"

# Ensure the path uses the correct separator for the operating system
data_path = os.path.normpath(data_path)

# Dataset names
csv_files = ['cards_data', 'transactions_data', 'users_data']
json_files = ['mcc_codes', 'train_fraud_labels']

# Create empty lists for DataFrames
csv_dataframes = []
json_dataframes = []

# Read CSV files
for file_name in csv_files:
    file_path = os.path.join(data_path, f"{file_name}.csv")
    try:
        temp_df = pd.read_csv(file_path)
        csv_dataframes.append(temp_df)
        print(f"Successfully loaded CSV: {file_name}")
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
    except Exception as e:
        print(f"Error loading {file_path}: {str(e)}")

#Read JSON files
for file_name in json_files:
    file_path = os.path.join(data_path, f"{file_name}.json")
    try:
        with open(file_path, 'r') as file:
            dict_file = json.load(file)
        
        # Convert JSON to DataFrame
        # Adjust based on JSON structure (assuming a list of records for flexibility)
        # Handle JSON files based on their type
        if file_name == "train_fraud_labels":
            # train_fraud_labels.json is a dictionary with a "target" key
            if "target" in dict_file:
                df_json = pd.DataFrame.from_dict(
                    dict_file["target"], orient='index', columns=['fraud_label']
                ).reset_index().rename(columns={'index': 'transaction_id'})
                try:
                    df_json['transaction_id'] = pd.to_numeric(df_json['transaction_id'], errors='coerce').astype('Int64')
                except Exception as e:
                    print(f"Error converting transaction_id to integer in {file_name}: {str(e)}")
            else:
                print(f"Error: 'target' key not found in {file_name}")
                continue
        elif isinstance(dict_file, list):
            df_json = pd.DataFrame(dict_file)
        else:
            df_json = pd.DataFrame.from_dict(
                dict_file, orient='index', columns=['business_type']
                ).reset_index().rename(columns={'index': 'mcc'})
            try:
                df_json['mcc'] = pd.to_numeric(df_json['mcc'], errors='coerce').astype('Int64')
            except Exception as e:
                print(f"Error converting transaction_id to integer in {file_name}: {str(e)}")
        json_dataframes.append(df_json)
        print(f"Successfully loaded JSON: {file_name}")
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {file_path}.")
    except Exception as e:
        print(f"Error loading {file_path}: {str(e)}")

# Optional: Inspect the loaded DataFrames
print("\nCSV DataFrames loaded:", len(csv_dataframes))
for i, df in enumerate(csv_dataframes):
    print(f"CSV {csv_files[i]}: {df.shape}")

print("\nJSON DataFrames loaded:", len(json_dataframes))
for i, df in enumerate(json_dataframes):
    print(f"JSON {json_files[i]}: {df.shape}")

csv_dataframes[1] = csv_dataframes[1].merge(json_dataframes[1], left_on='id', right_on='transaction_id', how='inner')
csv_dataframes[1] = csv_dataframes[1].merge(json_dataframes[0], on='mcc', how='left')


for df in csv_dataframes:
    print(df.isna().sum() / len(df) * 100)

for df in csv_dataframes:
    print(df.info())
    

csv_dataframes[1]['merchant_state'].fillna('unKnown',inplace=True)
#csv_dataframes[1]['zip'].fillna('unKnown',inplace=True)
csv_dataframes[1]['errors'].fillna('No Error',inplace=True)

for df in csv_dataframes:
    print(df.isna().sum() / len(df) * 100)

####################################Cards data############################################

expires_cleaned = csv_dataframes[0]['expires'].astype(str).str.strip().replace(['nan', ''], np.nan)
csv_dataframes[0]['expires'] = pd.to_datetime(expires_cleaned, errors='coerce', infer_datetime_format=True)

credit_limit_cleaned = csv_dataframes[0]['credit_limit'].astype(str).str.strip().replace('nan', '')
csv_dataframes[0]['credit_limit'] =credit_limit_cleaned.str.extract('(\d+)').astype(int)

acct_open_cleaned = csv_dataframes[0]['acct_open_date'].astype(str).str.strip().replace(['nan', ''],np.nan)
csv_dataframes[0]['acct_open_date'] = pd.to_datetime(acct_open_cleaned, errors='coerce', infer_datetime_format=True)

####################################Transaction data######################################
csv_dataframes[1]['date'] = pd.to_datetime(csv_dataframes[1]['date'])

amount_cleaned = csv_dataframes[1]['amount'].astype(str).str.strip().replace('nan', '')
csv_dataframes[1]['amount'] =amount_cleaned.str.extract('(\d+\.\d+)').astype(float)


########################################User data########################################
capita_income_cleaned = csv_dataframes[2]['per_capita_income'].astype(str).str.strip().replace('nan', '')
csv_dataframes[2]['per_capita_income'] = capita_income_cleaned.str.replace(r"[^\d]", "", regex=True)
csv_dataframes[2]['per_capita_income'] = pd.to_numeric(csv_dataframes[2]['per_capita_income'], errors='coerce').astype("Int64")

yearly_income_cleaned = csv_dataframes[2]['yearly_income'].astype(str).str.strip().replace('nan', '')
csv_dataframes[2]['yearly_income'] = yearly_income_cleaned.str.replace(r"[^\d]", "", regex=True)
csv_dataframes[2]['yearly_income'] = pd.to_numeric(csv_dataframes[2]['yearly_income'], errors='coerce').astype("Int64")


total_debte_cleaned = csv_dataframes[2]['total_debt'].astype(str).str.strip().replace('nan', '')
csv_dataframes[2]['total_debt'] = total_debte_cleaned.str.replace(r"[^\d]", "", regex=True)
csv_dataframes[2]['total_debt'] = pd.to_numeric(csv_dataframes[2]['total_debt'], errors='coerce').astype("Int64")

birth_date = csv_dataframes[2].apply(lambda row: str(row['birth_month']) + '-' + str(row['birth_year']), axis=1)
csv_dataframes[2]['birth_date'] = pd.to_datetime(birth_date, errors='coerce', infer_datetime_format=True)

for df in csv_dataframes:
    print(df.info())


#Save data cleaned in csv files
csv_dataframes[1][['id','date','client_id','card_id','amount','use_chip','merchant_id','fraud_label']].to_csv('Data/transactions_table.csv', index=False, header=True)
csv_dataframes[1][['merchant_id','merchant_city','merchant_state','zip','business_type', 'errors']].to_csv('Data/merchants_table.csv', index=False, header=True)
csv_dataframes[0][['id','client_id','card_brand','card_type','card_number','expires','cvv','has_chip',
                   'num_cards_issued','credit_limit','acct_open_date', 'card_on_dark_web',
                   'year_pin_last_changed']].to_csv('Data/cards_table.csv', index=False, header=True)

csv_dataframes[2][['id','current_age','retirement_age','birth_date','gender','address','latitude','longitude',
                   'per_capita_income','yearly_income','total_debt','credit_score','num_credit_cards']].to_csv('Data/users_table.csv', index=False, header=True)