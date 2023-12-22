import json
import pandas as pd
import sys
import os

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Get the parent directory
parent_dir = os.path.dirname(current_dir)

# Add the parent directory to sys.path
sys.path.append(parent_dir)
from helper_function.write_db import write_dataframe_to_sqlite
import helper_function.misc as helper

# Load the JSON data
file_path = 'data/hospitcal_pricing/HSSStandardCharges.json'

with open(file_path, 'r') as file:
    json_data = json.load(file)

hospital_id = 330270

# TODO - pull from seperate file
billing_category = ['professional','facility']
setting_category = ['inpatient','outpatient','both']

# Import Data
commercial_mrf_data = json_data["Commercial MRF"]["data"]
government_mrf_data = json_data["Govt MRF"]["data"]
cash_discount_mrf_data = json_data["Charge & Disc Cash Price"]["data"]

cash_discount_mrf_data_df = pd.DataFrame.from_dict(cash_discount_mrf_data)

# Gross Charge
cash_discount_mrf_data_df["hospital_id"] = hospital_id

cash_discount_mrf_data_df = cash_discount_mrf_data_df.rename(columns={'Description of Item/Service': 'description', 'HCPCS': 'hcpcs_cpt', 
                                                                      "Rev Code": "rev_code", 'Charge': 'standard_charge'})

cash_mrf_data_df = cash_discount_mrf_data_df[['description', 'hcpcs_cpt','rev_code', 'standard_charge', 'hospital_id']]

cash_mrf_data_df["code"] = cash_mrf_data_df["hcpcs_cpt"]
cash_mrf_data_df["rate_category"] = "gross"
cash_mrf_data_df["payer_name"] = "Gross_Charge"

# Discount Charge
discounted_mrf_data_df = cash_discount_mrf_data_df[['description', 'hcpcs_cpt', 'rev_code','Discounted \nCash Price', 'hospital_id']]
discounted_mrf_data_df = discounted_mrf_data_df.rename(columns={"Discounted \nCash Price":"standard_charge"})
discounted_mrf_data_df["code"] = cash_mrf_data_df["hcpcs_cpt"]
discounted_mrf_data_df["rate_category"] = "cash"
discounted_mrf_data_df["payer_name"] = "Cash_Charge"

# Goverment Insurance 
government_mrf_data_df = pd.DataFrame.from_dict(government_mrf_data)

# Note Insurance plan might changes in the future 

government_mrf_data_df = government_mrf_data_df.rename(columns={"Minimum Negotiated Rate": "Min_Negotiated_Rate", "Maximum Negotiated Rate": "Max_Negotiated_Rate"})
core_columns = ['Location', 'Setting', 'Billing Class', 'Code', 'Code Type']
insurance_company_columns = list(set(government_mrf_data_df.columns) - set(core_columns))

government_mrf_data_df = government_mrf_data_df.melt(id_vars=core_columns, value_vars=insurance_company_columns, var_name='payer_name', value_name='standard_charge')

government_mrf_data_df['Billing Class'] = government_mrf_data_df['Billing Class'].apply(lambda x: helper.get_best_match(x, billing_category))
government_mrf_data_df['Setting'] = government_mrf_data_df['Setting'].apply(lambda x: helper.get_best_match(x, setting_category))

government_mrf_data_df = government_mrf_data_df.rename(columns={'Setting':"setting", 'Billing Class': "billing_class", 'Code': "code"})

government_mrf_data_df["ms_drg"] = government_mrf_data_df.apply(lambda row: row["code"] if row["Code Type"] == "MS-DRG" else None, axis=1)
government_mrf_data_df["apc"] = government_mrf_data_df.apply(lambda row: row["code"] if row["Code Type"] == "APC" else None, axis=1)
government_mrf_data_df["hospital_id"] = hospital_id
government_mrf_data_df["rate_category"] = "negotiated"

government_mrf_data_df["rate_category"] = government_mrf_data_df.apply(lambda row: "min" if row["payer_name"] == "Min_Negotiated_Rate" else row["rate_category"], axis=1)
government_mrf_data_df["rate_category"] = government_mrf_data_df.apply(lambda row: "max" if row["payer_name"] == "Max_Negotiated_Rate" else row["rate_category"], axis=1)


government_mrf_data_df = government_mrf_data_df[['setting', 'billing_class', 'code', 'payer_name', 'standard_charge', 'ms_drg', 'apc', "hospital_id", "rate_category"]]


# Commercial Insurance
commercial_mrf_data_df = pd.DataFrame.from_dict(commercial_mrf_data)

# Note Insurance plan might changes in the future 

commercial_mrf_data_df = commercial_mrf_data_df.rename(columns={"Minimum Negotiated Rate": "Min_Negotiated_Rate", "Maximum Negotiated Rate": "Max_Negotiated_Rate"})
core_columns = ['Location', 'Setting', 'Billing Class', 'Code', 'Code Type']
insurance_company_columns = list(set(commercial_mrf_data_df.columns) - set(core_columns))

commercial_mrf_data_df = commercial_mrf_data_df.melt(id_vars=core_columns, value_vars=insurance_company_columns, var_name='payer_name', value_name='standard_charge')

commercial_mrf_data_df['Billing Class'] = commercial_mrf_data_df['Billing Class'].apply(lambda x: helper.get_best_match(x, billing_category))
commercial_mrf_data_df['Setting'] = commercial_mrf_data_df['Setting'].apply(lambda x: helper.get_best_match(x, setting_category))

commercial_mrf_data_df = commercial_mrf_data_df.rename(columns={'Setting':"setting", 'Billing Class': "billing_class", 'Code': "code"})

commercial_mrf_data_df["ms_drg"] = commercial_mrf_data_df.apply(lambda row: row["code"] if row["Code Type"] == "MSDRG" else None, axis=1)
commercial_mrf_data_df["hospital_id"] = hospital_id

commercial_mrf_data_df["rate_category"] = "negotiated"
commercial_mrf_data_df["rate_category"] = commercial_mrf_data_df.apply(lambda row: "min" if row["payer_name"] == "Min_Negotiated_Rate" else row["rate_category"], axis=1)
commercial_mrf_data_df["rate_category"] = commercial_mrf_data_df.apply(lambda row: "max" if row["payer_name"] == "Max_Negotiated_Rate" else row["rate_category"], axis=1)


commercial_mrf_data_df = commercial_mrf_data_df[['setting', 'billing_class', 'code', 'payer_name', 'standard_charge', 'ms_drg', "hospital_id", "rate_category"]]

data_write = [cash_mrf_data_df, discounted_mrf_data_df, government_mrf_data_df, commercial_mrf_data_df]
for data in data_write:
    write_dataframe_to_sqlite(data, 'rate', 'data/hospital_pricing.db')