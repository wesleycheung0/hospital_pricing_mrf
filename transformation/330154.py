import pandas as pd
import numpy as np
import json
import sys
import os

# Get the directory of the current script
current_dir = os.getcwd() 

# Get the parent directory
parent_dir = os.path.dirname(current_dir)

# Add the parent directory to sys.path
sys.path.append(parent_dir)


from helper_function.write_db import write_dataframe_to_sqlite
import helper_function.misc as helper
from helper_function.metadata import setting_category

# Function to try conversion to string, or return -1
def try_convert_to_float(value):
    if pd.isna(value):
        # Check for both None and NaN values
        return value
    try:
        # Attempt to convert to float
        return float(value)
    except ValueError:
        # If conversion fails, return -1
        return -1

    
def contains_alphabet(s):
    # Check if s is np.nan
    if isinstance(s, float) and np.isnan(s):
        return False

    s = str(s)
    return any(char.isalpha() for char in s)

data_path = "data/hospitcal_pricing/131924236_memorial-hospital-for-cancer-and-allied-diseases-nyc_standardcharges_test.json"
hospital_id = 330154

with open(data_path, 'r') as file:
    json_data = json.load(file)

op_data = pd.DataFrame.from_dict(json_data["OP_NYC_07272023"])
ip_data = pd.DataFrame.from_dict(json_data["IP_NYC_07272023"])
phy_data = pd.DataFrame.from_dict(json_data["IP_NYC_07272023"])

core_col_op = ['CHG CD', 'CHG CD DESC', 'MOD', 'MCD CPT', 'REV CD']
insurer_col_op = list(set(op_data.columns) - set(core_col_op))

core_col_phy = ['CHG CD', 'CHG CD DESC', 'MOD', 'MCD CPT/MS DRG', 'REV CD']
insurer_col_phy = list(set(phy_data.columns) - set(core_col_phy))

core_col_ip = ['CHG CD', 'CHG CD DESC', 'MOD', 'MCD CPT/MS DRG', 'REV CD']
insurer_col_ip = list(set(ip_data.columns) - set(core_col_ip))

def transformation_op (data, core_column):

    data = data.melt(id_vars=core_column, var_name="payer_name", value_name="standard_charge")

    # standard charge column cleaning 
    # op_data.standard_charge = op_data.standard_charge.str.replace(',', '')

    data["additional_generic_notes"] = data.apply(lambda row: f"standard_charge: {row['standard_charge']}" if contains_alphabet(row["standard_charge"]) else None, axis =1)
    data.standard_charge = data.standard_charge.apply(try_convert_to_float)

    data.standard_charge = data.standard_charge.astype(float)

    # replace payer_name to min max
    data['payer_name'].mask(data['payer_name'] == 'MAX OP' , "Max_Negotiated_Rate", inplace=True)
    data['payer_name'].mask(data['payer_name'] == 'MIN OP', "Min_Negotiated_Rate", inplace=True)
    data['payer_name'].mask(data['payer_name'] == 'PRICE', "Gross_Charge", inplace=True)
    data['payer_name'].mask(data['payer_name'] == 'SELF PAY', "Cash_Charge", inplace=True)

    # # Create rate category
    data["rate_category"] = op_data.apply(helper.determine_rate_category, axis=1)

    # TODO How to use LLM to dynamically map the columns?
    data.rename(columns={
        'CHG CD': 'code',
        'CHG CD DESC': 'description',
        'MOD': 'modifiers',
        'REV CD': 'rev_code',
        'payer_name': 'payer_name',
        'standard_charge': 'standard_charge',
        'additional_generic_notes': 'additional_generic_notes'
    }, inplace=True)

    data["hospital_id"] = hospital_id

    return data

def transformation_ip_phy (data, core_column):

    data = data.melt(id_vars=core_column, var_name="payer_name", value_name="standard_charge")

    # standard charge column cleaning 
    # op_data.standard_charge = op_data.standard_charge.str.replace(',', '')

    data["additional_generic_notes"] = data.apply(lambda row: f"standard_charge: {row['standard_charge']}" if contains_alphabet(row["standard_charge"]) else None, axis =1)
    data.standard_charge = data.standard_charge.apply(try_convert_to_float)

    data.standard_charge = data.standard_charge.astype(float)

    # replace payer_name to min max
    data['payer_name'].mask(data['payer_name'] == 'MAX IP' , "Max_Negotiated_Rate", inplace=True)
    data['payer_name'].mask(data['payer_name'] == 'MIN IP', "Min_Negotiated_Rate", inplace=True)
    data['payer_name'].mask(data['payer_name'] == 'PRICE', "Gross_Charge", inplace=True)
    data['payer_name'].mask(data['payer_name'] == 'SELF PAY', "Cash_Charge", inplace=True)

    # # Create rate category
    data["rate_category"] = op_data.apply(helper.determine_rate_category, axis=1)

    # TODO How to use LLM to dynamically map the columns?
    data.rename(columns={
        'CHG CD': 'code',
        'CHG CD DESC': 'description',
        'MOD': 'modifiers',
        'REV CD': 'rev_code',
        'payer_name': 'payer_name',
        'standard_charge': 'standard_charge',
        'additional_generic_notes': 'additional_generic_notes'
    }, inplace=True)

    data["hospital_id"] = hospital_id

    return data
    
op_data = transformation_op(op_data, core_col_op)
ip_data = transformation_op(ip_data, core_col_ip)
phy_data = transformation_op(phy_data, core_col_phy)

#write_dataframe_to_sqlite(op_data, 'rate', 'data/hospital_pricing.db')
#write_dataframe_to_sqlite(ip_data, 'rate', 'data/hospital_pricing.db')
#write_dataframe_to_sqlite(phy_data, 'rate', 'data/hospital_pricing.db')