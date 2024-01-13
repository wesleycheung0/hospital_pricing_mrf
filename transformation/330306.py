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

data_path = "data/hospitcal_pricing/NYU Langone Hospital—Brooklyn standard charges.csv"

data = pd.read_csv(data_path, skiprows=[0,1])
hospital_id = 330306

# melt
core_col = ['Identifier Code', 'Billing Code', 'Identifier Description']
melt_col = list(set(data.columns) - set(core_col))

data = data.melt(id_vars=core_col, value_vars=melt_col, var_name="payer_name", value_name="standard_charge")

# standard charge column cleaning 
data.standard_charge = data.standard_charge.str.replace(',', '')

data["additional_generic_notes"] = data.apply(lambda row: f"standard_charge: {row['standard_charge']}" if contains_alphabet(row["standard_charge"]) else None, axis =1)
data.standard_charge = data.standard_charge.apply(try_convert_to_float)

data.standard_charge = data.standard_charge.astype(float)

# replace payer_name to min max
data['payer_name'].mask(data['payer_name'] == 'De-identified Maximum', "Max_Negotiated_Rate", inplace=True)
data['payer_name'].mask(data['payer_name'] == 'De-identified Minimum', "Min_Negotiated_Rate", inplace=True)
data['payer_name'].mask(data['payer_name'] == 'NYU Langone Gross Charges', "Gross_Charge", inplace=True)
data['payer_name'].mask(data['payer_name'] == 'NYU Langone Discounted Cash Price', "Cash_Charge", inplace=True)

# Create rate category
data["rate_category"] = data.apply(helper.determine_rate_category, axis=1)

# Rename columns
data = data.rename(columns={"Billing Code":"code", "Identifier Code": "local_code", "Identifier Description": "description"})

data["hospital_id"] = hospital_id
# write_dataframe_to_sqlite(data, 'rate', 'data/hospital_pricing.db')