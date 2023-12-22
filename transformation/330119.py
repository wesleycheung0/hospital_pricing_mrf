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
from helper_function.metadata import setting_category

# Function to try conversion to string, or return -1
def try_convert_to_float(value):
    try:
        # Attempt to convert to string
        return float(value)
    except:
        # If conversion fails, return -1
        return -1
    
# Function to check if a string contains any alphabet character
def contains_alphabet(s):
    s = str(s)
    return any(char.isalpha() for char in s)

# Load the JSON data
file_path = 'data/hospitcal_pricing/13-1624070_Lenox Hill Hospital_StandardCharges.json'

with open(file_path, 'r') as file:
    json_data = json.load(file)

hospital_id = 330119

data = pd.DataFrame.from_dict(json_data["Data"])
data.columns = json_data["Headers"]

data = data.drop("Site", axis=1)

# melt
core_col = ['Identifier_Code', 'Billing_Code', 'Identifier_Description']
melt_col = list(set(data.columns) - set(core_col))

data = data.melt(id_vars=core_col, value_vars=melt_col, var_name="payer_name", value_name="standard_charge")

# standard charge column cleaning 
data.standard_charge = data.standard_charge.replace('N/A', np.nan)
data.standard_charge = data.standard_charge.str.replace(',', '')

data["additional_generic_notes"] = data.apply(lambda row: f"standard_charge: {row['standard_charge']}" if contains_alphabet(row["standard_charge"]) else None, axis =1)
data.standard_charge = data.standard_charge.apply(try_convert_to_float)

data.standard_charge = data.standard_charge.astype(float)

# replace payer_name to min max
data['payer_name'].mask(data['payer_name'] == 'De-identified Maximum', "Max_Negotiated_Rate", inplace=True)
data['payer_name'].mask(data['payer_name'] == 'De-identified Minimum', "Min_Negotiated_Rate", inplace=True)
data['payer_name'].mask(data['payer_name'] == 'Charges', "Gross_Charge", inplace=True)
data['payer_name'].mask(data['payer_name'] == 'Discounted Cash Price', "Cash_Charge", inplace=True)

# Create rate category
data["rate_category"] = data.apply(helper.determine_rate_category, axis=1)

# Rename columns
data = data.rename(columns={"Billing_Code":"code", "Identifier_Code": "local_code", "Identifier_Description": "description"})

# Add hospital id
data["hospital_id"] = hospital_id

write_dataframe_to_sqlite(data, 'rate', 'data/hospital_pricing.db')