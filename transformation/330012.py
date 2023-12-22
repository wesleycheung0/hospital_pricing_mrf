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

# Load the JSON data
file_path = 'data/hospitcal_pricing/133957095_NewYorkPresbyterianHospital_standardcharges.json'

with open(file_path, 'r') as file:
    json_data = json.load(file)

hospital_id = 330012

data = pd.DataFrame.from_dict(json_data)

# melt
core_col = ['Code (CPT/DRG)', 'Description', 'Rev Code', 'Inpatient/Outpatient']
melt_col = list(set(data.columns) - set(core_col))

data = data.melt(id_vars=core_col, value_vars=melt_col, var_name="payer_name", value_name="standard_charge")

data["additional_generic_notes"] = data.apply(lambda row: f"standard_charge: {row['standard_charge']}" if isinstance(row["standard_charge"], str) else None, axis =1)
data["standard_charge"] = data.apply(lambda row: -1 if isinstance(row["standard_charge"], str) else row["standard_charge"], axis =1)

# replace payer_name to min max
data['payer_name'].mask(data['payer_name'] == 'Maximum Negotiated Charge', "Max_Negotiated_Rate", inplace=True)
data['payer_name'].mask(data['payer_name'] == 'Minimum Negotiated Charge', "Min_Negotiated_Rate", inplace=True)
data['payer_name'].mask(data['payer_name'] == 'Gross Charges', "Gross_Charge", inplace=True)
data['payer_name'].mask(data['payer_name'] == 'Discounted Cash Price', "Cash_Charge", inplace=True)

# Create rate category
data["rate_category"] = data.apply(helper.determine_rate_category, axis=1)

# Rename columns
data = data.rename(columns={"Code (CPT/DRG)":"code", "Description": "description", "Rev Code": "rev_code",
                     "Inpatient/Outpatient": "setting"})
# Replace Inpatient/Outpatient to both
data['setting'].mask(data['setting'] == 'Inpatient/Outpatient', "both", inplace=True)

# billing class fuzzy
data['setting'] = data['setting'].apply(lambda x: helper.get_best_match(x, setting_category))

# Add hospital id
data["hospital_id"] = hospital_id

write_dataframe_to_sqlite(data, 'rate', 'data/hospital_pricing.db')