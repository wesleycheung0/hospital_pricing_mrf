from fuzzywuzzy import process


def get_best_match(item, choices, score_cutoff=80):
    # Returns the best match for an item from a list of choices.
    best_match = process.extractOne(item, choices, score_cutoff=score_cutoff)
    return best_match[0] if best_match else None

def determine_rate_category(row):
    if row["payer_name"] == "Gross_Charge":
        return "gross"
    elif row["payer_name"] == "Min_Negotiated_Rate":
        return "min"
    elif row["payer_name"] == "Max_Negotiated_Rate":
        return "max"
    elif row["payer_name"] == "Cash_Charge":
        return "max"
    else:
        return "negotiated"