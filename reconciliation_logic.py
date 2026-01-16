import pandas as pd
import numpy as np
import re
from rapidfuzz import process, fuzz

def process_reco(gst, pur):
    """
    Accepts two DataFrames (gst and pur) and returns the reconciled DataFrame.
    """
    # --- Data Cleaning ---
    #def clean_doc_no(x):
    #if pd.isna(x):
      #return None
    #return (
        #str(x)
        #.strip()
   # )
    #gst["Return Period"] = gst["Return Period"].astype(str)
    pur["FI Document Number"] = pur["FI Document Number"].astype(str)
    
    # --- Aggregation ---
    gst_agg = (
        gst.groupby(["Supplier GSTIN", "Document Number"], as_index=False)
           .agg({
               "Supplier Name" : "first",
               #"Return Period" : "first",
               "Document Date": "first",
               "IGST Amount" : "sum",
               "CGST Amount" : "sum",
               "SGST Amount"  : "sum",
               "Invoice Value" : "sum"
           })
    )

    pur_agg = (
        pur.groupby(["GSTIN Of Vendor/Customer","Reference Document No.","FI Document Number"], as_index=False)
           .agg({
               #"Return Period": "first",
               "Vendor/Customer Name" : "first",
               "Reference Document No.": "first",
               "FI Document Number": "first",
               "IGST Amount": "sum",
               "CGST Amount": "sum",
               "SGST Amount": "sum",
               "Invoice Value": "sum"
           })
    )

    # Align Column Names
    pur_agg = pur_agg.rename(columns={"GSTIN Of Vendor/Customer": "Supplier GSTIN", 
                                      "Reference Document No.": "Document Number"})

    # --- Initial Merge ---
    #def process_reco(gst_agg, pur_agg):
    # 1. --- Initial Exact Match ---
    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "Document Number"],
        how="outer",
        suffixes=("_2B", "_PUR"),
        indicator=True
    )

    # 2. --- Initialize Table with your requested columns ---
    merged_diagnose = merged.copy()
    merged_diagnose["Match_Status"] = merged_diagnose["_merge"].map({
        "both": "Exact Match",
        "left_only": "Open in 2B",
        "right_only": "Open in Books"
    }).astype('category').cat.add_categories('Fuzzy Match')

    # ADDING YOUR REQUESTED COLUMNS
    merged_diagnose["Matched_Doc_no._other_Side"] = None
    merged_diagnose["Fuzzy Score"] = 0.0

    # 3. --- Prepare Fuzzy Matching Logic ---
    from rapidfuzz import process, fuzz

threshold = 90
used_pur_indexes = set()

for gstin in common_gstins:
    left_subset = left_only_df[left_only_df["Supplier GSTIN"] == gstin]
    right_subset = right_only_df[right_only_df["Supplier GSTIN"] == gstin]

    if left_subset.empty or right_subset.empty:
        continue

    right_map = (
        right_subset["Document Number_str"]
        .dropna()
        .to_dict()
    )

    for left_idx, left_row in left_subset.iterrows():
        query = left_row["Document Number_str"]

        if not query:
            continue

        match = process.extractOne(
            query,
            right_map,
            scorer=fuzz.ratio,
            score_cutoff=threshold
        )

        if not match:
            continue

        matched_string, score, pur_idx = match

        # Prevent one-to-many matches
        if pur_idx in used_pur_indexes:
            continue

        used_pur_indexes.add(pur_idx)

        # Update match info
        merged_diagnose.loc[left_idx, "Match_Status"] = "Fuzzy Match"
        merged_diagnose.loc[left_idx, "Matched_Doc_no._other_Side"] = matched_string
        merged_diagnose.loc[left_idx, "Fuzzy Score"] = score

        # Copy PUR values
        pur_cols = [c for c in merged_diagnose.columns if c.endswith("_PUR")]
        merged_diagnose.loc[left_idx, pur_cols] = merged_diagnose.loc[pur_idx, pur_cols].values

        rows_to_drop.append(pur_idx)

    # Clean up the dataframe by removing the extra rows we just merged
    merged_diagnose.drop(index=rows_to_drop, inplace=True, errors='ignore')

    # 5. --- Final Calculations ---
    merged_diagnose["diff IGST"] = merged_diagnose["IGST Amount_PUR"].fillna(0) - merged_diagnose["IGST Amount_2B"].fillna(0)
    merged_diagnose["diff CGST"] = merged_diagnose["CGST Amount_PUR"].fillna(0) - merged_diagnose["CGST Amount_2B"].fillna(0)
    merged_diagnose["diff SGST"] = merged_diagnose["SGST Amount_PUR"].fillna(0) - merged_diagnose["SGST Amount_2B"].fillna(0)

    # Drop the internal pandas _merge column before returning
    return merged_diagnose

