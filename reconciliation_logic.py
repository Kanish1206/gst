import pandas as pd
import numpy as np
import re
from rapidfuzz import process, fuzz

def process_reco(gst, pur,threshold):
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
               "Return Period" : "first",
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
               "Return Period": "first",
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
    left_only_df = merged_diagnose[merged_diagnose['_merge'] == 'left_only'].copy()
    right_only_df = merged_diagnose[merged_diagnose['_merge'] == 'right_only'].copy()

    left_only_df['Document Number_str'] = left_only_df['Document Number'].astype(str).replace('nan', '')
    right_only_df['Document Number_str'] = right_only_df['Document Number'].astype(str).replace('nan', '')
    
    common_gstins = set(left_only_df['Supplier GSTIN'].unique()) & set(right_only_df['Supplier GSTIN'].unique())
    
    #threshold = 90
    rows_to_drop = []

    # 4. --- Fuzzy Matching Loop ---
    for gstin_val in common_gstins:
        left_subset = left_only_df[left_only_df['Supplier GSTIN'] == gstin_val]
        right_subset = right_only_df[right_only_df['Supplier GSTIN'] == gstin_val]

        if not left_subset.empty and not right_subset.empty:
            right_choices_series = right_subset['Document Number_str']
            
            for original_idx_2B, row_2B in left_subset.iterrows():
                query_val = row_2B['Document Number_str']
                match_result = process.extractOne(query_val, right_choices_series, scorer=fuzz.ratio, score_cutoff=threshold)

                if match_result:
                    matched_pur_string, score, original_idx_PUR = match_result
                    
                    # Update status to Fuzzy Match
                    merged_diagnose.loc[original_idx_2B, 'Match_Status'] = 'Fuzzy Match'
                    
                    # POPULATE YOUR REQUESTED COLUMNS
                    merged_diagnose.loc[original_idx_2B, 'Matched_Doc_no._other_Side'] = matched_pur_string
                    merged_diagnose.loc[original_idx_2B, 'Fuzzy Score'] = score

                    # BRIDGING DATA: Copy Book tax values to the 2B row for calculation
                    pur_cols = [col for col in merged_diagnose.columns if col.endswith('_PUR')]
                    for col in pur_cols:
                        merged_diagnose.loc[original_idx_2B, col] = merged_diagnose.loc[original_idx_PUR, col]

                    # Mark the redundant Book-only row for deletion
                    rows_to_drop.append(original_idx_PUR)

    # Clean up the dataframe by removing the extra rows we just merged
    merged_diagnose.drop(index=rows_to_drop, inplace=True, errors='ignore')

    # 5. --- Final Calculations ---
    merged_diagnose["diff IGST"] = merged_diagnose["IGST Amount_PUR"].fillna(0) - merged_diagnose["IGST Amount_2B"].fillna(0)
    merged_diagnose["diff CGST"] = merged_diagnose["CGST Amount_PUR"].fillna(0) - merged_diagnose["CGST Amount_2B"].fillna(0)
    merged_diagnose["diff SGST"] = merged_diagnose["SGST Amount_PUR"].fillna(0) - merged_diagnose["SGST Amount_2B"].fillna(0)

    # Drop the internal pandas _merge column before returning
    return merged_diagnose





