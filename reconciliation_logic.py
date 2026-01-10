import pandas as pd
import numpy as np
import re
from rapidfuzz import process, fuzz
from lshkrepresentatives import LSHkRepresentatives

def apply_lsh_clustering(open_df, n_clusters_ratio=0.1):
    """
    Groups open items into clusters using LSH-k-representatives 
    based on Supplier GSTIN and Document Number.
    """
    if open_df.empty:
        return open_df

    # Prepare features for clustering (Categorical)
    # We combine GSTIN and Doc Number to ensure the cluster respects both
    features = open_df[['Supplier GSTIN', 'Document Number']].astype(str).values
    
    # Dynamically determine number of clusters (min 2)
    n_clusters = max(2, int(len(open_df) * n_clusters_ratio))
    
    # Initialize and Fit LSH-k-representatives
    # n_init is number of times the algo will run with different centroid seeds
    model = LSHkRepresentatives(n_clusters=n_clusters, n_init=3, verbose=0)
    clusters = model.fit_predict(features)
    
    open_df['Cluster_ID'] = clusters
    return open_df

def process_reco(gst, pur):
    """
    Reconciles GST 2B and Purchase Register using Exact Match 
    followed by LSH-accelerated Fuzzy Matching.
    """
    # --- 1. Data Cleaning & Aggregation ---
    pur["FI Document Number"] = pur["FI Document Number"].astype(str)

    gst_agg = gst.groupby(["Supplier GSTIN", "Document Number"], as_index=False).agg({
        "Supplier Name" : "first",
        "Document Date": "first",
        "IGST Amount" : "sum",
        "CGST Amount" : "sum",
        "SGST Amount"  : "sum",
        "Invoice Value" : "sum"
    })

    pur_agg = pur.groupby(["GSTIN Of Vendor/Customer","Reference Document No.","FI Document Number"], as_index=False).agg({
        "Vendor/Customer Name" : "first",
        "IGST Amount": "sum",
        "CGST Amount": "sum",
        "SGST Amount": "sum",
        "Invoice Value": "sum"
    }).rename(columns={"GSTIN Of Vendor/Customer": "Supplier GSTIN", "Reference Document No.": "Document Number"})

    # --- 2. Initial Exact Match ---
    merged_diagnose = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "Document Number"],
        how="outer",
        suffixes=("_2B", "_PUR"),
        indicator=True
    )

    merged_diagnose["Match_Status"] = merged_diagnose["_merge"].map({
        "both": "Exact Match",
        "left_only": "Open in 2B",
        "right_only": "Open in Books"
    }).astype('category').cat.add_categories('Fuzzy Match')

    merged_diagnose["Matched_Doc_no._other_Side"] = None
    merged_diagnose["Fuzzy Score"] = 0.0

    # --- 3. LSH-k-representatives Clustering for Open Items ---
    # We only care about rows that didn't match exactly
    open_items = merged_diagnose[merged_diagnose['_merge'] != 'both'].copy()
    
    if not open_items.empty:
        open_items = apply_lsh_clustering(open_items)
        
        # Split back to 2B and Books but now with Cluster IDs
        left_only_df = open_items[open_items['_merge'] == 'left_only'].copy()
        right_only_df = open_items[open_items['_merge'] == 'right_only'].copy()
        
        threshold = 75
        rows_to_drop = []

        # --- 4. Fuzzy Matching (Within Clusters) ---
        # Instead of looping through everything, we only loop through clusters
        for cluster_id in left_only_df['Cluster_ID'].unique():
            l_subset = left_only_df[left_only_df['Cluster_ID'] == cluster_id]
            r_subset = right_only_df[right_only_df['Cluster_ID'] == cluster_id]

            if not l_subset.empty and not r_subset.empty:
                r_choices = r_subset['Document Number'].astype(str)
                
                for idx_2B, row_2B in l_subset.iterrows():
                    query = str(row_2B['Document Number'])
                    # Fuzzy match only against items in the same LSH cluster
                    match_result = process.extractOne(query, r_choices, scorer=fuzz.ratio, score_cutoff=threshold)

                    if match_result:
                        matched_str, score, idx_PUR = match_result
                        
                        # Update status
                        merged_diagnose.at[idx_2B, 'Match_Status'] = 'Fuzzy Match'
                        merged_diagnose.at[idx_2B, 'Matched_Doc_no._other_Side'] = matched_str
                        merged_diagnose.at[idx_2B, 'Fuzzy Score'] = score

                        # Bridge Data from PUR to 2B row
                        pur_cols = [c for c in merged_diagnose.columns if c.endswith('_PUR')]
                        for col in pur_cols:
                            merged_diagnose.at[idx_2B, col] = merged_diagnose.at[idx_PUR, col]

                        rows_to_drop.append(idx_PUR)

        # Remove the Book rows that were merged during fuzzy matching
        merged_diagnose.drop(index=rows_to_drop, inplace=True, errors='ignore')

    # --- 5. Final Calculations ---
    tax_types = ["IGST", "CGST", "SGST"]
    for tax in tax_types:
        merged_diagnose[f"diff {tax}"] = (
            merged_diagnose[f"{tax} Amount_PUR"].fillna(0) - 
            merged_diagnose[f"{tax} Amount_2B"].fillna(0)
        )

    return merged_diagnose.drop(columns=['_merge'], errors='ignore')