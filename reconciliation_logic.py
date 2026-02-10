import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz

def normalize_doc_vectorized(series):
    """
    Vectorized normalization of document numbers (Faster than apply/loop)
    """
    return (
        series.astype(str)
        .str.upper()
        .str.replace(r'[^A-Z0-9]', '', regex=True)
        .replace('NAN', '')  # Handle string 'nan' from astype conversion if original was actual NaN
    )

def process_reco(gst, pur, threshold=90):
    """
    Optimized GST 2B vs Purchase Register Reconciliation
    """
    
    # ----------------------------
    # 1. Aggregation (Group Duplicates)
    # ----------------------------
    # Ensure string types for grouping keys
    gst["Document Number"] = gst["Document Number"].astype(str)
    pur["FI Document Number"] = pur["FI Document Number"].astype(str)

    gst_agg = (
        gst.groupby(["Supplier GSTIN", "Document Number"], as_index=False)
        .agg({
            "Supplier Name": "first",
            "Document Date": "first",
            "IGST Amount": "sum",
            "CGST Amount": "sum",
            "SGST Amount": "sum",
            "Invoice Value": "sum"
        })
    )

    pur_agg = (
        pur.groupby(
            ["GSTIN Of Vendor/Customer", "Reference Document No.", "FI Document Number"],
            as_index=False
        )
        .agg({
            "Vendor/Customer Name": "first",
            "IGST Amount": "sum",
            "CGST Amount": "sum",
            "SGST Amount": "sum",
            "Invoice Value": "sum"
        })
        .rename(columns={
            "GSTIN Of Vendor/Customer": "Supplier GSTIN",
            "Reference Document No.": "Document Number"
        })
    )

    # ----------------------------
    # 2. Exact Match Merge
    # ----------------------------
    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "Document Number"],
        how="outer",
        suffixes=("_2B", "_PUR"),
        indicator=True
    )

    # Map initial statuses
    status_map = {
        "both": "Exact Match",
        "left_only": "Open in 2B",
        "right_only": "Open in Books"
    }
    merged["Match_Status"] = merged["_merge"].map(status_map).astype("object")
    merged["Matched_Doc_no._other_Side"] = None
    merged["Fuzzy Score"] = 0.0

    # ----------------------------
    # 3. Fuzzy Matching Setup
    # ----------------------------
    # Filter only rows that need matching
    mask_left = merged["_merge"] == "left_only"
    mask_right = merged["_merge"] == "right_only"

    # Vectorized Normalization (Calculate only for relevant rows)
    # We store this in a temporary column to avoid SettingWithCopy warnings on slices
    merged["doc_norm"] = ""
    merged.loc[mask_left | mask_right, "doc_norm"] = normalize_doc_vectorized(
        merged.loc[mask_left | mask_right, "Document Number"]
    )

    # Create subsets for processing
    left_df = merged[mask_left].copy()
    right_df = merged[mask_right].copy()

    # Pre-group the RIGHT side (Books) by GSTIN for fast lookups
    # {GSTIN: {index: doc_norm, index: doc_norm...}}
    right_groups = {}
    for gstin, group in right_df.groupby("Supplier GSTIN"):
        # dropna() ensures we don't match against empty strings
        valid_docs = group["doc_norm"]
        valid_docs = valid_docs[valid_docs != ""]
        if not valid_docs.empty:
            right_groups[gstin] = valid_docs.to_dict()

    matches = [] # List to store tuples: (left_idx, right_idx, score, matched_str)

    # ----------------------------
    # 4. Fuzzy Matching Loop
    # ----------------------------
    # Iterate over GSTINs present in the LEFT side (2B)
    for gstin, group in left_df.groupby("Supplier GSTIN"):
        
        # If this GSTIN has no candidates in Books, skip
        if gstin not in right_groups:
            continue
            
        candidates = right_groups[gstin] # Dict: {idx: normalized_str}
        
        # Iterate over 2B documents for this GSTIN
        for left_idx, query in group["doc_norm"].items():
            if not query:
                continue
                
            # extractOne with a dict returns: (match_str, score, key)
            # processor=None is crucial because we already normalized the data
            result = process.extractOne(
                query, 
                candidates, 
                scorer=fuzz.ratio, 
                score_cutoff=threshold,
                processor=None 
            )

            if result:
                match_str, score, right_idx = result
                
                # Record the match
                matches.append((left_idx, right_idx, score, match_str))
                
                # GREEDY MATCHING: Remove this candidate so it can't be matched again
                del candidates[right_idx]
                
                # If no candidates left for this GSTIN, break inner loop
                if not candidates:
                    break

    # ----------------------------
    # 5. Bulk Update & Cleanup
    # ----------------------------
    if matches:
        # Unzip the match data
        left_idxs, right_idxs, scores, match_strs = zip(*matches)
        
        left_idxs = list(left_idxs)
        right_idxs = list(right_idxs)

        # Update metadata
        merged.loc[left_idxs, "Match_Status"] = "Fuzzy Match"
        merged.loc[left_idxs, "Fuzzy Score"] = scores
        merged.loc[left_idxs, "Matched_Doc_no._other_Side"] = match_strs

        # Move Purchase Data from Right Rows to Left Rows
        pur_cols = [c for c in merged.columns if c.endswith("_PUR")]
        # Values alignment is safe because lists are ordered
        merged.loc[left_idxs, pur_cols] = merged.loc[right_idxs, pur_cols].values

        # Drop the now-matched "Open in Books" rows
        merged.drop(index=right_idxs, inplace=True)

    # ----------------------------
    # 6. Final Calculations
    # ----------------------------
    merged["diff IGST"] = merged["IGST Amount_PUR"].fillna(0) - merged["IGST Amount_2B"].fillna(0)
    merged["diff CGST"] = merged["CGST Amount_PUR"].fillna(0) - merged["CGST Amount_2B"].fillna(0)
    merged["diff SGST"] = merged["SGST Amount_PUR"].fillna(0) - merged["SGST Amount_2B"].fillna(0)

    # Cleanup temporary column
    merged.drop(columns=["doc_norm", "_merge"], inplace=True)

    return merged
