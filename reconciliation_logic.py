import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz

def normalize_doc_vectorized(series):
    """
    Vectorized normalization of document numbers
    """
    return (
        series.astype(str)
        .str.upper()
        .str.replace(r'[^A-Z0-9]', '', regex=True)
        .replace('NAN', '')
    )

def process_reco(gst, pur, threshold=90):
    
    # ----------------------------
    # 1. Aggregation 
    # ----------------------------
    gst["Document Number"] = gst["Document Number"].astype(str)
    pur["FI Document Number"] = pur["FI Document Number"].astype(str)

    # GST Aggregation
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

    # Purchase Aggregation
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

    merged["Match_Status"] = merged["_merge"].map({
        "both": "Exact Match",
        "left_only": "Open in 2B",
        "right_only": "Open in Books"
    }).astype("object")
    
    merged["Matched_Doc_no._other_Side"] = None
    merged["Fuzzy Score"] = 0.0

    # ----------------------------
    # 3. Fuzzy Matching Setup
    # ----------------------------
    mask_left = merged["_merge"] == "left_only"
    mask_right = merged["_merge"] == "right_only"

    merged["doc_norm"] = ""
    merged.loc[mask_left | mask_right, "doc_norm"] = normalize_doc_vectorized(
        merged.loc[mask_left | mask_right, "Document Number"]
    )

    left_df = merged[mask_left].copy()
    right_df = merged[mask_right].copy()

    # Group Right side (Books) by GSTIN
    right_groups = {}
    for gstin, group in right_df.groupby("Supplier GSTIN"):
        valid_docs = group["doc_norm"]
        valid_docs = valid_docs[valid_docs != ""]
        if not valid_docs.empty:
            right_groups[gstin] = valid_docs.to_dict()

    matches = [] 

    # ----------------------------
    # 4. Fuzzy Matching Loop
    # ----------------------------
    for gstin, group in left_df.groupby("Supplier GSTIN"):
        if gstin not in right_groups:
            continue
            
        candidates = right_groups[gstin]
        
        for left_idx, query in group["doc_norm"].items():
            if not query:
                continue
                
            match = process.extractOne(
                query, 
                candidates, 
                scorer=fuzz.ratio, 
                score_cutoff=threshold,
                processor=None 
            )

            if match:
                matched_str, score, right_idx = match
                matches.append((left_idx, right_idx, score, matched_str))
                
                # Greedy Match: Remove used candidate
                del candidates[right_idx]
                if not candidates:
                    break

    # ----------------------------
    # 5. Bulk Update (FIXED HERE)
    # ----------------------------
    if matches:
        left_idxs, right_idxs, scores, match_strs = zip(*matches)
        
        left_idxs = list(left_idxs)
        right_idxs = list(right_idxs)

        # Update metadata
        merged.loc[left_idxs, "Match_Status"] = "Fuzzy Match"
        merged.loc[left_idxs, "Fuzzy Score"] = scores
        merged.loc[left_idxs, "Matched_Doc_no._other_Side"] = match_strs

        # --- KEY FIX: Separate Text and Numeric updates ---
        
        # 1. Update Text Columns (The ones you were missing)
        # We explicitly select these columns to ensure they copy over
        text_cols = ["Vendor/Customer Name", "FI Document Number"]
        for col in text_cols:
            if col in merged.columns:
                # Copy values directly
                merged.loc[left_idxs, col] = merged.loc[right_idxs, col].values

        # 2. Update Numeric/Other Columns (ending in _PUR)
        suffixed_cols = [c for c in merged.columns if c.endswith("_PUR")]
        if suffixed_cols:
             merged.loc[left_idxs, suffixed_cols] = merged.loc[right_idxs, suffixed_cols].values

        # Drop the now-matched "Open in Books" rows
        merged.drop(index=right_idxs, inplace=True)

    # ----------------------------
    # 6. Final Calculations
    # ----------------------------
    merged["diff IGST"] = merged["IGST Amount_PUR"].fillna(0) - merged["IGST Amount_2B"].fillna(0)
    merged["diff CGST"] = merged["CGST Amount_PUR"].fillna(0) - merged["CGST Amount_2B"].fillna(0)
    merged["diff SGST"] = merged["SGST Amount_PUR"].fillna(0) - merged["SGST Amount_2B"].fillna(0)

    # Cleanup
    merged.drop(columns=["doc_norm", "_merge"], inplace=True)

    return merged
