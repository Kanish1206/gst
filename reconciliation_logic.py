import pandas as pd
import numpy as np
import re
from rapidfuzz import process, fuzz


# -------------------------------------------------
# 1. INVOICE NORMALIZATION (CRITICAL)
# -------------------------------------------------
def normalize_invoice_no(x):
    if pd.isna(x):
        return ""

    x = str(x).upper().strip()
    x = re.sub(r'[^A-Z0-9]', '', x)

    prefix = ''.join(re.findall(r'[A-Z]+', x))
    numbers = ''.join(re.findall(r'\d+', x)).lstrip('0')

    return f"{prefix}{numbers}"


# -------------------------------------------------
# 2. MAIN RECONCILIATION FUNCTION
# -------------------------------------------------
def process_reco(gst, pur, fuzzy_threshold=90):

    # ---------------------------
    # DATA STANDARDIZATION
    # ---------------------------
    gst["Document Number"] = gst["Document Number"].astype(str)
    pur["Reference Document No."] = pur["Reference Document No."].astype(str)

    # ---------------------------
    # AGGREGATION
    # ---------------------------
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

    # ---------------------------
    # NORMALIZATION
    # ---------------------------
    gst_agg["Doc_Normalized"] = gst_agg["Document Number"].apply(normalize_invoice_no)
    pur_agg["Doc_Normalized"] = pur_agg["Document Number"].apply(normalize_invoice_no)

    # ---------------------------
    # EXACT MATCH (NORMALIZED)
    # ---------------------------
    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "Doc_Normalized"],
        how="outer",
        suffixes=("_2B", "_PUR"),
        indicator=True
    )

    # ---------------------------
    # DIAGNOSTIC LAYER
    # ---------------------------
    merged["Match_Status"] = merged["_merge"].map({
        "both": "Exact Match",
        "left_only": "Open in 2B",
        "right_only": "Open in Books"
    })
    merged["Match_Status"] = merged["Match_Status"].cat.add_categories(
    ["Fuzzy Match"]
    )

    merged["Matched_Doc_no._other_Side"] = None
    merged["Fuzzy Score"] = np.nan

    # Audit Columns
    merged["Match_Type"] = None
    merged["Match_Logic"] = None
    merged["Threshold_Used"] = None
    merged["Reviewer_Action"] = "Pending"
    #merged["System_Remark"] = None

    merged.loc[merged["Match_Status"] == "Exact Match", [
        "Match_Type", "Match_Logic"
    ]] = ["System", "Exact_Normalized"]

    # ---------------------------
    # FUZZY MATCH PREPARATION
    # ---------------------------
    left_only_df = merged[merged["_merge"] == "left_only"].copy()
    right_only_df = merged[merged["_merge"] == "right_only"].copy()

    common_gstins = (
        set(left_only_df["Supplier GSTIN"])
        & set(right_only_df["Supplier GSTIN"])
    )

    used_pur_indices = set()
    rows_to_drop = []

    # ---------------------------
    # FUZZY MATCHING (ONE-TO-ONE)
    # ---------------------------
    for gstin in common_gstins:
        left_sub = left_only_df[left_only_df["Supplier GSTIN"] == gstin]
        right_sub = right_only_df[right_only_df["Supplier GSTIN"] == gstin]

        right_choices = right_sub["Doc_Normalized"].tolist()
        right_index_map = dict(zip(right_choices, right_sub.index))

        for left_idx, left_row in left_sub.iterrows():
            query = left_row["Doc_Normalized"]

            if len(query) < 3:
                continue

            match = process.extractOne(
                query,
                right_choices,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=fuzzy_threshold
            )

            if not match:
                continue

            matched_value, score, _ = match
            right_idx = right_index_map.get(matched_value)

            if right_idx in used_pur_indices:
                continue

            # ---- APPLY MATCH ----
            merged.loc[left_idx, "Match_Status"] = "Fuzzy Match"
            merged.loc[left_idx, "Matched_Doc_no._other_Side"] = matched_value
            merged.loc[left_idx, "Fuzzy Score"] = score

            merged.loc[left_idx, [
                "Match_Type",
                "Match_Logic",
                "Threshold_Used",
                "System_Remark"
            ]] = [
                "System",
                "Fuzzy_Normalized",
                fuzzy_threshold,
                "Invoice number format mismatch"
            ]

            # Copy Purchase values
            pur_cols = [c for c in merged.columns if c.endswith("_PUR")]
            merged.loc[left_idx, pur_cols] = merged.loc[right_idx, pur_cols]

            used_pur_indices.add(right_idx)
            rows_to_drop.append(right_idx)

    # ---------------------------
    # CLEANUP
    # ---------------------------
    merged.drop(index=rows_to_drop, inplace=True, errors="ignore")

    # ---------------------------
    # TAX DIFFERENCES
    # ---------------------------
    merged["diff IGST"] = (
        merged["IGST Amount_PUR"].fillna(0)
        - merged["IGST Amount_2B"].fillna(0)
    )

    merged["diff CGST"] = (
        merged["CGST Amount_PUR"].fillna(0)
        - merged["CGST Amount_2B"].fillna(0)
    )

    merged["diff SGST"] = (
        merged["SGST Amount_PUR"].fillna(0)
        - merged["SGST Amount_2B"].fillna(0)
    )

    # Final cleanup
    merged.drop(columns=["_merge"], inplace=True)

    return merged


