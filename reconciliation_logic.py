import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz


# -------------------------------------------------
# 1. Normalization Utility
# -------------------------------------------------
def normalize_doc(series):
    return (
        series.astype(str)
        .str.upper()
        .str.replace(r'[^A-Z0-9]', '', regex=True)
        .replace('NAN', '')
    )


# -------------------------------------------------
# 2. Main Reconciliation Function
# -------------------------------------------------
def process_reco(
    gst,
    pur,
    doc_threshold=90,
    amount_tolerance=5
):
    """
    Production Grade GST 2B Reconciliation Tool
    --------------------------------------------------
    doc_threshold     : Fuzzy matching threshold (0-100)
    amount_tolerance  : Allowed difference in invoice value
    """

    gst = gst.copy()
    pur = pur.copy()

    # -------------------------------------------------
    # STEP 1: Aggregation
    # -------------------------------------------------
    gst["Document Number"] = gst["Document Number"].astype(str)
    pur["Reference Document No."] = pur["Reference Document No."].astype(str)

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

    # -------------------------------------------------
    # STEP 2: Exact Merge
    # -------------------------------------------------
    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "Document Number"],
        how="outer",
        suffixes=("_2B", "_PUR"),
        indicator=True
    )

    merged["Match_Status"] = None
    merged["Fuzzy Score"] = 0.0
    merged["Matched_Doc_no._other_Side"] = None

    # -------------------------------------------------
    # STEP 3: Classify Exact Matches
    # -------------------------------------------------
    both_mask = merged["_merge"] == "both"

    merged.loc[both_mask, "diff Invoice Value"] = (
        merged["Invoice Value_PUR"].fillna(0)
        - merged["Invoice Value_2B"].fillna(0)
    )

    exact_amount_match = (
        both_mask &
        (merged["diff Invoice Value"].abs() <= amount_tolerance)
    )

    merged.loc[exact_amount_match, "Match_Status"] = "Exact Match"

    merged.loc[
        both_mask & ~exact_amount_match,
        "Match_Status"
    ] = "Exact Doc - Amount Mismatch"

    merged.loc[
        merged["_merge"] == "left_only",
        "Match_Status"
    ] = "Open in 2B"

    merged.loc[
        merged["_merge"] == "right_only",
        "Match_Status"
    ] = "Open in Books"

    # -------------------------------------------------
    # STEP 4: Prepare for Fuzzy Matching
    # -------------------------------------------------
    left_df = merged[merged["Match_Status"] == "Open in 2B"].copy()
    right_df = merged[merged["Match_Status"] == "Open in Books"].copy()

    merged["doc_norm"] = normalize_doc(merged["Document Number"])

    # Group right side by GSTIN
    right_groups = {}
    for gstin, grp in right_df.groupby("Supplier GSTIN"):
        right_groups[gstin] = grp.index.tolist()

    # -------------------------------------------------
    # STEP 5: Fuzzy Matching with Amount Validation
    # -------------------------------------------------
    for gstin, left_group in left_df.groupby("Supplier GSTIN"):

        if gstin not in right_groups:
            continue

        available_right_indices = right_groups[gstin]

        for left_idx in left_group.index:

            if not available_right_indices:
                break

            left_doc = merged.at[left_idx, "doc_norm"]
            left_amount = merged.at[left_idx, "Invoice Value_2B"]

            # Build candidate dictionary
            candidates = {
                idx: merged.at[idx, "doc_norm"]
                for idx in available_right_indices
            }

            match = process.extractOne(
                left_doc,
                candidates,
                scorer=fuzz.ratio,
                score_cutoff=doc_threshold,
                processor=None
            )

            if match:
                matched_str, score, right_idx = match

                right_amount = merged.at[right_idx, "Invoice Value_PUR"]

                # Amount validation
                if abs((right_amount or 0) - (left_amount or 0)) <= amount_tolerance:

                    # Update Left Row
                    merged.at[left_idx, "Match_Status"] = "Fuzzy Match"
                    merged.at[left_idx, "Fuzzy Score"] = score
                    merged.at[left_idx, "Matched_Doc_no._other_Side"] = matched_str

                    # Copy purchase side values
                    pur_cols = [c for c in merged.columns if c.endswith("_PUR")]
                    for col in pur_cols:
                        merged.at[left_idx, col] = merged.at[right_idx, col]

                    # Mark right row as consumed
                    merged.at[right_idx, "Match_Status"] = "Fuzzy Consumed"

                    available_right_indices.remove(right_idx)

    # Remove consumed rows
    merged = merged[merged["Match_Status"] != "Fuzzy Consumed"]

    # -------------------------------------------------
    # STEP 6: Final Difference Calculations
    # -------------------------------------------------
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

    merged["diff Invoice Value"] = (
        merged["Invoice Value_PUR"].fillna(0)
        - merged["Invoice Value_2B"].fillna(0)
    )

    # Cleanup
    merged.drop(columns=["_merge", "doc_norm"], inplace=True)

    return merged
