import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz

def process_reco(gst, pur, threshold):

    # ------------------ Data Cleaning ------------------
    pur["FI Document Number"] = pur["FI Document Number"].astype(str)

    # ------------------ Aggregation ------------------
    gst_agg = (
        gst.groupby(["Supplier GSTIN", "Document Number", "Return Period"], as_index=False)
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
            "Return Period": "first",
            "Vendor/Customer Name": "first",
            "IGST Amount": "sum",
            "CGST Amount": "sum",
            "SGST Amount": "sum",
            "Invoice Value": "sum"
        })
    )

    pur_agg = pur_agg.rename(columns={
        "GSTIN Of Vendor/Customer": "Supplier GSTIN",
        "Reference Document No.": "Document Number"
    })

    # ------------------ Initial Merge ------------------
    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "Document Number"],
        how="outer",
        suffixes=("_2B", "_PUR"),
        indicator=True
    )

    merged_diagnose = merged.copy()
    merged_diagnose["Match_Status"] = merged_diagnose["_merge"].map({
        "both": "Exact Match",
        "left_only": "Open in 2B",
        "right_only": "Open in Books"
    }).astype("category").cat.add_categories("Fuzzy Match")

    merged_diagnose["Matched_Doc_no._other_Side"] = None
    merged_diagnose["Fuzzy Score"] = 0.0

    # ------------------ Prepare Fuzzy Matching ------------------
    left_only_df = merged_diagnose[merged_diagnose["_merge"] == "left_only"].copy()
    right_only_df = merged_diagnose[merged_diagnose["_merge"] == "right_only"].copy()

    left_only_df["Document Number_norm"] = (
        left_only_df["Document Number"].astype(str).str.upper().str.replace(r"\W+", "", regex=True)
    )
    right_only_df["Document Number_norm"] = (
        right_only_df["Document Number"].astype(str).str.upper().str.replace(r"\W+", "", regex=True)
    )

    common_gstins = (
        set(left_only_df["Supplier GSTIN"]) &
        set(right_only_df["Supplier GSTIN"])
    )

    rows_to_drop = []

    # ------------------ Fuzzy Matching Loop ------------------
    for gstin_val in common_gstins:

        left_subset = left_only_df[left_only_df["Supplier GSTIN"] == gstin_val]
        right_subset = right_only_df[right_only_df["Supplier GSTIN"] == gstin_val]

        right_choices = right_subset["Document Number_norm"].tolist()

        for idx_2B, row_2B in left_subset.iterrows():

            query = row_2B["Document Number_norm"]
            if not query:
                continue

            match = process.extractOne(
                query,
                right_choices,
                scorer=fuzz.token_set_ratio,
                score_cutoff=threshold
            )

            if match:
                matched_val, score, pos = match
                idx_PUR = right_subset.index[pos]

                merged_diagnose.loc[idx_2B, "Match_Status"] = "Fuzzy Match"
                merged_diagnose.loc[idx_2B, "Matched_Doc_no._other_Side"] = \
                    merged_diagnose.loc[idx_PUR, "Document Number"]
                merged_diagnose.loc[idx_2B, "Fuzzy Score"] = score

                pur_cols = [c for c in merged_diagnose.columns if c.endswith("_PUR")]
                for c in pur_cols:
                    merged_diagnose.loc[idx_2B, c] = merged_diagnose.loc[idx_PUR, c]

                rows_to_drop.append(idx_PUR)

    merged_diagnose = merged_diagnose.drop(index=rows_to_drop)

    # ------------------ Difference Calculation ------------------
    merged_diagnose["diff IGST"] = (
        merged_diagnose["IGST Amount_PUR"].fillna(0) -
        merged_diagnose["IGST Amount_2B"].fillna(0)
    )
    merged_diagnose["diff CGST"] = (
        merged_diagnose["CGST Amount_PUR"].fillna(0) -
        merged_diagnose["CGST Amount_2B"].fillna(0)
    )
    merged_diagnose["diff SGST"] = (
        merged_diagnose["SGST Amount_PUR"].fillna(0) -
        merged_diagnose["SGST Amount_2B"].fillna(0)
    )

    return merged_diagnose.drop(columns="_merge")
