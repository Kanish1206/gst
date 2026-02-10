import pandas as pd
import numpy as np
import re
from rapidfuzz import process, fuzz

# ------------------ NORMALIZATION ------------------
_norm_re = re.compile(r'[^A-Z0-9]')

def normalize_series(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
         .astype(str)
         .str.upper()
         .str.replace(_norm_re, "", regex=True)
    )

# ------------------ MAIN FUNCTION ------------------
def process_reco(gst, pur, threshold):

    pur["FI Document Number"] = pur["FI Document Number"].astype(str)

    # --- Aggregation ---
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
        .rename(columns={
            "GSTIN Of Vendor/Customer": "Supplier GSTIN",
            "Reference Document No.": "Document Number"
        })
    )

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
    }).astype("category").cat.add_categories("Fuzzy Match")

    merged["Matched_Doc_no._other_Side"] = None
    merged["Fuzzy Score"] = 0.0

    left = merged[merged["_merge"] == "left_only"].copy()
    right = merged[merged["_merge"] == "right_only"].copy()

    left["Document Number_str"] = left["Document Number"].astype(str)
    right["Document Number_str"] = right["Document Number"].astype(str)

    rows_to_drop = []

    for gstin, left_grp in left.groupby("Supplier GSTIN"):
        right_grp = right[right["Supplier GSTIN"] == gstin]
        if right_grp.empty:
            continue

        choices = right_grp["Document Number_str"]

        for idx_2b, row in left_grp.iterrows():
            match = process.extractOne(
                row["Document Number_str"],
                choices,
                scorer=fuzz.ratio,
                score_cutoff=threshold
            )

            if match:
                _, score, pos = match
                idx_pur = choices.index[pos]

                merged.loc[idx_2b, "Match_Status"] = "Fuzzy Match"
                merged.loc[idx_2b, "Matched_Doc_no._other_Side"] = merged.loc[idx_pur, "Document Number"]
                merged.loc[idx_2b, "Fuzzy Score"] = score

                pur_cols = [c for c in merged.columns if c.endswith("_PUR")]
                merged.loc[idx_2b, pur_cols] = merged.loc[idx_pur, pur_cols]

                rows_to_drop.append(idx_pur)

    merged.drop(index=rows_to_drop, inplace=True, errors="ignore")

    for tax in ["IGST", "CGST", "SGST"]:
        merged[f"diff {tax}"] = (
            merged[f"{tax} Amount_PUR"].fillna(0)
            - merged[f"{tax} Amount_2B"].fillna(0)
        )

    return merged.drop(columns="_merge")

