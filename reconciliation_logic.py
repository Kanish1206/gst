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
def process_reco(gst: pd.DataFrame, pur: pd.DataFrame, threshold: int) -> pd.DataFrame:

    # ------------------ Data Cleaning ------------------
    pur = pur.copy()
    gst = gst.copy()

    pur["FI Document Number"] = pur["FI Document Number"].astype(str)

    # ------------------ Aggregation ------------------
    gst_agg = (
        gst.groupby(
            ["Supplier GSTIN", "Document Number", "Return Period"],
            as_index=False
        )
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

    # ------------------ Initial Merge ------------------
    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "Document Number"],
        how="outer",
        suffixes=("_2B", "_PUR"),
        indicator=True
    )

    merged["Match_Status"] = np.select(
        [
            merged["_merge"] == "both",
            merged["_merge"] == "left_only",
            merged["_merge"] == "right_only"
        ],
        [
            "Exact Match",
            "Open in 2B",
            "Open in Books"
        ],
        default="Unknown"
    )

    merged["Matched_Doc_no._other_Side"] = None
    merged["Fuzzy Score"] = np.nan

    # ------------------ Prepare Fuzzy Sets ------------------
    left = merged[merged["_merge"] == "left_only"].copy()
    right = merged[merged["_merge"] == "right_only"].copy()

    left["Document Number_norm"] = normalize_series(left["Document Number"])
    right["Document Number_norm"] = normalize_series(right["Document Number"])

    # Group right side by GSTIN
    right_groups = {
        gstin: grp
        for gstin, grp in right.groupby("Supplier GSTIN")
    }

    fuzzy_matches = []

    # ------------------ Fuzzy Matching (INDEX SAFE) ------------------
    for idx_2b, row in left.iterrows():

        gstin = row["Supplier GSTIN"]
        query = row["Document Number_norm"]

        if not query or len(query) < 3:
            continue

        if gstin not in right_groups:
            continue

        candidates = right_groups[gstin]

        if candidates.empty:
            continue

        # Bind normalized doc + real index
        choices = list(
            zip(
                candidates["Document Number_norm"].tolist(),
                candidates.index.tolist()
            )
        )

        match = process.extractOne(
            query,
            choices,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold
        )

        if match:
            (_, pur_idx), score, _ = match
            fuzzy_matches.append((idx_2b, pur_idx, score))

    # ------------------ Apply Fuzzy Matches ------------------
    used_pur_rows = set()

    for idx_2b, idx_pur, score in fuzzy_matches:

        if idx_pur in used_pur_rows:
            continue

        merged.loc[idx_2b, "Match_Status"] = "Fuzzy Match"
        merged.loc[idx_2b, "Matched_Doc_no._other_Side"] = merged.loc[idx_pur, "Document Number"]
        merged.loc[idx_2b, "Fuzzy Score"] = score

        pur_cols = [c for c in merged.columns if c.endswith("_PUR")]
        merged.loc[idx_2b, pur_cols] = merged.loc[idx_pur, pur_cols]

        used_pur_rows.add(idx_pur)

    # Drop matched Book-only rows
    merged.drop(index=used_pur_rows, inplace=True, errors="ignore")

    # ------------------ Tax Differences ------------------
    for tax in ["IGST", "CGST", "SGST"]:
        merged[f"diff {tax}"] = (
            merged[f"{tax} Amount_PUR"].fillna(0)
            - merged[f"{tax} Amount_2B"].fillna(0)
        )

    return merged.drop(columns="_merge")
