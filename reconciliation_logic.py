import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz


def normalize_doc(series):
    return (
        series.astype(str)
        .str.upper()
        .str.replace(r'[^A-Z0-9]', '', regex=True)
        .replace('NAN', '')
    )


def process_reco(gst, pur, doc_threshold=85, amount_tolerance=10):

    gst = gst.copy()
    pur = pur.copy()

    # -------------------------------------------------
    # 1️⃣ Normalize FIRST (Critical Fix)
    # -------------------------------------------------
    gst["doc_norm"] = normalize_doc(gst["Document Number"])
    pur["doc_norm"] = normalize_doc(pur["Reference Document No."])

    # -------------------------------------------------
    # 2️⃣ Aggregate on NORMALIZED doc
    # -------------------------------------------------
    gst_agg = (
        gst.groupby(["Supplier GSTIN", "doc_norm"], as_index=False)
        .agg({
            "Document Number": "first",
            "Supplier Name": "first",
            "2B Month": "first",
            "Document Date": "first",
            "IGST Amount": "sum",
            "CGST Amount": "sum",
            "SGST Amount": "sum",
            "Invoice Value": "sum"
        })
    )

    pur_agg = (
        pur.groupby(
            ["GSTIN Of Vendor/Customer", "doc_norm", "FI Document Number"],
            as_index=False
        )
        .agg({
            "Reference Document No.": "first",
            "Vendor/Customer Name": "first",
            "IGST Amount": "sum",
            "CGST Amount": "sum",
            "SGST Amount": "sum",
            "Invoice Value": "sum"
        })
        .rename(columns={
            "GSTIN Of Vendor/Customer": "Supplier GSTIN"
        })
    )

    # -------------------------------------------------
    # 3️⃣ Exact Match on NORMALIZED doc
    # -------------------------------------------------
    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "doc_norm"],
        how="outer",
        suffixes=("_2B", "_PUR"),
        indicator=True
    )

    merged["Match_Status"] = None
    merged["Fuzzy Score"] = 0.0

    both_mask = merged["_merge"] == "both"

    merged["diff Invoice Value"] = (
        merged["Invoice Value_PUR"].fillna(0)
        - merged["Invoice Value_2B"].fillna(0)
    )

    merged.loc[
        both_mask & (merged["diff Invoice Value"].abs() <= amount_tolerance),
        "Match_Status"
    ] = "Exact Match"

    merged.loc[
        both_mask & (merged["diff Invoice Value"].abs() > amount_tolerance),
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
    # 4️⃣ Fuzzy ONLY on unmatched
    # -------------------------------------------------
    left_df = merged[merged["Match_Status"] == "Open in 2B"].copy()
    right_df = merged[merged["Match_Status"] == "Open in Books"].copy()

    for gstin, left_grp in left_df.groupby("Supplier GSTIN"):

        right_grp = right_df[right_df["Supplier GSTIN"] == gstin]

        if right_grp.empty:
            continue

        for left_idx in left_grp.index:

            left_doc = merged.at[left_idx, "doc_norm"]
            left_amt = merged.at[left_idx, "Invoice Value_2B"]

            candidates = right_grp["doc_norm"].to_dict()

            match = process.extractOne(
                left_doc,
                candidates,
                scorer=fuzz.ratio,
                score_cutoff=doc_threshold,
                processor=None
            )

            if match:
                _, score, right_idx = match

                right_amt = merged.at[right_idx, "Invoice Value_PUR"]

                if abs((right_amt or 0) - (left_amt or 0)) <= amount_tolerance:

                    merged.at[left_idx, "Match_Status"] = "Fuzzy Match"
                    merged.at[left_idx, "Fuzzy Score"] = score

                    # Copy purchase columns
                    pur_cols = [c for c in merged.columns if c.endswith("_PUR")]
                    for col in pur_cols:
                        merged.at[left_idx, col] = merged.at[right_idx, col]

                    merged.at[right_idx, "Match_Status"] = "Fuzzy Consumed"

    merged = merged[merged["Match_Status"] != "Fuzzy Consumed"]

    merged.drop(columns=["_merge"], inplace=True)

    return merged


