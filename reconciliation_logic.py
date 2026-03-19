import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz

# -------------------------------------------------
# 1️⃣ NORMALIZE DOCUMENT
# -------------------------------------------------
def normalize_doc(series):
    return (
        series.fillna("")
        .astype(str)
        .str.upper()
        .str.replace(r"[^A-Z0-9]", "", regex=True)
    )

# -------------------------------------------------
# 2️⃣ COLUMN VALIDATION
# -------------------------------------------------
def validate_columns(df, required_cols, df_name):
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {missing}")

# -------------------------------------------------
# 3️⃣ EXTRACT PAN FROM GSTIN
# -------------------------------------------------
def extract_pan(gstin):
    if pd.isna(gstin):
        return ""
    gstin = str(gstin)
    if len(gstin) < 15:
        return ""
    return gstin[2:12]  # PAN part

# -------------------------------------------------
# 4️⃣ MAIN RECON FUNCTION
# -------------------------------------------------
def process_reco(
    gst_df,
    pur_df,
    doc_threshold=75,
    tax_tolerance=10,
    gstin_mismatch_tolerance=20,
):

    gst = gst_df.copy()
    pur = pur_df.copy()

    # ---------------- REQUIRED COLUMNS ----------------
    gst_required = [
        "Supplier GSTIN", "Document Number", "Document Date",
        "Return Period", "Taxable Value",
        "Supplier Name", "IGST Amount", "CGST Amount",
        "SGST Amount", "Invoice Value",
    ]

    pur_required = [
        "GSTIN Of Vendor/Customer", "Reference Document No.",
        "Taxable Amount", "Document Date", "FI Document Number",
        "Vendor/Customer Name",
        "IGST Amount", "CGST Amount",
        "SGST Amount", "Invoice Value",
    ]

    validate_columns(gst, gst_required, "2B File")
    validate_columns(pur, pur_required, "Purchase File")

    # ---------------- PREP ----------------
    pur["Vendor/Customer GSTIN"] = pur["GSTIN Of Vendor/Customer"]

    gst["doc_norm"] = normalize_doc(gst["Document Number"])
    pur["doc_norm"] = normalize_doc(pur["Reference Document No."])

    pur.rename(columns={"GSTIN Of Vendor/Customer": "Supplier GSTIN"}, inplace=True)

    # ---------------- AGGREGATION ----------------
    gst_agg = gst.groupby(["Supplier GSTIN", "doc_norm"], as_index=False).agg({
        "Document Number": "first",
        "Return Period": "first",
        "Supplier Name": "first",
        "Document Date": "first",
        "IGST Amount": "sum",
        "CGST Amount": "sum",
        "SGST Amount": "sum",
        "Taxable Value": "sum",
        "Invoice Value": "sum",
    })

    pur_agg = pur.groupby(["Supplier GSTIN", "doc_norm"], as_index=False).agg({
        "Reference Document No.": "first",
        "Vendor/Customer GSTIN": "first",
        "Vendor/Customer Name": "first",
        "Document Date": "first",
        "FI Document Number": "first",
        "Taxable Amount": "sum",
        "IGST Amount": "sum",
        "CGST Amount": "sum",
        "SGST Amount": "sum",
        "Invoice Value": "sum",
    })

    # ---------------- MERGE ----------------
    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "doc_norm"],
        how="outer",
        suffixes=("_2B", "_PUR"),
        indicator=True,
    )

    # ---------------- NUMERIC CLEAN ----------------
    numeric_cols = [
        "IGST Amount_2B", "CGST Amount_2B", "SGST Amount_2B",
        "Invoice Value_2B", "Taxable Value",
        "IGST Amount_PUR", "CGST Amount_PUR", "SGST Amount_PUR",
        "Invoice Value_PUR", "Taxable Amount",
    ]

    for col in numeric_cols:
        if col not in merged.columns:
            merged[col] = 0
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0)

    # ---------------- DIFF ----------------
    merged["IGST Diff"] = merged["IGST Amount_PUR"] - merged["IGST Amount_2B"]
    merged["CGST Diff"] = merged["CGST Amount_PUR"] - merged["CGST Amount_2B"]
    merged["SGST Diff"] = merged["SGST Amount_PUR"] - merged["SGST Amount_2B"]
    merged["Invoice Diff"] = merged["Invoice Value_PUR"] - merged["Invoice Value_2B"]
    merged["Taxable Diff"] = merged["Taxable Amount"] - merged["Taxable Value"]

    merged["Match_Status"] = None
    merged["Fuzzy Score"] = 0.0

    both_mask = merged["_merge"] == "both"

    tax_condition = (
        (merged["IGST Diff"].abs() <= tax_tolerance) &
        (merged["CGST Diff"].abs() <= tax_tolerance) &
        (merged["SGST Diff"].abs() <= tax_tolerance) &
        (merged["Invoice Diff"].abs() <= tax_tolerance) &
        (merged["Taxable Diff"].abs() <= tax_tolerance)
    )

    merged.loc[both_mask & tax_condition, "Match_Status"] = "Exact Match"
    merged.loc[both_mask & ~tax_condition, "Match_Status"] = "Exact Doc - Value Mismatch"
    merged.loc[merged["_merge"] == "left_only", "Match_Status"] = "Open in 2B"
    merged.loc[merged["_merge"] == "right_only", "Match_Status"] = "Open in Books"

    # ---------------- FUZZY MATCH ----------------
    for gstin in merged["Supplier GSTIN"].dropna().unique():

        open_2b = merged[(merged["Supplier GSTIN"] == gstin) & (merged["Match_Status"] == "Open in 2B")]
        open_books = merged[(merged["Supplier GSTIN"] == gstin) & (merged["Match_Status"] == "Open in Books")]

        for left_idx in open_2b.index:
            left_doc = merged.at[left_idx, "doc_norm"]
            left_invoice = merged.at[left_idx, "Invoice Value_2B"]

            candidates = open_books[
                (open_books["Invoice Value_PUR"] - left_invoice).abs() <= tax_tolerance
            ]

            if candidates.empty:
                continue

            candidate_dict = dict(zip(candidates.index, candidates["doc_norm"]))
            match = process.extractOne(left_doc, candidate_dict, scorer=fuzz.ratio, score_cutoff=doc_threshold)

            if match:
                _, score, right_idx = match

                for col in merged.columns:
                    if col.endswith("_PUR") or col in [
                        "Reference Document No.",
                        "Vendor/Customer Name",
                        "FI Document Number",
                        "Taxable Amount",
                        "Vendor/Customer GSTIN",
                    ]:
                        if col in merged.columns:
                            merged.at[left_idx, col] = merged.at[right_idx, col]

                merged.at[left_idx, "Match_Status"] = "Fuzzy Match"
                merged.at[left_idx, "Fuzzy Score"] = score
                merged.at[right_idx, "Match_Status"] = "Fuzzy Consumed"

    merged = merged[merged["Match_Status"] != "Fuzzy Consumed"]

    # ---------------- GSTIN MISMATCH (FINAL FIXED) ----------------

    merged["PAN_2B"] = merged["Supplier GSTIN"].apply(extract_pan)
    merged["PAN_PUR"] = merged["Vendor/Customer GSTIN"].apply(extract_pan)

    for left_idx in merged[merged["Match_Status"] == "Open in 2B"].index:

        left_doc = merged.at[left_idx, "doc_norm"]
        left_val = merged.at[left_idx, "Invoice Value_2B"]
        left_pan = merged.at[left_idx, "PAN_2B"]
        left_gstin = merged.at[left_idx, "Supplier GSTIN"]

        candidates = merged[
            (merged["Match_Status"] == "Open in Books") &
            ((merged["Invoice Value_PUR"] - left_val).abs() <= gstin_mismatch_tolerance)
        ]

        best_match = None
        best_score = -1

        for right_idx in candidates.index:

            right_doc = merged.at[right_idx, "doc_norm"]
            right_pan = merged.at[right_idx, "PAN_PUR"]
            right_gstin = merged.at[right_idx, "Supplier GSTIN"]

            if left_pan == "" or left_pan != right_pan:
                continue

            if left_gstin == right_gstin:
                continue

            score = fuzz.ratio(left_doc, right_doc)

            if score > best_score:
                best_score = score
                best_match = right_idx

        if best_match is not None and best_score >= 70:
            merged.at[left_idx, "Match_Status"] = "GSTIN Mismatch"
            merged.at[left_idx, "Fuzzy Score"] = best_score
            merged.at[left_idx, "Vendor/Customer GSTIN"] = merged.at[best_match, "Vendor/Customer GSTIN"]

            merged.at[best_match, "Match_Status"] = "GSTIN Consumed"

    merged = merged[merged["Match_Status"] != "GSTIN Consumed"]

    merged.drop(columns=["_merge"], inplace=True)

    return merged
