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
# 3️⃣ MAIN RECON FUNCTION
# -------------------------------------------------
def process_reco(gst_df, pur_df, doc_threshold=75, tax_tolerance=10, 
                 gstin_mismatch_tolerance=20, date_window_days=30): # [Feature 3] Added Optional date window parameter

    gst = gst_df.copy()
    pur = pur_df.copy()

    # Define Required Columns
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

    # ---------------- PRESERVE ORIGINAL GSTIN & NORMALIZE ----------------
    pur["Vendor/Customer GSTIN"] = pur["GSTIN Of Vendor/Customer"]

    gst["doc_norm"] = normalize_doc(gst["Document Number"])
    pur["doc_norm"] = normalize_doc(pur["Reference Document No."])
    pur.rename(columns={"GSTIN Of Vendor/Customer": "Supplier GSTIN"}, inplace=True)

    # ---------------- [Feature 7] CREDIT NOTE SEGREGATION ----------------
    # Identify Credit Notes based on negative values
    gst["Doc_Type"] = np.where(pd.to_numeric(gst["Invoice Value"], errors='coerce') < 0, "Credit Note", "Invoice")
    pur["Doc_Type"] = np.where(pd.to_numeric(pur["Invoice Value"], errors='coerce') < 0, "Credit Note", "Invoice")

    # ---------------- [Feature 6] DUPLICATE DETECTION ----------------
    # Flag rows where the same GSTIN + Document combination appears multiple times before aggregating
    gst["Is_Duplicate"] = gst.duplicated(subset=["Supplier GSTIN", "doc_norm"], keep=False)
    pur["Is_Duplicate"] = pur.duplicated(subset=["Supplier GSTIN", "doc_norm"], keep=False)

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
        "Doc_Type": "first",
        "Is_Duplicate": "max" # Will be True if any duplicates existed
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
        "Doc_Type": "first",
        "Is_Duplicate": "max"
    })

    # ---------------- MERGE [Feature 1: Exact match (GSTIN + Doc)] ----------------
    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "doc_norm"],
        how="outer",
        suffixes=("_2B", "_PUR"),
        indicator=True,
    )

    # ---------------- NUMERIC CLEANING ----------------
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

    # Parse dates for windowing logic
    merged["Parsed_Date_2B"] = pd.to_datetime(merged["Document Date_2B"], errors="coerce")
    merged["Parsed_Date_PUR"] = pd.to_datetime(merged["Document Date_PUR"], errors="coerce")

    # ---------------- INITIAL DIFF CALCULATION [Feature 5] ----------------
    merged["IGST Diff"] = merged["IGST Amount_PUR"] - merged["IGST Amount_2B"]
    merged["CGST Diff"] = merged["CGST Amount_PUR"] - merged["CGST Amount_2B"]
    merged["SGST Diff"] = merged["SGST Amount_PUR"] - merged["SGST Amount_2B"]
    merged["Invoice Diff"] = merged["Invoice Value_PUR"] - merged["Invoice Value_2B"]
    merged["Taxable Diff"] = merged["Taxable Amount"] - merged["Taxable Value"]

    merged["Match_Status"] = None
    merged["Fuzzy Score"] = 0.0

    both_mask = merged["_merge"] == "both"
    
    # [Feature 2 & 4] Amount-based validation with Tolerance logic
    tax_condition = (
        (merged["IGST Diff"].abs() <= tax_tolerance) &
        (merged["CGST Diff"].abs() <= tax_tolerance) &
        (merged["SGST Diff"].abs() <= tax_tolerance) &
        (merged["Invoice Diff"].abs() <= tax_tolerance) &
        (merged["Taxable Diff"].abs() <= tax_tolerance)
    )

    # [Feature 9] Multi-level match statuses initial assignment
    merged.loc[both_mask & tax_condition, "Match_Status"] = "Exact Match"
    merged.loc[both_mask & ~tax_condition, "Match_Status"] = "Exact Doc - Amount Mismatch"
    merged.loc[merged["_merge"] == "left_only", "Match_Status"] = "Open in 2B"
    merged.loc[merged["_merge"] == "right_only", "Match_Status"] = "Open in Books"

    # ---------------- FUZZY MATCHING [Feature 3] ----------------
    for gstin in merged["Supplier GSTIN"].dropna().unique():
        open_2b = merged[(merged["Supplier GSTIN"] == gstin) & (merged["Match_Status"] == "Open in 2B")]
        open_books = merged[(merged["Supplier GSTIN"] == gstin) & (merged["Match_Status"] == "Open in Books")]

        for left_idx in open_2b.index:
            left_doc = merged.at[left_idx, "doc_norm"]
            left_invoice = merged.at[left_idx, "Invoice Value_2B"]
            left_date = merged.at[left_idx, "Parsed_Date_2B"]

            if not left_doc or pd.isna(left_doc):
                continue

            # Feature 3: Amount similarity
            candidates = open_books[(open_books["Invoice Value_PUR"] - left_invoice).abs() <= tax_tolerance]
            
            # Feature 3: Optional date window
            if date_window_days is not None and pd.notna(left_date):
                date_diffs = (candidates["Parsed_Date_PUR"] - left_date).dt.days.abs()
                candidates = candidates[(date_diffs <= date_window_days) | candidates["Parsed_Date_PUR"].isna()]

            if candidates.empty: continue

            # Feature 3: Doc similarity
            candidate_dict = dict(zip(candidates.index, candidates["doc_norm"]))
            match = process.extractOne(left_doc, candidate_dict, scorer=fuzz.ratio, score_cutoff=doc_threshold)

            if match:
                _, score, right_idx = match
                
                # Copy Purchase data to the 2B row
                pur_columns = [col for col in merged.columns if col.endswith("_PUR") or col in [
                    "Reference Document No.", "Vendor/Customer Name", 
                    "FI Document Number", "Taxable Amount", "Vendor/Customer GSTIN", "Doc_Type_PUR"
                ]]
                for col in pur_columns:
                    if col in merged.columns:
                        merged.at[left_idx, col] = merged.at[right_idx, col]

                # [Feature 9] Multi-level mapping
                merged.at[left_idx, "Match_Status"] = "Fuzzy Doc - Amount Match"
                merged.at[left_idx, "Fuzzy Score"] = score
                merged.at[right_idx, "Match_Status"] = "Fuzzy Consumed"

                open_books = open_books.drop(index=right_idx)

    merged = merged[merged["Match_Status"] != "Fuzzy Consumed"]

    # ---------------- GSTIN MISMATCH CHECK (Mapped to Partial Match) ----------------
    open_2b_final = merged[merged["Match_Status"] == "Open in 2B"]
    open_books_final = merged[merged["Match_Status"] == "Open in Books"]

    for left_idx in open_2b_final.index:
        left_doc = merged.at[left_idx, "doc_norm"]
        left_val = merged.at[left_idx, "Invoice Value_2B"]
        
        possible = open_books_final[open_books_final["doc_norm"] == left_doc]

        for right_idx in possible.index:
            right_val = merged.at[right_idx, "Invoice Value_PUR"]
            
            if abs(left_val - right_val) <= gstin_mismatch_tolerance:
                # [Feature 9] Used "Partial Match" for matching docs with wrong GSTINs
                merged.at[left_idx, "Match_Status"] = "Partial Match"
                merged.at[right_idx, "Match_Status"] = "Partial Match"
                
                merged.at[left_idx, "Vendor/Customer GSTIN"] = merged.at[right_idx, "Vendor/Customer GSTIN"]
                
                open_books_final = open_books_final.drop(index=right_idx)
                break 

    # Recalculate diffs after fuzzy merging
    merged["IGST Diff"] = merged["IGST Amount_PUR"] - merged["IGST Amount_2B"]
    merged["CGST Diff"] = merged["CGST Amount_PUR"] - merged["CGST Amount_2B"]
    merged["SGST Diff"] = merged["SGST Amount_PUR"] - merged["SGST Amount_2B"]
    merged["Invoice Diff"] = merged["Invoice Value_PUR"] - merged["Invoice Value_2B"]
    merged["Taxable Diff"] = merged["Taxable Amount"] - merged["Taxable Value"]

    # ---------------- [Feature 8] MATCH CONFIDENCE TAGGING ----------------
    confidence_conditions = [
        merged["Match_Status"] == "Exact Match",
        merged["Match_Status"].isin(["Exact Doc - Amount Mismatch", "Fuzzy Doc - Amount Match"]),
        merged["Match_Status"] == "Partial Match"
    ]
    confidence_choices = ["High", "Medium", "Low"]
    merged["Match_Confidence"] = np.select(confidence_conditions, confidence_choices, default="None")

    # Clean up temp columns
    merged.drop(columns=["_merge", "Parsed_Date_2B", "Parsed_Date_PUR"], inplace=True)

    return merged
