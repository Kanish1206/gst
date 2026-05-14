import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz

# -------------------------------------------------
# CONSTANTS
# -------------------------------------------------
MATCH_EXACT = "Exact Match"
MATCH_VALUE_MISMATCH = "Value Mismatch"
MATCH_OPEN_2B = "Open in 2B"
MATCH_OPEN_BOOKS = "Open in Books"

MATCH_FUZZY = "Fuzzy Match"
MATCH_FUZZY_CONSUMED = "Fuzzy Consumed"

MATCH_GSTIN_MISMATCH = "GSTIN Mismatch"
MATCH_GSTIN_CONSUMED = "GSTIN Consumed"

MATCH_PAN = "PAN Match (GSTIN Variation)"
MATCH_PAN_CONSUMED = "PAN Consumed"

MATCH_DOC_IGNORE_GSTIN = "Doc Match (Ignore GSTIN)"
MATCH_DOC_CONSUMED = "Doc Consumed (Ignore GSTIN)"

# List of all consumed statuses to drop at the very end
CONSUMED_STATUSES = [
    MATCH_FUZZY_CONSUMED, 
    MATCH_GSTIN_CONSUMED, 
    MATCH_PAN_CONSUMED, 
    MATCH_DOC_CONSUMED
]

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
# 3️⃣ DIFF CALCULATION
# -------------------------------------------------
def compute_diffs(df):
    df["IGST Diff"] = df["IGST Amount_PUR"] - df["IGST Amount_2B"]
    df["CGST Diff"] = df["CGST Amount_PUR"] - df["CGST Amount_2B"]
    df["SGST Diff"] = df["SGST Amount_PUR"] - df["SGST Amount_2B"]
    return df

# -------------------------------------------------
# 4️⃣ MAIN FUNCTION
# -------------------------------------------------
def process_reco(
    gst_df,
    pur_df,
    doc_threshold=60,
    tax_tolerance=10,
    gstin_mismatch_tolerance=5,
):
    gst = gst_df.copy()
    pur = pur_df.copy()

    # ---------------- DOCUMENT TYPE ----------------
    doc_type_map = {
        "INVOICE": "R",
        "CREDIT NOTE": "C",
        "DEBIT NOTE": "D",
    }
    pur["Document Type"] = pur["Invoice Type"].map(doc_type_map).fillna("UNKNOWN")

    # ---------------- VALIDATION ----------------
    gst_required = [
        "Supplier GSTIN", "Document Number", "Document Date",
        "Return Period", "Taxable Value", "Supplier Name",
        "IGST Amount", "CGST Amount", "SGST Amount", "Invoice Value","Document Type"
    ]
    pur_required = [
        "GSTIN Of Vendor/Customer", "Reference Document No.",
        "Taxable Amount", "Document Date",
        "Vendor/Customer Name", "IGST Amount", "CGST Amount",
        "SGST Amount", "Invoice Value", "Invoice Type"
    ]

    validate_columns(gst, gst_required, "2B File")
    validate_columns(pur, pur_required, "Purchase File")

    # ---------------- PREP ----------------
    pur["Vendor/Customer GSTIN"] = pur["GSTIN Of Vendor/Customer"]
    gst["Return Period"] = gst["Return Period"].astype(str)

    gst["doc_norm"] = normalize_doc(gst["Document Number"])
    pur["doc_norm"] = normalize_doc(pur["Reference Document No."])

    pur.rename(columns={"GSTIN Of Vendor/Customer": "Supplier GSTIN"}, inplace=True)

    # ---------------- AGGREGATION ----------------
    gst_agg = gst.groupby(
        ["Supplier GSTIN", "doc_norm","Document Type"], as_index=False 
    ).agg({
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
    
    pur_agg = pur.groupby(
        ["Supplier GSTIN", "doc_norm", "Document Type"], as_index=False
    ).agg({
        "Reference Document No.": "first",
        "Vendor/Customer GSTIN": "first",
        "FI Document Number": "first",
        "Vendor/Customer Name": "first",
        "Document Date": "first",
        "Taxable Amount": "sum",
        "IGST Amount": "sum",
        "CGST Amount": "sum",
        "SGST Amount": "sum",
        "Invoice Value": "sum",
    })

    # ---------------- MERGE ----------------
    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "doc_norm", "Document Type"],
        how="outer",
        suffixes=["_2B", "_PUR"],
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

    # ---------------- INITIAL MATCH ----------------
    merged = compute_diffs(merged)

    merged["Match_Status"] = None
    merged["Fuzzy Score"] = 0.0

    both_mask = merged["_merge"] == "both"
    tax_condition = (
        (merged["IGST Diff"].abs() <= tax_tolerance) &
        (merged["CGST Diff"].abs() <= tax_tolerance) &
        (merged["SGST Diff"].abs() <= tax_tolerance)
    )

    merged.loc[both_mask & tax_condition, "Match_Status"] = MATCH_EXACT
    merged.loc[both_mask & ~tax_condition, "Match_Status"] = MATCH_VALUE_MISMATCH
    merged.loc[merged["_merge"] == "left_only", "Match_Status"] = MATCH_OPEN_2B
    merged.loc[merged["_merge"] == "right_only", "Match_Status"] = MATCH_OPEN_BOOKS

    # =========================================================================
    # 🛠️ HELPER FUNCTION: Safely copy data and consume right_idx
    # =========================================================================
    def apply_cross_match(l_idx, r_idx, match_label, consumed_label, fuzzy_score=0.0):
        merged.at[l_idx, "Match_Status"] = match_label
        merged.at[r_idx, "Match_Status"] = consumed_label
        merged.at[l_idx, "Fuzzy Score"] = fuzzy_score
        
        pur_cols = [col for col in merged.columns if col.endswith("_PUR") or col in [
            "Reference Document No.", "FI Document Number", "Vendor/Customer Name", "Vendor/Customer GSTIN"
        ]]
        
        # Copy pur columns over to the 2B row
        for c in pur_cols:
            if c in merged.columns:
                merged.at[l_idx, c] = merged.at[r_idx, c]

    # ---------------- FUZZY MATCH ----------------
    for gstin in merged["Supplier GSTIN"].dropna().unique():
        open_2b = merged[(merged["Supplier GSTIN"] == gstin) & (merged["Match_Status"] == MATCH_OPEN_2B)]
        open_books = merged[(merged["Supplier GSTIN"] == gstin) & (merged["Match_Status"] == MATCH_OPEN_BOOKS)]

        for left_idx in open_2b.index:
            left_doc = str(merged.at[left_idx, "Document Number"])
            if not left_doc or open_books.empty:
                continue

            candidates = open_books.copy()
            candidates["tax_score"] = (
                (candidates["IGST Amount_PUR"] - merged.at[left_idx, "IGST Amount_2B"]).abs() +
                (candidates["CGST Amount_PUR"] - merged.at[left_idx, "CGST Amount_2B"]).abs() +
                (candidates["SGST Amount_PUR"] - merged.at[left_idx, "SGST Amount_2B"]).abs()
            )

            candidates = candidates[candidates["tax_score"] <= tax_tolerance * 2]
            if candidates.empty:
                continue

            candidate_dict = dict(zip(candidates.index, candidates["Reference Document No."].astype(str)))
            match = process.extractOne(
                left_doc, candidate_dict, scorer=fuzz.partial_token_set_ratio, score_cutoff=doc_threshold
            )

            if match:
                _, score, right_idx = match
                # ✅ Use Helper
                apply_cross_match(left_idx, right_idx, MATCH_FUZZY, MATCH_FUZZY_CONSUMED, score)
                open_books = open_books.drop(index=right_idx) # Remove from candidate pool

    # ---------------- GSTIN MISMATCH ----------------
    # Refresh pools based on current un-matched statuses
    open_2b = merged[merged["Match_Status"] == MATCH_OPEN_2B]
    open_books = merged[merged["Match_Status"] == MATCH_OPEN_BOOKS]

    for left_idx in open_2b.index:
        doc = merged.at[left_idx, "doc_norm"]
        if not doc:
            continue

        left_val = merged.at[left_idx, "Invoice Value_2B"]
        left_igst = merged.at[left_idx, "IGST Amount_2B"]
        left_cgst = merged.at[left_idx, "CGST Amount_2B"]
        left_sgst = merged.at[left_idx, "SGST Amount_2B"]

        possible = open_books[open_books["doc_norm"] == doc]
        if possible.empty or len(possible) > 3: # Avoid 1234 generic matching 10 different vendors
            continue

        for right_idx in possible.index:
            right_val = merged.at[right_idx, "Invoice Value_PUR"]
            right_igst = merged.at[right_idx, "IGST Amount_PUR"]
            right_cgst = merged.at[right_idx, "CGST Amount_PUR"]
            right_sgst = merged.at[right_idx, "SGST Amount_PUR"]

            if (
                abs(left_val - right_val) <= gstin_mismatch_tolerance and
                abs(left_igst - right_igst) <= tax_tolerance and
                abs(left_cgst - right_cgst) <= tax_tolerance and
                abs(left_sgst - right_sgst) <= tax_tolerance
            ):
                # ✅ Use Helper (Fixes previous bug where data wasn't copied!)
                apply_cross_match(left_idx, right_idx, MATCH_GSTIN_MISMATCH, MATCH_GSTIN_CONSUMED)
                open_books = open_books.drop(index=right_idx)
                break

    # ---------------- PAN MATCH ----------------
    open_2b_for_pan = merged[merged["Match_Status"] == MATCH_OPEN_2B]
    open_books_for_pan = merged[merged["Match_Status"] == MATCH_OPEN_BOOKS]

    merged["PAN_2B"] = merged["Supplier GSTIN"].fillna("").astype(str).str.strip().str.upper().str[2:12]
    merged["PAN_PUR"] = merged["Vendor/Customer GSTIN"].fillna("").astype(str).str.strip().str.upper().str[2:12]

    for left_idx in open_2b_for_pan.index:
        pan_2b = merged.at[left_idx, "PAN_2B"]
        doc_2b = merged.at[left_idx, "doc_norm"]

        if not pan_2b or not doc_2b:
            continue

        igst_2b = merged.at[left_idx, "IGST Amount_2B"]
        cgst_2b = merged.at[left_idx, "CGST Amount_2B"]
        sgst_2b = merged.at[left_idx, "SGST Amount_2B"]

        candidates = merged[
            (merged.index.isin(open_books_for_pan.index)) &
            (merged["PAN_PUR"] == pan_2b) &
            (merged["doc_norm"] == doc_2b)
        ].copy()

        if candidates.empty:
            continue

        candidates["tax_diff"] = (
            (candidates["IGST Amount_PUR"] - igst_2b).abs() +
            (candidates["CGST Amount_PUR"] - cgst_2b).abs() +
            (candidates["SGST Amount_PUR"] - sgst_2b).abs()
        )

        candidates = candidates.sort_values("tax_diff")

        for right_idx in candidates.index:
            if candidates.at[right_idx, "tax_diff"] > tax_tolerance * 3:
                continue

            # ✅ Use Helper
            apply_cross_match(left_idx, right_idx, MATCH_PAN, MATCH_PAN_CONSUMED)
            open_books_for_pan = open_books_for_pan.drop(index=right_idx)
            break

    # Drop PAN helpers safely
    merged.drop(columns=["PAN_2B", "PAN_PUR"], inplace=True, errors="ignore")

    # ---------------- FINAL MATCH (IGNORE GSTIN) ----------------
    open_2b_final = merged[merged["Match_Status"] == MATCH_OPEN_2B]
    open_books_final = merged[merged["Match_Status"] == MATCH_OPEN_BOOKS]

    for left_idx in open_2b_final.index:
        doc_2b = merged.at[left_idx, "doc_norm"]
        if not doc_2b:
            continue

        igst_2b = merged.at[left_idx, "IGST Amount_2B"]
        cgst_2b = merged.at[left_idx, "CGST Amount_2B"]
        sgst_2b = merged.at[left_idx, "SGST Amount_2B"]

        candidates = open_books_final[open_books_final["doc_norm"] == doc_2b].copy()
        if candidates.empty:
            continue

        candidates["tax_diff"] = (
            (candidates["IGST Amount_PUR"] - igst_2b).abs() +
            (candidates["CGST Amount_PUR"] - cgst_2b).abs() +
            (candidates["SGST Amount_PUR"] - sgst_2b).abs()
        )

        candidates = candidates.sort_values("tax_diff")

        for right_idx in candidates.index:
            if candidates.at[right_idx, "tax_diff"] > tax_tolerance * 3:
                continue

            # ✅ Use Helper
            apply_cross_match(left_idx, right_idx, MATCH_DOC_IGNORE_GSTIN, MATCH_DOC_CONSUMED)
            open_books_final = open_books_final.drop(index=right_idx)
            break

    # ---------------- CLEANUP ----------------
    # ⚠️ Safely drop ALL consumed rows exactly once at the very end
    merged = merged[~merged["Match_Status"].isin(CONSUMED_STATUSES)]
    
    # Re-run diffs to account for all newly merged data
    merged = compute_diffs(merged)
    merged.drop(columns=["_merge"], inplace=True, errors="ignore")

    priority_cols = [
       "Supplier GSTIN", "doc_norm", "Document Type", "Document Number",
       "Return Period", "Supplier Name", "Document Date_2B",
       "IGST Amount_2B", "CGST Amount_2B", "SGST Amount_2B", "Taxable Value",
       "Invoice Value_2B", "FI Document Number", "Reference Document No.",
       "Vendor/Customer GSTIN", "Vendor/Customer Name", "Document Date_PUR",
       "IGST Amount_PUR", "CGST Amount_PUR", "SGST Amount_PUR",
       "Taxable Amount", "Invoice Value_PUR", "IGST Diff", "CGST Diff",
       "SGST Diff", "Match_Status", "Fuzzy Score",
    ]

    # Keep only columns that exist (avoids errors)
    priority_cols = [col for col in priority_cols if col in merged.columns]
    merged = merged[priority_cols + [col for col in merged.columns if col not in priority_cols]]
    
    return merged
