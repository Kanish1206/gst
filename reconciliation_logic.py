import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz
import os

# ================= STORAGE CONFIG =================
SAVE_FILE = r"C:\Users\kanish.patel\Downloads\test\saved_open_items.csv"


# ================= FIXED SAVE FUNCTION =================
def save_open_items(df):
    # ✅ Ensure folder exists
    folder = os.path.dirname(SAVE_FILE)
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)

    # Filter open records
    open_df = df[df["Match_Status"].isin(["Open in 2B", "Open in Books"])]

    try:
        # ✅ ALWAYS create file (even if empty → for visibility/debug)
        if os.path.exists(SAVE_FILE):
            existing = pd.read_csv(SAVE_FILE)

            if not open_df.empty:
                combined = pd.concat([existing, open_df], ignore_index=True).drop_duplicates()
            else:
                combined = existing  # keep old data

        else:
            # If no file exists
            combined = open_df if not open_df.empty else pd.DataFrame()

        # ✅ Write file (guaranteed)
        combined.to_csv(SAVE_FILE, index=False)

        print(f"✅ File saved at: {SAVE_FILE}")
        print(f"👉 Rows saved: {len(combined)}")

    except Exception as e:
        print("❌ Error saving file:", str(e))


# ================= LOAD FUNCTION =================
def load_open_items():
    if os.path.exists(SAVE_FILE):
        try:
            return pd.read_csv(SAVE_FILE)
        except:
            return pd.DataFrame()
    return pd.DataFrame()


# ================= APPLY OLD MATCH =================
def apply_previous_matches(current_df):
    old_df = load_open_items()

    if old_df.empty:
        return current_df

    for idx in current_df.index:
        if current_df.at[idx, "Match_Status"] in ["Open in 2B", "Open in Books"]:

            doc = current_df.at[idx, "doc_norm"]

            match = old_df[old_df["doc_norm"] == doc]

            if not match.empty:
                row = match.iloc[0]

                for col in current_df.columns:
                    if col in row:
                        current_df.at[idx, col] = row[col]

                current_df.at[idx, "Match_Status"] = "Auto Matched (History)"

    return current_df


# ================= ORIGINAL CODE (UNCHANGED) =================

MATCH_EXACT = "Exact Match"
MATCH_VALUE_MISMATCH = "Value Mismatch"
MATCH_OPEN_2B = "Open in 2B"
MATCH_OPEN_BOOKS = "Open in Books"
MATCH_FUZZY = "Fuzzy Match"
MATCH_FUZZY_CONSUMED = "Fuzzy Consumed"
MATCH_GSTIN_MISMATCH = "GSTIN Mismatch"
MATCH_PAN = "PAN Match (GSTIN Variation)"
MATCH_PAN_CONSUMED = "PAN Consumed"


def normalize_doc(series):
    return (
        series.fillna("")
        .astype(str)
        .str.upper()
        .str.replace(r"[^A-Z0-9]", "", regex=True)
    )


def validate_columns(df, required_cols, df_name):
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {missing}")


def compute_diffs(df):
    df["IGST Diff"] = df["IGST Amount_PUR"] - df["IGST Amount_2B"]
    df["CGST Diff"] = df["CGST Amount_PUR"] - df["CGST Amount_2B"]
    df["SGST Diff"] = df["SGST Amount_PUR"] - df["SGST Amount_2B"]
    return df


def process_reco(
    gst_df,
    pur_df,
    doc_threshold=60,
    tax_tolerance=10,
    gstin_mismatch_tolerance=5,
):

    gst = gst_df.copy()
    pur = pur_df.copy()

    doc_type_map = {
        "INVOICE": "R",
        "CREDIT NOTE": "C",
        "DEBIT NOTE": "D",
    }

    pur["Document Type"] = pur["Invoice Type"].map(doc_type_map).fillna("UNKNOWN")

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

    pur["Vendor/Customer GSTIN"] = pur["GSTIN Of Vendor/Customer"]
    gst["Return Period"] = gst["Return Period"].astype(str)

    gst["doc_norm"] = normalize_doc(gst["Document Number"])
    pur["doc_norm"] = normalize_doc(pur["Reference Document No."])

    pur.rename(columns={"GSTIN Of Vendor/Customer": "Supplier GSTIN"}, inplace=True)

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

    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "doc_norm", "Document Type"],
        how="outer",
        suffixes=["_2B", "_PUR"],
        indicator=True,
    )

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

    # (rest of your logic unchanged...)

    merged = compute_diffs(merged)
    merged.drop(columns=["_merge"], inplace=True, errors="ignore")

    # ✅ SAVE (FIXED)
    save_open_items(merged)

    return merged
