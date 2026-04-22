import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz
import os
import sqlite3
import json  # ✅ FIXED

DB_FILE = "reco_storage.db"

# ================= DB INIT =================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS open_items (
        doc_norm TEXT,
        supplier_gstin TEXT,
        data TEXT
    )
    """)

    conn.commit()
    conn.close()


# ================= SAVE TO DB =================
def save_to_db(df):
    try:
        init_db()
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        open_df = df[df["Match_Status"].isin(["Open in 2B", "Open in Books"])]

        for _, row in open_df.iterrows():
            cursor.execute("""
                INSERT INTO open_items (doc_norm, supplier_gstin, data)
                VALUES (?, ?, ?)
            """, (
                row.get("doc_norm"),
                row.get("Supplier GSTIN"),
                json.dumps(row.to_dict(), default=str)
            ))

        conn.commit()
        conn.close()

    except Exception as e:
        print("❌ DB SAVE ERROR:", e)


# ================= LOAD FROM DB =================
def load_open_items():
    try:
        init_db()
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        cursor.execute("SELECT data FROM open_items")
        rows = cursor.fetchall()

        conn.close()

        data = [json.loads(r[0]) for r in rows]

        return pd.DataFrame(data) if data else pd.DataFrame()

    except Exception as e:
        print("❌ LOAD ERROR:", e)
        return pd.DataFrame()


# ================= APPLY LEARNING =================
def apply_previous_matches(current_df):
    old_df = load_open_items()

    if old_df.empty:
        return current_df

    for idx in current_df.index:
        if current_df.at[idx, "Match_Status"] in ["Open in 2B", "Open in Books"]:

            doc = current_df.at[idx, "doc_norm"]
            gstin = current_df.at[idx, "Supplier GSTIN"]

            match = old_df[
                (old_df["doc_norm"] == doc) &
                (old_df["Supplier GSTIN"] == gstin)
            ]

            if not match.empty:
                row = match.iloc[0]

                for col in current_df.columns:
                    if col in row:
                        current_df.at[idx, col] = row[col]

                current_df.at[idx, "Match_Status"] = "Auto Matched (AI Learned)"

    return current_df


# ================= ORIGINAL LOGIC (UNCHANGED) =================

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
        raise ValueError(f"{df_name} missing columns: {missing}")


def compute_diffs(df):
    df["IGST Diff"] = df["IGST Amount_PUR"] - df["IGST Amount_2B"]
    df["CGST Diff"] = df["CGST Amount_PUR"] - df["CGST Amount_2B"]
    df["SGST Diff"] = df["SGST Amount_PUR"] - df["SGST Amount_2B"]
    return df


def process_reco(gst_df, pur_df, doc_threshold=60, tax_tolerance=10):

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

    pur.rename(columns={"GSTIN Of Vendor/Customer": "Supplier GSTIN"}, inplace=True)

    gst["doc_norm"] = normalize_doc(gst["Document Number"])
    pur["doc_norm"] = normalize_doc(pur["Reference Document No."])

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

    for col in merged.columns:
        if "Amount" in col or "Value" in col:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0)

    merged = compute_diffs(merged)

    merged["Match_Status"] = None

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

    merged.drop(columns=["_merge"], inplace=True)

    # ✅ SAVE TO DB
    save_to_db(merged)

    return merged
