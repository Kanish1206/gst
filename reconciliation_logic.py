import pandas as pd
from rapidfuzz import process, fuzz


def process_reco(gst, pur,threshold=90):
    """
    GST 2B vs Purchase Register Reconciliation
    """

    # ----------------------------
    # Basic cleaning
    # ----------------------------
    pur["FI Document Number"] = pur["FI Document Number"].astype(str)
    gst["Document Number"] = gst["Document Number"].astype(str)

    # ----------------------------
    # Aggregation
    # ----------------------------
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
            ["GSTIN Of Vendor/Customer", "Reference Document No."],
            as_index=False
        )
        .agg({
            "Vendor/Customer Name": "first",
            "FI Document Number":"first",
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

    # ----------------------------
    # Exact Match Merge
    # ----------------------------
    merged = gst_agg.merge(
        pur_agg,
        on=["Supplier GSTIN", "Document Number"],
        how="outer",
        suffixes=("_2B", "_PUR"),
        indicator=True
    )

    # ----------------------------
    # Match Status (FORCE OBJECT)
    # ----------------------------
    merged["Match_Status"] = merged["_merge"].map({
        "both": "Exact Match",
        "left_only": "Open in 2B",
        "right_only": "Open in Books"
    }).astype("object")   # 🔑 critical fix

    merged["Matched_Doc_no._other_Side"] = None
    merged["Fuzzy Score"] = 0.0

    # ----------------------------
    # Prepare Fuzzy Matching
    # ----------------------------
    left_only_df = merged[merged["_merge"] == "left_only"].copy()
    right_only_df = merged[merged["_merge"] == "right_only"].copy()

    left_only_df["Document Number_str"] = left_only_df["Document Number"].astype(str)
    right_only_df["Document Number_str"] = right_only_df["Document Number"].astype(str)

    common_gstins = set(left_only_df["Supplier GSTIN"]) & set(right_only_df["Supplier GSTIN"])

    #threshold = 90
    used_pur_indexes = set()
    rows_to_drop = []

    # ----------------------------
    # Fuzzy Matching Logic
    # ----------------------------
    for gstin in common_gstins:
        left_subset = left_only_df[left_only_df["Supplier GSTIN"] == gstin]
        right_subset = right_only_df[right_only_df["Supplier GSTIN"] == gstin]

        right_map = right_subset["Document Number_str"].to_dict()

        for left_idx, left_row in left_subset.iterrows():
            query = left_row["Document Number_str"]

            match = process.extractOne(
                query,
                right_map,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=threshold
            )

            if not match:
                continue

            matched_str, score, pur_idx = match

            if pur_idx in used_pur_indexes:
                continue

            used_pur_indexes.add(pur_idx)

            # ✅ SAFE ASSIGNMENTS
            merged.loc[left_idx, "Match_Status"] = "Fuzzy Match"
            merged.loc[left_idx, "Matched_Doc_no._other_Side"] = matched_str
            merged.loc[left_idx, "Fuzzy Score"] = score

            pur_cols = [c for c in merged.columns if c.endswith("_PUR")]
            merged.loc[left_idx, pur_cols] = merged.loc[pur_idx, pur_cols].values

            rows_to_drop.append(pur_idx)

    merged.drop(index=rows_to_drop, inplace=True, errors="ignore")

    # ----------------------------
    # Tax Difference
    # ----------------------------
    merged["diff IGST"] = merged["IGST Amount_PUR"].fillna(0) - merged["IGST Amount_2B"].fillna(0)
    merged["diff CGST"] = merged["CGST Amount_PUR"].fillna(0) - merged["CGST Amount_2B"].fillna(0)
    merged["diff SGST"] = merged["SGST Amount_PUR"].fillna(0) - merged["SGST Amount_2B"].fillna(0)

    return merged



