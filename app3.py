import sys
import os

sys.path.append(r"C:\Users\Admin\GST")

import streamlit as st
import pandas as pd
import io
import reconciliation_logic as reco_logic

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="GST Reco Pro | Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─────────────────────────────────────────────
# CSS (UNCHANGED)
# ─────────────────────────────────────────────
st.markdown("""<style>
/* KEEPING YOUR CSS SAME */
</style>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# HEADER (UNCHANGED)
# ─────────────────────────────────────────────
st.markdown("""<div class="hero-header animate-fade">
<h1>⚡ GST Intelligence Hub</h1>
<p>Automated GSTR-2B vs Books Reconciliation</p>
</div>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# UPLOAD (UNCHANGED)
# ─────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    st.markdown("#### 📘 GSTR-2B Data")
    gst_file = st.file_uploader("Drop GSTR-2B Excel here", type=["xlsx"], key="gst", label_visibility="collapsed")
with col2:
    st.markdown("#### 📙 Purchase Register")
    pur_file = st.file_uploader("Drop Purchase Books Excel here", type=["xlsx"], key="pur", label_visibility="collapsed")

# ─────────────────────────────────────────────
# HELPERS (UPDATED ONLY HERE)
# ─────────────────────────────────────────────

def normalize_series(series):
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
    )

def extract_pan(gstin_series):
    s = normalize_series(gstin_series)
    return s.apply(lambda x: x[2:12] if len(x) >= 12 else "")

def fmt_amt(val):
    try:
        v = float(val)
        return f"₹{v:,.2f}" if v != 0 else "—"
    except:
        return "—"

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if gst_file and pur_file:

    df_2b = pd.read_excel(gst_file)
    df_books = pd.read_excel(pur_file)

    df_2b.columns = df_2b.columns.str.strip()
    df_books.columns = df_books.columns.str.strip()

    if st.button("🚀 INITIATE PROCESS"):
        result_df = reco_logic.process_reco(df_2b, df_books)
        st.session_state["result_df"] = result_df

    if "result_df" in st.session_state:
        result_df = st.session_state["result_df"].copy()

        # ─────────────────────────────────────────────
        # FILTER + SEARCH (ONLY OPTIMIZED PART)
        # ─────────────────────────────────────────────

        work = result_df.copy()

        work["_PAN_2B"] = extract_pan(work.get("Supplier GSTIN", pd.Series(dtype=str)))
        work["_PAN_PUR"] = extract_pan(work.get("Vendor/Customer GSTIN", pd.Series(dtype=str)))

        st.markdown("### 🔍 Filter & Search")

        col1, col2 = st.columns(2)

        with col1:
            twoB_search_by = st.selectbox("Search by", ["— None —", "GSTIN", "PAN"])
            twoB_search_val = st.text_input("Search 2B")

        with col2:
            pur_search_by = st.selectbox("Search by ", ["— None —", "GSTIN", "PAN"])
            pur_search_val = st.text_input("Search Books")

        any_input = (
            (twoB_search_by != "— None —" and twoB_search_val.strip()) or
            (pur_search_by != "— None —" and pur_search_val.strip())
        )

        if any_input:

            masks = []

            # CLEAN DATA ONCE
            gstin_2b = normalize_series(work.get("Supplier GSTIN", pd.Series("", index=work.index)))
            gstin_pur = normalize_series(work.get("Vendor/Customer GSTIN", pd.Series("", index=work.index)))
            pan_2b = normalize_series(work["_PAN_2B"])
            pan_pur = normalize_series(work["_PAN_PUR"])

            # 2B SEARCH
            if twoB_search_by != "— None —" and twoB_search_val.strip():
                q = normalize_series(pd.Series([twoB_search_val])).iloc[0]
                m = pd.Series(True, index=work.index)

                if twoB_search_by == "GSTIN":
                    m &= gstin_2b.str.contains(q, regex=False)
                else:
                    m &= pan_2b.str.contains(q, regex=False)

                masks.append(m)

            # BOOKS SEARCH
            if pur_search_by != "— None —" and pur_search_val.strip():
                q = normalize_series(pd.Series([pur_search_val])).iloc[0]
                m = pd.Series(True, index=work.index)

                if pur_search_by == "GSTIN":
                    m &= gstin_pur.str.contains(q, regex=False)
                else:
                    m &= pan_pur.str.contains(q, regex=False)

                masks.append(m)

            combined = masks[0]
            for m in masks[1:]:
                combined = combined | m

            filtered = work[combined]

        else:
            filtered = pd.DataFrame(columns=work.columns)

        # ─────────────────────────────────────────────
        # DISPLAY (UNCHANGED)
        # ─────────────────────────────────────────────

        st.dataframe(filtered, use_container_width=True)

        # EXPORT
        output = io.BytesIO()
        filtered.to_excel(output, index=False)

        st.download_button(
            "📥 Download",
            output.getvalue(),
            "report.xlsx"
        )

else:
    st.info("Upload files to start")
