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
# CSS
st.markdown("""
    <style>
    .stApp { background-color: #F0F4F8; }

    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(20px); }
        to   { opacity: 1; transform: translateY(0); }
    }
    .animate-fade { animation: fadeIn 0.6s ease-out forwards; }

    .hero-header {
        background: linear-gradient(135deg, #0F172A 0%, #1E3A8A 50%, #F97316 100%);
        padding: 2.5rem 2rem;
        border-radius: 16px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 10px 25px rgba(30,58,138,0.2);
        position: relative;
        overflow: hidden;
    }

    .kpi-container {
        display: flex;
        justify-content: space-between;
        gap: 1rem;
        margin-bottom: 2rem;
    }
    .kpi-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        flex: 1;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        border-bottom: 4px solid #3B82F6;
        text-align: center;
        transition: transform 0.3s ease;
    }
    .kpi-card:hover { transform: translateY(-5px); }
    .kpi-card.orange { border-bottom: 4px solid #F97316; }
    .kpi-value { font-size: 2.2rem; font-weight: 800; color: #0F172A; margin: 0.5rem 0; }
    .kpi-label { font-size: 0.9rem; color: #64748B; text-transform: uppercase; letter-spacing: 1px; font-weight: 600; }

    .stButton>button {
        background: linear-gradient(135deg, #F97316 0%, #EA580C 100%);
        color: white;
        border: none;
        padding: 0.8rem 2rem;
        border-radius: 50px;
        font-weight: bold;
        font-size: 1.1rem;
        letter-spacing: 0.5px;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(249,115,22,0.3);
    }
    .stButton>button:hover {
        transform: translateY(-2px) scale(1.02);
        box-shadow: 0 6px 20px rgba(249,115,22,0.5);
        color: white;
    }

    .empty-state {
        background: white;
        padding: 4rem 2rem;
        text-align: center;
        border-radius: 16px;
        border: 2px dashed #CBD5E1;
        color: #64748B;
        margin-top: 2rem;
    }

    .side-header-2b {
        background: linear-gradient(90deg, #1E3A8A, #2563EB);
        color: white;
        padding: 0.55rem 1rem;
        border-radius: 8px;
        font-weight: 700;
        font-size: 0.95rem;
        letter-spacing: 0.4px;
        margin-bottom: 0.6rem;
    }
    .side-header-pur {
        background: linear-gradient(90deg, #C2410C, #F97316);
        color: white;
        padding: 0.55rem 1rem;
        border-radius: 8px;
        font-weight: 700;
        font-size: 0.95rem;
        letter-spacing: 0.4px;
        margin-bottom: 0.6rem;
    }

    .mm-row-card {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 0.5rem 0.75rem;
        margin-bottom: 0.35rem;
        font-size: 0.82rem;
        line-height: 1.7;
    }

    .ledger-hint {
        background: white;
        border: 2px dashed #CBD5E1;
        border-radius: 12px;
        padding: 2.5rem;
        text-align: center;
        color: #94A3B8;
        margin-top: 1rem;
    }
    </style>
""", unsafe_allow_html=True)
# ─────────────────────────────────────────────
st.markdown("""<style>
/* SAME CSS */
</style>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.markdown("""
<div class="hero-header animate-fade">
<h1>⚡ GST Intelligence Hub</h1>
<p>Automated GSTR-2B vs Books Reconciliation</p>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# UPLOAD
# ─────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 📘 GSTR-2B Data")
    gst_file = st.file_uploader("", type=["xlsx"], key="gst")

with col2:
    st.markdown("#### 📙 Purchase Register")
    pur_file = st.file_uploader("", type=["xlsx"], key="pur")

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# SESSION
# ─────────────────────────────────────────────
if "result_df" not in st.session_state:
    st.session_state["result_df"] = None

# ─────────────────────────────────────────────
# HELPERS
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

    if st.session_state["result_df"] is not None:
        result_df = st.session_state["result_df"].copy()

        work = result_df.copy()

        work["_PAN_2B"] = extract_pan(work.get("Supplier GSTIN", pd.Series(dtype=str)))
        work["_PAN_PUR"] = extract_pan(work.get("Vendor/Customer GSTIN", pd.Series(dtype=str)))

        st.markdown("### 🔍 Filter & Search")

        fs_left, fs_right = st.columns(2)

        # LEFT
        with fs_left:
            twoB_search_by = st.selectbox("Search by", ["— None —", "GSTIN", "PAN"], key="search_by_2b")
            twoB_search_val = st.text_input("Search GSTIN/PAN", key="search_val_2b")

        # RIGHT
        with fs_right:
            pur_search_by = st.selectbox("Search by ", ["— None —", "GSTIN", "PAN"], key="search_by_pur")
            pur_search_val = st.text_input("Search GSTIN/PAN ", key="search_val_pur")

        any_input = (
            (twoB_search_by != "— None —" and twoB_search_val.strip()) or
            (pur_search_by != "— None —" and pur_search_val.strip())
        )

        if any_input:

            masks = []

            gstin_2b = normalize_series(work.get("Supplier GSTIN", pd.Series("", index=work.index)))
            gstin_pur = normalize_series(work.get("Vendor/Customer GSTIN", pd.Series("", index=work.index)))
            pan_2b = normalize_series(work["_PAN_2B"])
            pan_pur = normalize_series(work["_PAN_PUR"])

            if twoB_search_by != "— None —" and twoB_search_val.strip():
                q = normalize_series(pd.Series([twoB_search_val])).iloc[0]
                m = pd.Series(True, index=work.index)

                if twoB_search_by == "GSTIN":
                    m &= gstin_2b.str.contains(q, regex=False)
                else:
                    m &= pan_2b.str.contains(q, regex=False)

                masks.append(m)

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

        st.dataframe(filtered, use_container_width=True)

else:
    st.info("Upload files to start")
