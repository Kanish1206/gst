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
# ─────────────────────────────────────────────
st.markdown("""
    <style>
    .stApp { background-color: #F0F4F8; }
    
    .stMarkdown, .stText, label, .stSelectbox, .stTextInput { 
        color: #0F172A !important; 
    }

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
    </style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.markdown("""
    <div class="hero-header animate-fade">
        <h1 style='margin:0; font-size:3rem; font-weight:800; text-shadow:2px 2px 4px rgba(0,0,0,0.3);'>
            ⚡ GST Intelligence Hub
        </h1>
        <p style='margin:5px 0 0 0; font-size:1.2rem; opacity:0.9;'>
            Automated GSTR-2B vs Books Reconciliation
        </p>
    </div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# UPLOAD
# ─────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    st.markdown("#### 📘 GSTR-2B Data")
    gst_file = st.file_uploader("Drop GSTR-2B Excel here", type=["xlsx"], key="gst", label_visibility="collapsed")
with col2:
    st.markdown("#### 📙 Purchase Register")
    pur_file = st.file_uploader("Drop Purchase Books Excel here", type=["xlsx"], key="pur", label_visibility="collapsed")

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
ALL_STATUSES = [
    reco_logic.MATCH_EXACT,
    reco_logic.MATCH_VALUE_MISMATCH,
    reco_logic.MATCH_OPEN_2B,
    reco_logic.MATCH_OPEN_BOOKS,
    reco_logic.MATCH_FUZZY,
    reco_logic.MATCH_GSTIN_MISMATCH,
    reco_logic.MATCH_PAN,
    "Doc Match (Ignore GSTIN)",
    "Manual Match",
]

STATUS_COLORS = {
    reco_logic.MATCH_EXACT:          "background-color: #D1FAE5",
    reco_logic.MATCH_VALUE_MISMATCH: "background-color: #FFEDD5",
    reco_logic.MATCH_OPEN_2B:        "background-color: #DBEAFE",
    reco_logic.MATCH_OPEN_BOOKS:     "background-color: #FEF9C3",
    reco_logic.MATCH_FUZZY:          "background-color: #EDE9FE",
    reco_logic.MATCH_GSTIN_MISMATCH: "background-color: #FFE4E6",
    reco_logic.MATCH_PAN:            "background-color: #CFFAFE",
    "Doc Match (Ignore GSTIN)":      "background-color: #F0FDF4",
    "Manual Match":                  "background-color: #FDF4FF",
}

PUR_COPY_COLS = [
    "Reference Document No.", "FI Document Number",
    "Vendor/Customer Name",  "Vendor/Customer GSTIN",
    "IGST Amount_PUR", "CGST Amount_PUR", "SGST Amount_PUR"
]

# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
if "result_df"      not in st.session_state: st.session_state["result_df"]      = None
if "manual_matches" not in st.session_state: st.session_state["manual_matches"] = []

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def extract_pan(gstin_series):
    return gstin_series.fillna("").astype(str).str.strip().str.upper().str[2:12]

def fmt_amt(val):
    try:
        v = float(val)
        return f"₹{v:,.2f}" if v != 0 else "—"
    except Exception:
        return "—"

def style_status(val):
    return STATUS_COLORS.get(val, "")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if gst_file and pur_file:
    try:
        df_2b    = pd.read_excel(gst_file)
        df_books = pd.read_excel(pur_file)
        df_2b.columns    = df_2b.columns.str.strip()
        df_books.columns = df_books.columns.str.strip()

        _, btn_col, _ = st.columns([1, 2, 1])
        with btn_col:
            run_btn = st.button("🚀 INITIATE PROCESS", use_container_width=True)

        if run_btn:
            with st.spinner("🧠 Please Wait!..."):
                result_df = reco_logic.process_reco(df_2b, df_books)
            st.session_state["result_df"]      = result_df
            st.session_state["manual_matches"] = []

        if st.session_state["result_df"] is not None:
            result_df = st.session_state["result_df"].copy()

            st.markdown('<div class="animate-fade">', unsafe_allow_html=True)

            # ── KPIs ────────────────────────────────────────────────
            total     = len(result_df)
            is_match  = result_df["Match_Status"].str.contains("Match", case=False, na=False)
            is_fuzzy  = result_df["Match_Status"].str.contains("Fuzzy", case=False, na=False)
            matched   = (is_match & ~is_fuzzy).sum()
            unmatched = total - matched

            st.markdown(f"""
                <div class="kpi-container">
                    <div class="kpi-card">
                        <div class="kpi-label">Total Invoices Processed</div>
                        <div class="kpi-value">{total:,}</div>
                    </div>
                    <div class="kpi-card" style="border-bottom-color:#10B981;">
                        <div class="kpi-label">Perfect Matches</div>
                        <div class="kpi-value" style="color:#10B981;">{matched:,}</div>
                    </div>
                    <div class="kpi-card orange">
                        <div class="kpi-label">Discrepancies</div>
                        <div class="kpi-value" style="color:#F97316;">{unmatched:,}</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)

            st.markdown("---")

            # ── Filter & Search ──────────────────────────────────────
            present_statuses = sorted(result_df["Match_Status"].dropna().unique().tolist())
            ordered_statuses = [s for s in ALL_STATUSES if s in present_statuses]
            ordered_statuses += [s for s in present_statuses if s not in ordered_statuses]

            st.markdown("### 🔍 Filter & Search")
            fs_left, fs_right = st.columns(2)

            with fs_left:
                st.markdown('<div class="side-header-2b">📘 GSTR-2B — Filter & Search</div>', unsafe_allow_html=True)
                twoB_status = st.multiselect("Filter by Status", options=ordered_statuses, key="filter_2b")
                sb2, sv2 = st.columns([1, 2])
                twoB_search_by = sb2.selectbox("Search by", options=["— None —", "GSTIN", "PAN"], key="search_by_2b")
                twoB_search_val = sv2.text_input("2B search value", key="search_val_2b", label_visibility="collapsed")

            with fs_right:
                st.markdown('<div class="side-header-pur">📙 Purchase Register — Filter & Search</div>', unsafe_allow_html=True)
                pur_status = st.multiselect("Filter by Status", options=ordered_statuses, key="filter_pur")
                sbp, svp = st.columns([1, 2])
                pur_search_by = sbp.selectbox("Search by", options=["— None —", "GSTIN", "PAN"], key="search_by_pur")
                pur_search_val = svp.text_input("Books search value", key="search_val_pur", label_visibility="collapsed")

            twoB_has_input  = bool(twoB_status) or (twoB_search_by != "— None —" and bool(twoB_search_val.strip()))
            books_has_input = bool(pur_status)  or (pur_search_by  != "— None —" and bool(pur_search_val.strip()))

            work = result_df.copy()
            work["_PAN_2B"]  = extract_pan(work.get("Supplier GSTIN",         pd.Series("", index=work.index)))
            work["_PAN_PUR"] = extract_pan(work.get("Vendor/Customer GSTIN", pd.Series("", index=work.index)))

            # ── Data Population for Panels ───────────────────────────
            if twoB_has_input:
                m2 = pd.Series(True, index=work.index)
                if twoB_status: m2 &= work["Match_Status"].isin(twoB_status)
                if twoB_search_by != "— None —" and twoB_search_val.strip():
                    q = twoB_search_val.strip().upper()
                    col = work.get("Supplier GSTIN" if twoB_search_by=="GSTIN" else "_PAN_2B", pd.Series("", index=work.index))
                    m2 &= col.fillna("").astype(str).str.upper().str.contains(q, regex=False)
                open_2b_rows = work[m2]
            else: open_2b_rows = pd.DataFrame(columns=work.columns)

            if books_has_input:
                m_pur = pd.Series(True, index=work.index)
                if pur_status: m_pur &= work["Match_Status"].isin(pur_status)
                if pur_search_by != "— None —" and pur_search_val.strip():
                    q = pur_search_val.strip().upper()
                    col = work.get("Vendor/Customer GSTIN" if pur_search_by=="GSTIN" else "_PAN_PUR", pd.Series("", index=work.index))
                    m_pur &= col.fillna("").astype(str).str.upper().str.contains(q, regex=False)
                open_books_rows = work[m_pur]
            else: open_books_rows = pd.DataFrame(columns=work.columns)

            # ── Manual Match Maker Logic ─────────────────────────────
            if not open_2b_rows.empty or not open_books_rows.empty:
                st.markdown("---")
                st.markdown("### 🤝 Manual Match Maker")
                
                # REVERT SECTION
                if st.session_state["manual_matches"]:
                    with st.expander("🕒 Recent Manual Matches (Click to Undo)", expanded=False):
                        for i, (idx_2b, idx_books) in enumerate(st.session_state["manual_matches"]):
                            doc_num = result_df.at[idx_2b, 'Document Number']
                            r1, r2 = st.columns([0.85, 0.15])
                            r1.write(f"Match #{i+1}: 2B Doc **{gst["doc_norm"]}** matched with Books index **{pur["doc_norm"]}**")
                            if r2.button("Undo", key=f"undo_{i}_{idx_2b}"):
                                live_df = st.session_state["result_df"]
                                live_df.at[idx_2b, "Match_Status"] = reco_logic.MATCH_OPEN_2B
                                live_df.at[idx_books, "Match_Status"] = reco_logic.MATCH_OPEN_BOOKS
                                for c in PUR_COPY_COLS:
                                    if c in live_df.columns: live_df.at[idx_2b, c] = None
                                st.session_state["manual_matches"].pop(i)
                                st.session_state["result_df"] = live_df
                                st.rerun()

                st.markdown("<small style='color:#64748B;'>Select one row from each side to reconcile.</small>", unsafe_allow_html=True)

                mm_left, mm_right = st.columns(2)
                sel_2b_idx = None
                sel_books_idx = None

                with mm_left:
                    st.markdown('<div class="side-header-2b">📘 Open in 2B</div>', unsafe_allow_html=True)
                    for df_idx, row in open_2b_rows.iterrows():
                        if row["Match_Status"] == "Manual Match (Consumed)": continue
                        chk_col, info_col = st.columns([0.1, 0.9])
                        if chk_col.checkbox("", key=f"chk_2b_{df_idx}"): sel_2b_idx = df_idx
                        info_col.markdown(f"<div class='mm-row-card'><b>GSTIN:</b> {row.get('Supplier GSTIN','—')}<br><b>Doc:</b> {row.get('Document Number','—')}<br><b>Return Period:</b> {row.get('Return Period','—')}<br><b>IGST:</b> {fmt_amt(row.get('IGST Amount_2B',0))}<br><b>CGST:</b> {fmt_amt(row.get('CGST Amount_2B',0))}<br><b>SGST:</b> {fmt_amt(row.get('SGST Amount_2B',0))}</div>", unsafe_allow_html=True)

                with mm_right:
                    st.markdown('<div class="side-header-pur">📙 Open in Books</div>', unsafe_allow_html=True)
                    for df_idx, row in open_books_rows.iterrows():
                        if row["Match_Status"] == "Manual Match (Consumed)": continue
                        chk_col, info_col = st.columns([0.1, 0.9])
                        if chk_col.checkbox("", key=f"chk_bk_{df_idx}"): sel_books_idx = df_idx
                        info_col.markdown(f"<div class='mm-row-card'><b>GSTIN:</b> {row.get('Vendor/Customer GSTIN','—')}<br><b>Doc:</b> {row.get('Reference Document No.','—')}<br><b>Document Date:</b> {row.get('Document Date_PUR','—')}<br><b>IGST:</b> {fmt_amt(row.get('IGST Amount_PUR',0))}<br><b>CGST:</b> {fmt_amt(row.get('CGST Amount_PUR',0))}<br><b>SGST:</b> {fmt_amt(row.get('SGST Amount_PUR',0))}</div>", unsafe_allow_html=True)

                _, ok_col, _ = st.columns([1, 2, 1])
                if ok_col.button("✅ Confirm Match", use_container_width=True):
                    if sel_2b_idx is not None and sel_books_idx is not None:
                        live_df = st.session_state["result_df"]
                        for col in PUR_COPY_COLS:
                            if col in live_df.columns: live_df.at[sel_2b_idx, col] = live_df.at[sel_books_idx, col]
                        
                        for tax in ["IGST", "CGST", "SGST"]:
                            p_col, b_col, d_col = f"{tax} Amount_PUR", f"{tax} Amount_2B", f"{tax} Diff"
                            if p_col in live_df.columns and b_col in live_df.columns:
                                live_df.at[sel_2b_idx, d_col] = pd.to_numeric(live_df.at[sel_2b_idx, p_col], errors="coerce") - pd.to_numeric(live_df.at[sel_2b_idx, b_col], errors="coerce")
                        
                        live_df.at[sel_2b_idx, "Match_Status"] = "Manual Match"
                        live_df.at[sel_books_idx, "Match_Status"] = "Manual Match (Consumed)"
                        st.session_state["result_df"] = live_df
                        st.session_state["manual_matches"].append((sel_2b_idx, sel_books_idx))
                        st.rerun()
                    else: st.warning("Please select one row from each side.")

            # ── Detailed Ledger ──────────────────────────────────────
            st.markdown("---")
            st.markdown(f"### 📋 Detailed Ledger <span style='font-size:0.9rem;color:#64748B;'>({len(result_df):,} rows)</span>", unsafe_allow_html=True)
            st.dataframe(result_df.style.map(style_status, subset=["Match_Status"]), use_container_width=True, height=400)

            # ── Export ───────────────────────────────────────────────
            export_df = st.session_state["result_df"].copy()
            export_df = export_df[export_df["Match_Status"] != "Manual Match (Consumed)"]
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                export_df.to_excel(writer, index=False)
            st.download_button("📥 DOWNLOAD FINAL REPORT", data=output.getvalue(), file_name="GST_Report.xlsx", use_container_width=True)

    except Exception as e: st.error(f"🚨 Error: {str(e)}")
else:
    st.markdown('<div class="empty-state animate-fade"><h2>Awaiting Data Injection 🚀</h2><p>Upload files to start.</p></div>', unsafe_allow_html=True)
