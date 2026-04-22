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

# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
if "result_df"      not in st.session_state: st.session_state["result_df"]      = None
if "manual_matches" not in st.session_state: st.session_state["manual_matches"] = []
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

        st.markdown("### 🔍 Filter & Search")
            fs_left, fs_right = st.columns(2)

            # ── Left: GSTR-2B ────────────────────────────────────────
            with fs_left:
                st.markdown('<div class="side-header-2b">📘 GSTR-2B — Filter & Search</div>', unsafe_allow_html=True)

                twoB_status = st.multiselect(
                    "Filter by Status",
                    options=ordered_statuses,
                    default=[],
                    placeholder="Select one or more statuses…",
                    key="filter_2b"
                )
                sb2, sv2 = st.columns([1, 2])
                with sb2:
                    twoB_search_by = st.selectbox(
                        "Search by",
                        options=["— None —", "GSTIN", "PAN"],
                        key="search_by_2b"
                    )
                with sv2:
                    twoB_search_val = st.text_input(
                        "2B search value",
                        placeholder="Type GSTIN or PAN…",
                        key="search_val_2b",
                        label_visibility="collapsed"
                    )

            # ── Right: Purchase Register ─────────────────────────────
            with fs_right:
                st.markdown('<div class="side-header-pur">📙 Purchase Register — Filter & Search</div>', unsafe_allow_html=True)

                pur_status = st.multiselect(
                    "Filter by Status",
                    options=ordered_statuses,
                    default=[],
                    placeholder="Select one or more statuses…",
                    key="filter_pur"
                )
                sbp, svp = st.columns([1, 2])
                with sbp:
                    pur_search_by = st.selectbox(
                        "Search by",
                        options=["— None —", "GSTIN", "PAN"],
                        key="search_by_pur"
                    )
                with svp:
                    pur_search_val = st.text_input(
                        "Books search value",
                        placeholder="Type GSTIN or PAN…",
                        key="search_val_pur",
                        label_visibility="collapsed"
                    )

            # ── Check if any panel has input ─────────────────────────
            twoB_has_input  = bool(twoB_status) or (twoB_search_by != "— None —" and twoB_search_val.strip())
            books_has_input = bool(pur_status)  or (pur_search_by  != "— None —" and pur_search_val.strip())
            any_input       = twoB_has_input or books_has_input

            # ── Build display dataframe ──────────────────────────────
            work = result_df.copy()
            work["_PAN_2B"]  = extract_pan(work.get("Supplier GSTIN",        pd.Series(dtype=str)))
            work["_PAN_PUR"] = extract_pan(work.get("Vendor/Customer GSTIN", pd.Series(dtype=str)))

            if any_input:
                masks = []

                if twoB_has_input:
                    m = pd.Series(True, index=work.index)
                    if twoB_status:
                        m &= work["Match_Status"].isin(twoB_status)
                    if twoB_search_by != "— None —" and twoB_search_val.strip():
                        q = twoB_search_val.strip().upper()
                        if twoB_search_by == "GSTIN":
                            col_gstin = work.get("Supplier GSTIN", pd.Series("", index=work.index))
                            m &= col_gstin.fillna("").astype(str).str.upper().str.contains(q, regex=False)
                        else:
                            m &= work["_PAN_2B"].str.contains(q, regex=False)
                    masks.append(m)

                if books_has_input:
                    m = pd.Series(True, index=work.index)
                    if pur_status:
                        m &= work["Match_Status"].isin(pur_status)
                    if pur_search_by != "— None —" and pur_search_val.strip():
                        q = pur_search_val.strip().upper()
                        if pur_search_by == "GSTIN":
                            col_gstin = work.get("Vendor/Customer GSTIN", pd.Series("", index=work.index))
                            m &= col_gstin.fillna("").astype(str).str.upper().str.contains(q, regex=False)
                        else:
                            m &= work["_PAN_PUR"].str.contains(q, regex=False)
                    masks.append(m)

                # Union of both panels' masks
                combined = masks[0]
                for m in masks[1:]:
                    combined = combined | m

                filtered = work[combined]
            else:
                # ← Show nothing until user inputs something
                filtered = pd.DataFrame(columns=work.columns)

            display_df = filtered.drop(columns=["_PAN_2B", "_PAN_PUR"], errors="ignore")

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

        open_2b_rows    = result_df[result_df["Match_Status"] == reco_logic.MATCH_OPEN_2B]
            open_books_rows = result_df[result_df["Match_Status"] == reco_logic.MATCH_OPEN_BOOKS]

            if not open_2b_rows.empty or not open_books_rows.empty:
                st.markdown("---")
                st.markdown("### 🤝 Manual Match Maker")
                st.markdown(
                    "<small style='color:#64748B;'>Tick <b>one row</b> on each side, then click "
                    "<b>✅ Confirm Match</b>.</small>",
                    unsafe_allow_html=True
                )

                mm_left, mm_right = st.columns(2)
                sel_2b_idx    = None
                sel_books_idx = None

                # ── 2B side ──────────────────────────────────────────
                with mm_left:
                    st.markdown('<div class="side-header-2b">📘 Open in 2B</div>', unsafe_allow_html=True)

                    if open_2b_rows.empty:
                        st.info("No 'Open in 2B' rows.")
                    else:
                        for df_idx, row in open_2b_rows.iterrows():
                            gstin = str(row.get("Supplier GSTIN",  "—"))
                            doc   = str(row.get("Document Number", "—"))
                            igst  = fmt_amt(row.get("IGST Amount_2B", 0))
                            cgst  = fmt_amt(row.get("CGST Amount_2B", 0))
                            sgst  = fmt_amt(row.get("SGST Amount_2B", 0))

                            chk_col, info_col = st.columns([0.07, 0.93])
                            with chk_col:
                                checked = st.checkbox(
                                    "", key=f"chk_2b_{df_idx}",
                                    label_visibility="collapsed"
                                )
                            with info_col:
                                st.markdown(
                                    f"<div class='mm-row-card'>"
                                    f"<b>GSTIN :</b> {gstin}<br>"
                                    f"<b>Doc No:</b> {doc}<br>"
                                    f"<b>IGST:</b> {igst} &nbsp;|&nbsp; "
                                    f"<b>CGST:</b> {cgst} &nbsp;|&nbsp; "
                                    f"<b>SGST:</b> {sgst}"
                                    f"</div>",
                                    unsafe_allow_html=True
                                )
                            if checked:
                                sel_2b_idx = df_idx

                # ── Books side ───────────────────────────────────────
                with mm_right:
                    st.markdown('<div class="side-header-pur">📙 Open in Books</div>', unsafe_allow_html=True)

                    if open_books_rows.empty:
                        st.info("No 'Open in Books' rows.")
                    else:
                        for df_idx, row in open_books_rows.iterrows():
                            gstin = str(row.get("Vendor/Customer GSTIN",   "—"))
                            doc   = str(row.get("Reference Document No.", "—"))
                            igst  = fmt_amt(row.get("IGST Amount_PUR", 0))
                            cgst  = fmt_amt(row.get("CGST Amount_PUR", 0))
                            sgst  = fmt_amt(row.get("SGST Amount_PUR", 0))

                            chk_col, info_col = st.columns([0.07, 0.93])
                            with chk_col:
                                checked = st.checkbox(
                                    "", key=f"chk_bk_{df_idx}",
                                    label_visibility="collapsed"
                                )
                            with info_col:
                                st.markdown(
                                    f"<div class='mm-row-card'>"
                                    f"<b>GSTIN :</b> {gstin}<br>"
                                    f"<b>Doc No:</b> {doc}<br>"
                                    f"<b>IGST:</b> {igst} &nbsp;|&nbsp; "
                                    f"<b>CGST:</b> {cgst} &nbsp;|&nbsp; "
                                    f"<b>SGST:</b> {sgst}"
                                    f"</div>",
                                    unsafe_allow_html=True
                                )
                            if checked:
                                sel_books_idx = df_idx

                # ── Confirm button ────────────────────────────────────
                _, ok_col, _ = st.columns([1, 2, 1])
                with ok_col:
                    confirm_btn = st.button(
                        "✅ Confirm Match", use_container_width=True, key="confirm_manual"
                    )

                if confirm_btn:
                    if sel_2b_idx is None or sel_books_idx is None:
                        st.warning("⚠️ Please select exactly one row from each side before confirming.")
                    else:
                        live_df = st.session_state["result_df"]

                        # Copy purchase columns into 2B row
                        pur_copy_cols = [
                            c for c in live_df.columns
                            if c.endswith("_PUR") or c in [
                                "Reference Document No.", "FI Document Number",
                                "Vendor/Customer Name",  "Vendor/Customer GSTIN"
                            ]
                        ]
                        for col in pur_copy_cols:
                            if col in live_df.columns:
                                live_df.at[sel_2b_idx, col] = live_df.at[sel_books_idx, col]

                        # Recompute diffs
                        for tax in ["IGST", "CGST", "SGST"]:
                            p_col = f"{tax} Amount_PUR"
                            b_col = f"{tax} Amount_2B"
                            d_col = f"{tax} Diff"
                            if p_col in live_df.columns and b_col in live_df.columns:
                                live_df.at[sel_2b_idx, d_col] = (
                                    pd.to_numeric(live_df.at[sel_2b_idx, p_col], errors="coerce") -
                                    pd.to_numeric(live_df.at[sel_2b_idx, b_col], errors="coerce")
                                )

                        live_df.at[sel_2b_idx,    "Match_Status"] = "Manual Match"
                        live_df.at[sel_books_idx, "Match_Status"] = "Manual Match (Consumed)"

                        st.session_state["result_df"] = live_df
                        st.session_state["manual_matches"].append((sel_2b_idx, sel_books_idx))
                        st.success("✅ Rows matched and marked as **Manual Match**!")
                        st.rerun()

            # ════════════════════════════════════════════════════════
            #  DETAILED LEDGER
            # ════════════════════════════════════════════════════════
            st.markdown("---")
            st.markdown(
                f"### 📋 Detailed Ledger &nbsp;"
                f"<span style='font-size:0.9rem;color:#64748B;'>({len(display_df):,} rows shown)</span>",
                unsafe_allow_html=True
            )

            if not any_input:
                # Show hint — nothing loaded until user types/selects
                st.markdown("""
                    <div class="ledger-hint">
                        <h3 style="margin:0 0 8px 0;">📂 Use the filters above to explore your data</h3>
                        <p style="margin:0;">
                            Select a <b>Status</b> or type a <b>GSTIN / PAN</b> in either the
                            <b>GSTR-2B</b> or <b>Purchase Register</b> panel to load records here.
                        </p>
                    </div>
                """, unsafe_allow_html=True)
            elif display_df.empty:
                st.info("🔍 No records match the current filter / search criteria.")
            else:
                st.dataframe(
                    display_df.style.map(style_status, subset=["Match_Status"]),
                    use_container_width=True,
                    height=420
                )

            # ── Export ───────────────────────────────────────────────
            st.markdown("<br>", unsafe_allow_html=True)
            export_df = st.session_state["result_df"].copy()
            export_df = export_df[~export_df["Match_Status"].isin(["Manual Match (Consumed)"])]

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                export_df.to_excel(writer, index=False)

            st.download_button(
                label="📥 DOWNLOAD FINAL REPORT (EXCEL)",
                data=output.getvalue(),
                file_name="GST_Reco_Smart_Report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

            st.markdown('</div>', unsafe_allow_html=True)

    except Exception as e:
        st.error(f"🚨 Process Error: {str(e)}")

else:
    st.markdown("""
        <div class="empty-state animate-fade">
            <h2 style="margin-bottom:10px;">Awaiting Data Injection 🚀</h2>
            <p>Upload your <b>GSTR-2B</b> and <b>Purchase Register</b> files above to trigger the reconciliation engine.</p>
        </div>
    """, unsafe_allow_html=True)
else:
    st.info("Upload files to start")
