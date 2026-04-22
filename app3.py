import streamlit as st
import pandas as pd
import io
import reconciliation_logic as reco_logic

st.set_page_config(page_title="GST Reco Pro | Analytics", layout="wide")

col1, col2 = st.columns(2)

with col1:
    gst_file = st.file_uploader("GSTR-2B", type=["xlsx"])

with col2:
    pur_file = st.file_uploader("Purchase", type=["xlsx"])

if gst_file and pur_file:

    df_2b = pd.read_excel(gst_file)
    df_books = pd.read_excel(pur_file)

    colA, colB = st.columns(2)

    with colA:
        run_btn = st.button("🚀 INITIATE PROCESS")

    with colB:
        reuse_btn = st.button("♻️ USE PREVIOUS OPEN MATCHES")

    if run_btn or reuse_btn:

        result_df = reco_logic.process_reco(df_2b, df_books)

        # ✅ NEW FEATURE
        if reuse_btn:
            result_df = reco_logic.apply_previous_matches(result_df)

        st.dataframe(result_df)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            result_df.to_excel(writer, index=False)

        st.download_button(
            "Download",
            data=output.getvalue(),
            file_name="report.xlsx"
        )
