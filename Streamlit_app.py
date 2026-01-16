import streamlit as st
import pandas as pd
import io
import reconciliation_logic as reco_logic

# --------------------------------------------------
# 1. Page Configuration
# --------------------------------------------------
st.set_page_config(
    page_title="GST Reco Pro",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --------------------------------------------------
# 2. Custom CSS
# --------------------------------------------------
st.markdown(
    """
    <style>
    .main {
        background-color: #f8f9fa;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3em;
        background-color: #ff4b4b;
        color: white;
        font-weight: bold;
    }
    .upload-card {
        padding: 20px;
        border-radius: 10px;
        background-color: #ffffff;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --------------------------------------------------
# Sidebar
# --------------------------------------------------
with st.sidebar:
    st.image(
        "https://www.gstatic.com/images/branding/product/2x/forms_96dp.png",
        width=80
    )
    st.title("Settings")
    match_threshold = st.slider("Matching Sensitivity (%)", 80, 100, 95)
    st.divider()
    st.markdown("### Support")
    st.info("Contact Finance IT for column mapping issues.")

# --------------------------------------------------
# Header
# --------------------------------------------------
header_col1, header_col2 = st.columns([3, 1])

with header_col1:
    st.title("⚖️ GST 2B vs Books Reconciliation")
    st.markdown("Automate your Input Tax Credit (ITC) verification seamlessly.")

# --------------------------------------------------
# File Upload Section
# --------------------------------------------------
st.subheader("1. Data Input")
col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="upload-card">', unsafe_allow_html=True)
    st.subheader("📑 GSTR-2B (Portal)")
    gst_file = st.file_uploader(
        "Upload 2B Excel",
        type=["xlsx"],
        key="2b",
        help="Standard GST portal export"
    )
    st.markdown("</div>", unsafe_allow_html=True)

with col2:
    st.markdown('<div class="upload-card">', unsafe_allow_html=True)
    st.subheader("📚 Purchase Register")
    pur_file = st.file_uploader(
        "Upload Books Excel",
        type=["xlsx"],
        key="books",
        help="Internal ERP / Tally export"
    )
    st.markdown("</div>", unsafe_allow_html=True)

st.divider()

# --------------------------------------------------
# Processing
# --------------------------------------------------
if gst_file and pur_file:

    df_2b = pd.read_excel(gst_file)
    df_books = pd.read_excel(pur_file)

    st.subheader("2. Run Analysis")

    if st.button("🚀 Execute Reconciliation"):
        with st.spinner("🔄 Matching records and calculating variances..."):
            try:
                # ✅ CORRECT FUNCTION CALL
                #result_df = reco_logic.process_reco(df_2b, df_books)
                 result_df = reco_logic.process_reco(
                             df_2b,
                             df_books,
                             threshold=match_threshold
                             )

                # --------------------------------------------------
                # Summary Metrics
                # --------------------------------------------------
                st.subheader("📊 Reconciliation Summary")
                m1, m2, m3, m4 = st.columns(4)

                m1.metric("Total Books Records", len(df_books))
                m2.metric("Total 2B Records", len(df_2b))

                if "Match_Status" in result_df.columns:
                    exact_cnt = (result_df["Match_Status"] == "Exact Match").sum()
                    fuzzy_cnt = (result_df["Match_Status"] == "Fuzzy Match").sum()
                    open_cnt = len(result_df) - exact_cnt - fuzzy_cnt

                    m3.metric("Matched (Exact + Fuzzy)", exact_cnt + fuzzy_cnt)
                    m4.metric("Action Required", open_cnt)

                # --------------------------------------------------
                # Data Preview & Export
                # --------------------------------------------------
                st.divider()
                tab1, tab2 = st.tabs(["📋 Result Preview", "📥 Export Data"])

                with tab1:
                    st.dataframe(
                        result_df,
                        use_container_width=True,
                        height=450
                    )

                with tab2:
                    output = io.BytesIO()
                    with pd.ExcelWriter(
                        output,
                        engine="xlsxwriter"
                    ) as writer:
                        result_df.to_excel(
                            writer,
                            index=False,
                            sheet_name="GST_Reco_Result"
                        )

                    st.success("Reconciliation generated successfully!")
                    st.download_button(
                        label="Download Excel Report",
                        data=output.getvalue(),
                        file_name="GST_Reconciliation_Report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

            except Exception as e:
                st.error(f"❌ Error during processing: {e}")

else:
    st.warning("Please upload both Excel files to activate the reconciliation engine.")

    with st.expander("📌 File Preparation Guide"):
        st.write(
            """
            **Books File Must Contain:**
            - GSTIN Of Vendor/Customer
            - Reference Document No.
            - FI Document Number
            - IGST / CGST / SGST Amount
            - Invoice Value

            **2B File Must Contain:**
            - Supplier GSTIN
            - Document Number
            - Document Date
            - IGST / CGST / SGST Amount
            - Invoice Value
            """
        )

# --------------------------------------------------
# Footer
# --------------------------------------------------
st.markdown("---")
st.caption("GST Reconciliation Tool v2.0 | Built with Streamlit")

