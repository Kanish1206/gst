import streamlit as st
import pandas as pd
import io
import reconciliation_logic as reco_logic

# 1. Page Configuration
st.set_page_config(
    page_title="GST Reco Pro",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. Custom CSS for a cleaner look
st.markdown("""
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
    }
    .upload-card {
        padding: 20px;
        border-radius: 10px;
        background-color: #ffffff;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    </style>
    """, unsafe_allow_index=True)

# --- Sidebar ---
with st.sidebar:
    st.image("https://www.gstatic.com/images/branding/product/2x/forms_96dp.png", width=80)
    st.title("Settings")
    match_threshold = st.slider("Matching Sensitivity (%)", 80, 100, 95)
    st.divider()
    st.markdown("### Support")
    st.info("Contact Finance IT for column mapping issues.")

# --- Header Section ---
header_col1, header_col2 = st.columns([3, 1])
with header_col1:
    st.title("⚖️ GST 2B vs Books Reconciliation")
    st.markdown("Automate your Input Tax Credit (ITC) verification seamlessly.")

# --- File Upload Section ---
st.subheader("1. Data Input")
col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="upload-card">', unsafe_allow_index=True)
    st.subheader("📑 GSTR-2B (Portal)")
    gst_file = st.file_uploader("Upload 2B Excel", type=['xlsx'], key="2b", help="Standard portal export")
    st.markdown('</div>', unsafe_allow_index=True)

with col2:
    st.markdown('<div class="upload-card">', unsafe_allow_index=True)
    st.subheader("📚 Purchase Register")
    pur_file = st.file_uploader("Upload Books Excel", type=['xlsx'], key="books", help="Internal ERP/Tally export")
    st.markdown('</div>', unsafe_allow_index=True)

st.divider()

# --- Processing & Results ---
if pur_file and gst_file:
    # Read files
    df_books = pd.read_excel(pur_file)
    df_2b = pd.read_excel(gst_file)

    st.subheader("2. Run Analysis")
    if st.button("🚀 Execute Reconciliation"):
        with st.spinner("🔄 Matching records and calculating variances..."):
            try:
                # Run Logic
                result_df = reco_logic.merged_diagnose(df_books, df_2b)

                # --- 3. Summary Metrics ---
                st.subheader("📊 Reconciliation Summary")
                m1, m2, m3, m4 = st.columns(4)
                
                # These are example calculations, adapt based on your logic's output
                m1.metric("Total Books Count", len(df_books))
                m2.metric("Total 2B Count", len(df_2b))
                
                if 'Status' in result_df.columns:
                    matched_count = len(result_df[result_df['Status'] == 'Matched'])
                    m3.metric("Fully Matched", matched_count, delta=f"{matched_count - len(df_books)}")
                
                m4.metric("Action Required", len(result_df[result_df.index.isin(result_df.dropna().index) == False]))

                # --- 4. Data Preview & Download ---
                st.divider()
                tab1, tab2 = st.tabs(["📋 Result Preview", "📥 Export Data"])
                
                with tab1:
                    st.dataframe(result_df, use_container_width=True, height=400)
                
                with tab2:
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        result_df.to_excel(writer, index=False, sheet_name='Reco_Result')
                    
                    st.success("Reconciliation generated successfully!")
                    st.download_button(
                        label="Click to Download Excel Report",
                        data=output.getvalue(),
                        file_name="GST_Reconciliation_Report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

            except Exception as e:
                st.error(f"❌ Error during processing: {str(e)}")
else:
    # Help Message when no files are uploaded
    st.warning("Please upload both Excel files to activate the reconciliation engine.")
    
    with st.expander("📌 File Preparation Guide"):
        st.write("""
        To ensure high accuracy, please ensure your files meet these criteria:
        * **Books:** Must include `GSTIN`, `Invoice Number`, `Date`, and `Taxable Value`.
        * **2B:** Use the official Excel download from the GST portal without modifying the structure.
        * **Format:** Files must be in `.xlsx` or `.xls` format.
        """)

# Footer
st.markdown("---")
st.caption("GST Reconciliation Tool v2.0 | Built with Streamlit")