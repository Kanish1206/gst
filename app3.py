import streamlit as st
import polars as pl
import io
import zipfile
import traceback
from sales_processor import SalesProcessor

# --------------------------------------------------
# PAGE CONFIG (Must be the first Streamlit command)
# --------------------------------------------------
st.set_page_config(
    page_title="Sales Engine",
    page_icon="📊",
    layout="wide"
)

# --------------------------------------------------
# SESSION STATE
# --------------------------------------------------
if "raw_data" not in st.session_state:
    st.session_state.raw_data = None
if "pivot_summary" not in st.session_state:
    st.session_state.pivot_summary = None
if "zip_buffer" not in st.session_state:
    st.session_state.zip_buffer = None

# --------------------------------------------------
# UI SETUP
# --------------------------------------------------
st.markdown("""
<style>
.block-container { padding-top: 2rem; }
h1, h2, h3 { font-weight: 600; }
</style>
""", unsafe_allow_html=True)

st.title("🚀 Sales & Master Processing Engine")
st.divider()

# --------------------------------------------------
# FILE UPLOAD
# --------------------------------------------------
st.subheader("📂 Upload Files")
c1, c2 = st.columns(2)
with c1:
    s_file = st.file_uploader("Upload Sales (.xlsb / .xlsx)", type=["xlsb", "xlsx"])
with c2:
    m_file = st.file_uploader("Upload Master (.xlsx)", type=["xlsx"])

# --------------------------------------------------
# PROCESSING
# --------------------------------------------------
if st.button("⚙️ Generate Report", type="primary", use_container_width=True):
    if not s_file or not m_file:
        st.warning("⚠️ Please upload BOTH the Sales and Master files before generating the report.")
    else:
        with st.spinner("Running data pipeline..."):
            st.session_state.zip_buffer = None # Reset buffer for new runs
            
            try:
                # Run the backend processor
                engine = SalesProcessor(s_file, m_file)
                st.session_state.raw_data, st.session_state.pivot_summary = engine.process()
                st.success("✅ Processing completed successfully!")
                
            except Exception as e:
                st.error(f"❌ An error occurred during processing: {str(e)}")
                # Optional: Add an expander with the traceback for debugging
                with st.expander("View detailed error log"):
                    st.code(traceback.format_exc())

# --------------------------------------------------
# RESULTS & EXPORT
# --------------------------------------------------
if st.session_state.raw_data is not None and st.session_state.pivot_summary is not None:
    raw_data = st.session_state.raw_data
    pivot_summary = st.session_state.pivot_summary

    # Metrics
    m1, m2 = st.columns(2)
    m1.metric("Total Rows Processed", f"{raw_data.height:,}")
    m2.metric("Pivot Groups Generated", f"{pivot_summary.height:,}")

    # Preview Tabs
    t1, t2 = st.tabs(["📋 Detailed Data (Preview)", "📊 Pivot Summary"])
    with t1:
        st.caption("Showing up to the first 1,000 rows for performance.")
        # Convert only the head to pandas to keep the UI snappy
        st.dataframe(raw_data.head(1000).to_pandas(), use_container_width=True)
    with t2:
        st.dataframe(pivot_summary.to_pandas(), use_container_width=True)

    st.divider()
    st.subheader("⬇️ Export Final Report")

    # --------------------------------------------------
    # ⚡ EXPORT LOGIC (CSV ZIP - Fastest Method)
    # --------------------------------------------------
    if st.session_state.zip_buffer is None:
        if st.button("📦 Prepare CSV Download"):
            with st.spinner("⚡ Zipping CSV files..."):
                
                zip_buffer = io.BytesIO()
                
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    # 1. Write Detailed Data to CSV inside ZIP
                    csv_detailed = raw_data.write_csv()
                    zf.writestr("Detailed_Sales.csv", csv_detailed)
                    
                    # 2. Write Pivot Summary to CSV inside ZIP
                    csv_pivot = pivot_summary.write_csv()
                    zf.writestr("Pivot_Summary.csv", csv_pivot)

                # Finalize buffer into session state
                st.session_state.zip_buffer = zip_buffer.getvalue()
                st.rerun()

    else:
        st.success("✅ ZIP File Ready! (Contains both Detailed Sales and Pivot Summary CSVs)")
        c_down, c_reset = st.columns([1, 4])
        
        with c_down:
            st.download_button(
                label="📥 Download ZIP",
                data=st.session_state.zip_buffer,
                file_name="Sales_Reports.zip",
                mime="application/zip",
                use_container_width=True
            )
            
        with c_reset:
            if st.button("🔄 Reset Download State"):
                st.session_state.zip_buffer = None
                st.rerun()
