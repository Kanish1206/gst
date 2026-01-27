import streamlit as st
import polars as pl
import io
import zipfile
from sales_processor import SalesProcessor

# --------------------------------------------------
# PAGE CONFIG
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
        st.error("❌ Upload BOTH Sales and Master files.")
    else:
        with st.spinner("Running data pipeline..."):
            st.session_state.zip_buffer = None # Reset buffer
            engine = SalesProcessor(s_file, m_file)
            st.session_state.raw_data, st.session_state.pivot_summary = engine.process()
        st.success("✅ Processing completed.")

# --------------------------------------------------
# RESULTS & EXPORT
# --------------------------------------------------
if st.session_state.raw_data is not None:
    raw_data = st.session_state.raw_data
    pivot_summary = st.session_state.pivot_summary

    # Metrics
    m1, m2 = st.columns(2)
    m1.metric("Total Rows", raw_data.height)
    m2.metric("Pivot Groups", pivot_summary.height)

    # Preview Tabs
    t1, t2 = st.tabs(["📋 Detailed Data (Preview)", "📊 Pivot Summary"])
    with t1:
        st.caption("Showing first 1,000 rows.")
        st.dataframe(raw_data.head(1000).to_pandas(), use_container_width=True)
    with t2:
        st.dataframe(pivot_summary.to_pandas(), use_container_width=True)

    st.divider()
    st.subheader("⬇️ Export Final Report")

    # --------------------------------------------------
    # ⚡ EXPORT LOGIC (CSV ZIP - Fastest Method)
    # --------------------------------------------------
    if st.session_state.zip_buffer is None:
        if st.button("Prepare CSV Download"):
            with st.spinner("⚡ Zipping CSV files..."):
                
                zip_buffer = io.BytesIO()
                
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    # 1. Write Detailed Data to CSV inside ZIP
                    # We preserve "0090019" by forcing quotes if needed, 
                    # but standard CSV readers usually respect the raw text.
                    csv_detailed = raw_data.write_csv()
                    zf.writestr("Detailed_Sales.csv", csv_detailed)
                    
                    # 2. Write Pivot Summary to CSV inside ZIP
                    csv_pivot = pivot_summary.write_csv()
                    zf.writestr("Pivot_Summary.csv", csv_pivot)

                # Finalize buffer
                st.session_state.zip_buffer = zip_buffer.getvalue()
                st.rerun()

    else:
        st.success("✅ ZIP File Ready! (Contains 2 CSVs)")
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
            if st.button("🔄 Reset"):
                st.session_state.zip_buffer = None
                st.rerun()