import streamlit as st
import pandas as pd
import io
import reconciliation_logic as reco_logic
import os

# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="GST Reco Pro | Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---------------- CSS ----------------
st.markdown("""
<style>
.stApp { background-color: #F0F4F8; }

.hero-header {
    background: linear-gradient(135deg, #0F172A 0%, #1E3A8A 50%, #F97316 100%);
    padding: 2.5rem 2rem;
    border-radius: 16px;
    color: white;
    margin-bottom: 2rem;
}

.kpi-container {
    display: flex;
    gap: 1rem;
    margin-bottom: 2rem;
}

.kpi-card {
    background: white;
    padding: 1.5rem;
    border-radius: 12px;
    flex: 1;
    text-align: center;
    border-bottom: 4px solid #3B82F6;
}

.kpi-value {
    font-size: 2rem;
    font-weight: 800;
}

.kpi-label {
    font-size: 0.9rem;
    color: #64748B;
}
</style>
""", unsafe_allow_html=True)

# ---------------- HEADER ----------------
st.markdown("""
<div class="hero-header">
    <h1>⚡ GST Intelligence Hub</h1>
    <p>Automated GSTR-2B vs Books Reconciliation</p>
</div>
""", unsafe_allow_html=True)

# ---------------- CONTROLS ----------------
auto_mode = st.checkbox("🧠 Enable Auto Learning", value=True)

col1, col2 = st.columns(2)

with col1:
    if st.button("🧹 Clear Learned Data"):
        if os.path.exists("reco_storage.db"):
            os.remove("reco_storage.db")
            st.success("Memory Cleared")

with col2:
    if st.button("📂 Show Learned Data"):
        df_saved = reco_logic.load_open_items()
        if not df_saved.empty:
            st.dataframe(df_saved)
        else:
            st.info("No learned data found")

# ---------------- FILE UPLOAD ----------------
col1, col2 = st.columns(2)

with col1:
    gst_file = st.file_uploader("Upload GSTR-2B Excel", type=["xlsx"])

with col2:
    pur_file = st.file_uploader("Upload Purchase Register", type=["xlsx"])

st.markdown("<br>", unsafe_allow_html=True)

# ---------------- PROCESS ----------------
if gst_file and pur_file:

    try:
        df_2b = pd.read_excel(gst_file)
        df_books = pd.read_excel(pur_file)

        # Clean column names
        df_2b.columns = df_2b.columns.str.strip()
        df_books.columns = df_books.columns.str.strip()

        if st.button("🚀 RUN RECONCILIATION", use_container_width=True):

            with st.spinner("Processing..."):

                # 🔹 MAIN RECO
                result_df = reco_logic.process_reco(df_2b, df_books)

                # 🔹 AUTO LEARNING APPLY (SAFE)
                if auto_mode:
                    try:
                        result_df = reco_logic.apply_previous_matches(result_df)
                    except Exception as e:
                        st.warning(f"Learning skipped: {e}")

            # ---------------- KPI ----------------
            total = len(result_df)

            matched = result_df["Match_Status"].str.contains(
                "Match", case=False, na=False
            ).sum()

            unmatched = total - matched

            st.markdown(f"""
            <div class="kpi-container">
                <div class="kpi-card">
                    <div class="kpi-label">Total</div>
                    <div class="kpi-value">{total}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Matched</div>
                    <div class="kpi-value">{matched}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Unmatched</div>
                    <div class="kpi-value">{unmatched}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            # ---------------- TABLE ----------------
            st.dataframe(result_df, use_container_width=True, height=400)

            # ---------------- DOWNLOAD ----------------
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                result_df.to_excel(writer, index=False)

            st.download_button(
                label="📥 Download Excel",
                data=output.getvalue(),
                file_name="GST_Reco_Report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

    except Exception as e:
        st.error(f"Error: {str(e)}")

else:
    st.info("Upload both files to start reconciliation")
