import streamlit as st
import pandas as pd
import io
import reco_logic as reco_logic

# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="GST Reco Pro | Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---------------- ULTRA-MODERN CSS INJECTION ----------------
st.markdown("""
    <style>
    /* Main Background */
    .stApp { background-color: #F0FDF4; } /* Very Light Green/Off-White */
    
    /* Animations */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    .animate-fade { animation: fadeIn 0.6s ease-out forwards; }

    /* Hero Header */
    .hero-header {
        background: linear-gradient(135deg, #064E3B 0%, #10B981 50%, #6EE7B7 100%); /* Green Gradient */
        padding: 2.5rem 2rem;
        border-radius: 16px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 10px 25px rgba(16, 185, 129, 0.2);
        position: relative;
        overflow: hidden;
    }
    
    /* Custom KPI Cards */
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
        border-bottom: 4px solid #6EE7B7; /* Light Green */
        text-align: center;
        transition: transform 0.3s ease;
    }
    .kpi-card:hover { transform: translateY(-5px); }
    .kpi-card.discrepancy { border-bottom: 4px solid #059669; } /* Medium Green */
    .kpi-value { font-size: 2.2rem; font-weight: 800; color: #064E3B; margin: 0.5rem 0; } /* Dark Green */
    .kpi-label { font-size: 0.9rem; color: #64748B; text-transform: uppercase; letter-spacing: 1px; font-weight: 600; }

    /* Modern Buttons */
    .stButton>button {
        background: linear-gradient(135deg, #10B981 0%, #059669 100%); /* Green Gradient */
        color: white;
        border: none;
        padding: 0.8rem 2rem;
        border-radius: 50px; /* Pill shape */
        font-weight: bold;
        font-size: 1.1rem;
        letter-spacing: 0.5px;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(16, 185, 129, 0.3);
    }
    .stButton>button:hover {
        transform: translateY(-2px) scale(1.02);
        box-shadow: 0 6px 20px rgba(16, 185, 129, 0.5);
        color: white;
    }
    
    /* Empty State */
    .empty-state {
        background: white;
        padding: 4rem 2rem;
        text-align: center;
        border-radius: 16px;
        border: 2px dashed #A7F3D0; /* Light Green Dashed */
        color: #064E3B;
        margin-top: 2rem;
    }
    </style>
""", unsafe_allow_html=True)

# ---------------- HEADER SECTION ----------------
st.markdown("""
    <div class="hero-header animate-fade">
        <h1 style='margin:0; font-size: 3rem; font-weight: 800; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);'>⚡ GST Intelligence Hub</h1>
        <p style='margin:5px 0 0 0; font-size: 1.2rem; opacity: 0.9;'>Automated GSTR-2B vs Books Reconciliation</p>
    </div>
""", unsafe_allow_html=True)

# ---------------- UPLOAD ZONE ----------------
col1, col2 = st.columns(2)
with col1:
    st.markdown("#### 📘 GSTR-2B Data")
    gst_file = st.file_uploader("Drop GSTR-2B Excel here", type=["xlsx"], key="gst", label_visibility="collapsed")
with col2:
    st.markdown("#### 📙 Purchase Register")
    pur_file = st.file_uploader("Drop Purchase Books Excel here", type=["xlsx"], key="pur", label_visibility="collapsed")

st.markdown("<br>", unsafe_allow_html=True)

# ---------------- MAIN PROCESSOR ----------------
if gst_file and pur_file:
    try:
        df_2b = pd.read_excel(gst_file)
        df_books = pd.read_excel(pur_file)
        
        df_2b.columns = df_2b.columns.str.strip()
        df_books.columns = df_books.columns.str.strip()

        # Center the button
        _, btn_col, _ = st.columns([1, 2, 1])
        with btn_col:
            run_btn = st.button("🚀 INITIATE PROCESS", use_container_width=True)

        if run_btn:
            with st.spinner("🧠 Please Wait!..."):
                result_df = reco_logic.process_reco(df_2b, df_books)

            st.markdown('<div class="animate-fade">', unsafe_allow_html=True)
            
            # --- CALCULATE METRICS ---
            total = len(result_df)

            # 1. Find everything that has "Match"
            is_match = result_df["Match_Status"].str.contains("Match", case=False, na=False)

            # 2. Find everything that has "Fuzzy"
            is_fuzzy = result_df["Match_Status"].str.contains("Fuzzy", case=False, na=False)

            # 3. Combine: Count where it IS a match, AND is NOT fuzzy
            matched = (is_match & ~is_fuzzy).sum()

            unmatched = total - matched

            # --- CUSTOM KPI CARDS ---
            st.markdown(f"""
                <div class="kpi-container">
                    <div class="kpi-card">
                        <div class="kpi-label">Total Invoices Processed</div>
                        <div class="kpi-value">{total:,}</div>
                    </div>
                    <div class="kpi-card" style="border-bottom-color: #10B981;">
                        <div class="kpi-label">Perfect Matches</div>
                        <div class="kpi-value" style="color: #10B981;">{matched:,}</div>
                    </div>
                    <div class="kpi-card discrepancy">
                        <div class="kpi-label">Discrepancies</div>
                        <div class="kpi-value" style="color: #059669;">{unmatched:,}</div>
                    </div>
                    
                </div>
            """, unsafe_allow_html=True)

            # --- DETAILED LEDGER (Direct View) ---
            st.markdown("### 📋 Detailed ")
            
            # FIXED: Changed applymap() to map() for modern Pandas compatibility
            st.dataframe(
                result_df.style.map(
                    lambda x: "background-color: #D1FAE5; color: #064E3B;" if x == "Mismatch" else "", 
                    subset=["Match_Status"]
                ),
                use_container_width=True, 
                height=400
            )

            # --- EXPORT SECTION ---
            st.markdown("<br>", unsafe_allow_html=True)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                result_df.to_excel(writer, index=False)
            
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
            <h2 style="margin-bottom: 10px;">Awaiting Data Injection 🚀</h2>
            <p>Upload your <b>GSTR-2B</b> and <b>Purchase Register</b> files above to trigger the reconciliation engine.</p>
        </div>
    """, unsafe_allow_html=True)
