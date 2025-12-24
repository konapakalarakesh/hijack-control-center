import streamlit as st
import pandas as pd
from engine import process_hijack_data, to_excel # Added to_excel import

# Amazon Corporate Setup
st.set_page_config(page_title="Amazon Hijack Control", layout="wide", page_icon="👑")

# Sleek Industrial CSS
st.markdown("""
    <style>
    .stApp { background-color: #F2F3F3; font-family: 'Amazon Ember', 'Helvetica', sans-serif; }
    .main-header { background: #232F3E; padding: 15px 30px; border-bottom: 4px solid #FF9900; color: white; display: flex; align-items: center; justify-content: space-between; margin-bottom: 25px; }
    .card { background: white; border: 1px solid #D5D9D9; padding: 20px; border-radius: 4px; box-shadow: 0 1px 2px rgba(0,0,0,0.1); }
    .card-val { font-size: 2rem; color: #007185; font-weight: 700; }
    .card-lbl { font-size: 0.75rem; color: #565959; font-weight: 600; text-transform: uppercase; }
    
    /* Sleek Amazon Button Design */
    .stButton>button { background: linear-gradient(to bottom, #f7dfa5, #f0c14b); border: 1px solid #a88734; border-radius: 3px; color: #111; font-weight: 500; height: 38px; width: 100%; transition: all 0.2s; }
    .stButton>button:hover { background: linear-gradient(to bottom, #f5d78e, #eeb933); border-color: #9c7e31; box-shadow: 0 1px 0 rgba(255,255,255,.4) inset; cursor: pointer; }
    
    /* Export Buttons */
    .stDownloadButton>button { background: white; border: 1px solid #D5D9D9; border-radius: 3px; color: #232F3E; font-size: 0.85rem; width: 100%; margin-top: 5px; }
    .stDownloadButton>button:hover { background: #F7FAFA; border-color: #007185; color: #007185; }
    </style>
    """, unsafe_allow_html=True)

if 'processed' not in st.session_state: st.session_state.processed = False

# High-End Banner
st.markdown('<div class="main-header"><span>AMAZON HIJACK CONTROL CENTER</span><span>A konapaks creation</span></div>', unsafe_allow_html=True)

with st.sidebar:
    st.write("### SYSTEM PARAMETERS")
    sample_rate = st.slider("Sampling Accuracy", 1, 100, 15) / 100
    
    # SIMPLE VERIFIER ENTRY (Comma separated, no Ctrl+Enter required)
    v_raw = st.text_input("AUTHORIZED VERIFIERS (Comma separated)", " ")
    verifiers = [v.strip() for v in v_raw.split(',') if v.strip()]
    
    st.markdown("---")
    if st.button("RESET GLOBAL PIPELINE"):
        st.session_state.processed = False
        st.rerun()

# --- STEP 1: INGESTION ---
if not st.session_state.processed:
    st.write("#### I. RESOURCE UPLOAD")
    files = st.file_uploader("", type=['xlsx', 'csv'], accept_multiple_files=True, label_visibility="collapsed")
    
    if files:
        # Compact File Counter instead of Inventory Chips
        st.info(f"System Readiness: {len(files)} objects detected. Ready for high-speed collation.")
        
        if st.button("EXECUTE SYSTEM COLLATION"):
            master, pending, sample, stats, errors = process_hijack_data(files, sample_rate, verifiers)
            if errors:
                for e in errors: st.error(e)
            else:
                st.session_state.master, st.session_state.pending, st.session_state.sample, st.session_state.stats = master, pending, sample, stats
                st.session_state.processed = True
                st.rerun()

# --- STEP 2: DASHBOARD ---
else:
    st.write("#### II. OPERATIONAL ANALYTICS")
    c1, c2, c3 = st.columns(3)
    with c1: st.markdown(f'<div class="card"><div class="card-lbl">Completed Entries</div><div class="card-val">{len(st.session_state.master)}</div></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="card"><div class="card-lbl">Pending Tasklist</div><div class="card-val">{len(st.session_state.pending)}</div></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="card"><div class="card-lbl">Active Auditors</div><div class="card-val">{len(st.session_state.stats)}</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.write("#### III. EXPORT REPOSITORY")
    
    expA, expB = st.columns(2)
    with expA:
        # Updated to XLSX to preserve clickable links
        st.download_button("Export Final Master Dataset", to_excel(st.session_state.master), "Master_Final.xlsx", help="Full database of all completed auditor work.")
        st.download_button("Export Pending Worklist", to_excel(st.session_state.pending), "Worklist_Pending.xlsx", help="Entries with no decision for re-assignment.")
    with expB:
        st.download_button("Export Verification Batch", to_excel(st.session_state.sample), "Auditor_Sample.xlsx", help="Sampled work for verification.")
        st.download_button("Export Productivity Report", to_excel(st.session_state.stats), "Performance.xlsx", help="Valid/Invalid/Plausible breakdown per auditor.")