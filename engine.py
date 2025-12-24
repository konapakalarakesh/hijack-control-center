import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import io

def validate_and_read(file):
    try:
        # STEP 1: Read file into a memory buffer to bypass ALL "Locked" or "Access" errors
        file_content = file.read()
        file_buffer = io.BytesIO(file_content)
        
        if file.name.endswith(('.xlsx', '.xls')):
            # STEP 2: Use openpyxl for maximum compatibility with Streamlit Cloud
            df = pd.read_excel(file_buffer, engine='openpyxl')
        else:
            df = pd.read_csv(file_buffer)
            
        required = {'Decision', 'Unique ID', 'Proof of Affiliation Links'}
        if not required.issubset(df.columns):
            return None, f"COLUMN ERROR: {file.name} | Missing required headers."
        
        df['Source_Auditor'] = file.name
        return df, None
    except Exception as e:
        # Returns the specific technical error if it fails for easier troubleshooting
        return None, f"PROCESSING ERROR: {file.name} | {str(e)}"

def process_hijack_data(uploaded_files, sample_perc, verifiers):
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(validate_and_read, uploaded_files))
    
    all_dfs = [r[0] for r in results if r[0] is not None]
    critical_errors = [r[1] for r in results if r[1] is not None]

    if critical_errors:
        return None, None, None, None, critical_errors

    full_df = pd.concat(all_dfs, ignore_index=True)
    master_df = full_df.dropna(subset=['Decision']).copy()
    master_df = master_df[master_df['Decision'].astype(str).str.strip() != ""]
    pending_df = full_df[full_df['Decision'].isna() | (full_df['Decision'].astype(str).str.strip() == "")].copy()

    stats_df = pd.DataFrame([{
        "Auditor": a,
        "Valid": g['Decision'].astype(str).str.lower().str.count('valid').sum(),
        "Invalid": g['Decision'].astype(str).str.lower().str.count('invalid').sum(),
        "Plausible": g['Decision'].astype(str).str.lower().str.count('plausible').sum()
    } for a, g in full_df.groupby('Source_Auditor')])

    try:
        # Stratified sampling logic per Auditor and Decision
        sample_df = master_df.groupby(['Source_Auditor', 'Decision'], group_keys=False).apply(
            lambda x: x.sample(frac=sample_perc) if len(x) > 0 else x
        ).reset_index(drop=True)
        
        if verifiers and not sample_df.empty:
            sample_df = sample_df.sample(frac=1).reset_index(drop=True)
            sample_df['Assigned_Verifier'] = np.resize(verifiers, len(sample_df))
    except Exception:
        sample_df = master_df.sample(frac=sample_perc) if not master_df.empty else master_df

    return master_df, pending_df, sample_df, stats_df, []