import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor

def validate_and_read(file):
    try:
        # High-speed reading for massive datasets
        if file.name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file, engine='calamine')
        else:
            df = pd.read_csv(file)
            
        required = {'Decision', 'Unique ID', 'Proof of Affiliation Links'}
        if not required.issubset(df.columns):
            return None, f"HEADER ERROR: {file.name} | Missing required columns."
        
        df['Source_Auditor'] = file.name
        return df, None
    except Exception:
        # Catches locked or corrupted files as seen in image_0d5746.png
        return None, f"FILE ERROR: {file.name} | Close in Excel and re-upload."

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
        # Group by both Auditor and Decision to pull the percentage from every subgroup
        sample_df = master_df.groupby(['Source_Auditor', 'Decision'], group_keys=False).apply(
            lambda x: x.sample(frac=sample_perc) if len(x) > 0 else x
        ).reset_index(drop=True)
        
        # Distribute verifiers evenly across the final combined sample
        if verifiers and not sample_df.empty:
            # Shuffle so verifiers get a mix of different auditors' work
            sample_df = sample_df.sample(frac=1).reset_index(drop=True)
            sample_df['Assigned_Verifier'] = np.resize(verifiers, len(sample_df))
    except Exception:
        # Emergency fallback to maintain system stability if data is too small
        sample_df = master_df.sample(frac=sample_perc) if not master_df.empty else master_df

    return master_df, pending_df, sample_df, stats_df, []