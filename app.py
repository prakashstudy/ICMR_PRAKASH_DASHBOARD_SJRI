import dash
from dash import dcc, html, dash_table, no_update, callback_context, ClientsideFunction
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, ALL
import plotly.graph_objects as go
import pandas as pd
import json
import re
import urllib.parse
from datetime import datetime
import threading
import hashlib



import flask
import os
from who_standards import calculate_bmi_z_score, classify_who_z_score

# =========================
# GLOBAL CONSTANTS
# =========================
BENEFICIARY_MAP = {
    2: "Pregnant Women",
    3: "Children 5-59 Months",
    4: "Children Aged 5-9 Years",
    5: "Adolescent Girls 10-19 Years",
    6: "Adolescent Boys 10-19 Years",
    7: "Women Of Reproductive Age"
}
BLOCK_CODE_MAP = {
    "2": "Yelburga",
    "3": "Kushtagi",
    "4": "Gangavathi",
    "5": "Koppal"
}

anemia_list = ["normal", "mild", "moderate", "severe"]

# =========================
# PII ANONYMIZATION (DPDP)
# =========================
PII_SALT = "DASHBOARD_2025_SECURE"

def salt_hash_pii(val, prefix=""):
    """Creates a non-reversible hash for PII data."""
    if val is None or pd.isna(val) or str(val).strip() == "":
        return ""
    clean_val = str(val).strip().lower()
    hash_obj = hashlib.sha256((clean_val + PII_SALT).encode())
    return f"{prefix}{hash_obj.hexdigest()[:8].upper()}"

def mask_pii_readable(val):
    """Masks string to show first and last letter (e.g. Ashwin -> A****n)"""
    if val is None or pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip()
    if len(s) <= 2:
        return s
    return f"{s[0]}{'*' * 4}{s[-1]}"

def mask_contact(val):
    """Masks phone numbers to protect identity (e.g. 91XXXXX12)"""
    if val is None or pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip()
    if len(s) >= 4:
        return f"{s[:2]}{'X' * (len(s)-4)}{s[-2:]}"
    return "****"

# =========================
# DASH INIT
# =========================
app = dash.Dash(__name__, 
                external_stylesheets=[
                    dbc.themes.BOOTSTRAP,
                    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css"
                ], 
                suppress_callback_exceptions=True,
                eager_loading=True)
app.scripts.config.serve_locally = True
app.css.config.serve_locally = True
server = app.server

@server.route('/<filename>')
def serve_assets(filename):
    if filename in ['images.png', 'main_logo.svg', 'government-of-karnataka.jpg']:
        root_dir = os.path.dirname(os.path.abspath(__file__))
        return flask.send_from_directory(root_dir, filename)
    return flask.abort(404)

# Styles are now loaded from assets/style_v3.css automatically

# =========================
# LOAD DATA (URL or CSV)
# =========================
DATA_SOURCE_URL = "https://script.google.com/macros/s/AKfycbzazlpEvo3qo2pVhp0fvcpUrlcyR9QRE2SYED5fu-5Og5oVBHZ-EIbaOR-VNCwEIC6JdQ/exec" 
# Paste your deployed Google Apps Script Web App URL here to enable write-back
EXCEL_WRITE_URL = "https://script.google.com/macros/s/AKfycbyfwRVnmXLB8qQt31kIGBmC1NxZ_atYNnM4h-M0sREFpIJJ5au8X9uu8Olwch80XRNpqQ/exec" 
LAST_SYNC_CACHE = {} 
CACHE_FILE = "sync_cache.json"

def load_sync_cache():
    global LAST_SYNC_CACHE
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                LAST_SYNC_CACHE = json.load(f)
            # print(f"DEBUG: Loaded {len(LAST_SYNC_CACHE)} records from sync cache.")
        except Exception as e:
            print(f"DEBUG: Failed to load sync cache: {e}")
            LAST_SYNC_CACHE = {}
    else:
        LAST_SYNC_CACHE = {}

def save_sync_cache():
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(LAST_SYNC_CACHE, f)
    except Exception as e:
        print(f"DEBUG: Failed to save sync cache: {e}")

# Initial load
load_sync_cache()

# Notification tracking
NOTIFIED_CACHE = {}
NOTIFIED_FILE = "notified_ashas.json"

def load_notified_cache():
    global NOTIFIED_CACHE
    if os.path.exists(NOTIFIED_FILE):
        try:
            with open(NOTIFIED_FILE, "r") as f:
                NOTIFIED_CACHE = json.load(f)
        except Exception as e:
            print(f"DEBUG: Failed to load notified cache: {e}")
            NOTIFIED_CACHE = {}
    else:
        NOTIFIED_CACHE = {}

def save_notified_cache():
    try:
        with open(NOTIFIED_FILE, "w") as f:
            json.dump(NOTIFIED_CACHE, f)
    except Exception as e:
        print(f"DEBUG: Failed to save notified cache: {e}")

# Initial load for notifications
load_notified_cache()

# BENEFICIARY_MAP moved to GLOBAL CONSTANTS at top of file

def parse_age(age_val):
    if pd.isna(age_val) or age_val == "":
        return None
    
    # Handle already numeric values
    if isinstance(age_val, (int, float)):
        return age_val if age_val < 150 else None
        
    if hasattr(age_val, 'year') and hasattr(age_val, 'month'):
        # If it's a date, we probably can't infer age without a reference date, 
        # but let's assume it's not an age.
        return None

    age_str = str(age_val).lower().strip()
    
    # 1. If it's just a simple number string (e.g. "21" or "21.5")
    clean_num = age_str.replace('yr', '').replace('yrs', '').replace('yr.', '').strip()
    try:
        val = float(clean_num)
        return val if val < 150 else None
    except:
        pass

    # 2. Rule out strings that look like full dates (e.g., "2021-06-01" or "21/06/19")
    if re.search(r'\d{1,4}[-/]\d{1,2}[-/]\d{1,4}', age_str):
        return None

    years = 0.0
    months = 0.0
    
    # 3. Explicit search for suffixes (Highest priority)
    y_match = re.search(r'(\d+(\.\d+)?)\s*(y|yr|year)', age_str)
    m_match = re.search(r'(\d+(\.\d+)?)\s*(m|mo|month)', age_str)
    
    if y_match or m_match:
        if y_match: years = float(y_match.group(1))
        if m_match: months = float(m_match.group(1))
        # If years looks like a birth year, disregard it
        if years > 1900: years = 0
    else:
        # 4. Fallback: No suffixes, look for "Number Number"
        nums = re.findall(r'(\d+(\.\d+)?)', age_str)
        if len(nums) >= 1:
            val1 = float(nums[0][0])
            if val1 > 1900: # First number is a year
                if len(nums) >= 2: years = float(nums[1][0])
                if len(nums) >= 3: months = float(nums[2][0])
            else:
                years = val1
                if len(nums) >= 2: months = float(nums[1][0])
    
    res = round(years + (months / 12), 2)
    return res if 0 < res < 150 else None

def classify_anemia_who(hgb, age, gender, beneficiary):
    """
    Classify anemia based on WHO guidelines.
    
    Parameters:
    - hgb: Haemoglobin level in g/dL (REQUIRED)
    - age: Age in years (Optional if beneficiary type is specific)
    - gender: Gender (Male/Female)
    - beneficiary: Beneficiary category
    
    Returns: 'normal', 'mild', 'moderate', 'severe', or 'incomplete' if data is insufficient
    """
    # Handle missing HGB - REQUIRED
    if pd.isna(hgb) or hgb is None:
        return "incomplete"
    
    # Convert HGB to float
    try:
        hgb = float(hgb)
    except:
        return "incomplete"

    # Handle Age (Optional, but convert if present)
    try:
        if age is not None and not pd.isna(age) and str(age).strip() != "":
            age = float(age)
        else:
            age = None
    except:
        age = None
    
    # Normalize inputs
    gender_str = str(gender).lower().strip() if not pd.isna(gender) else ""
    beneficiary_str = str(beneficiary).lower().strip() if not pd.isna(beneficiary) else ""
    
    # Determine classification based on beneficiary type OR age
    
    # Pregnant Women
    if "pregnant" in beneficiary_str:
        if hgb >= 11.0:
            return "normal"
        elif hgb >= 10.0:
            return "mild"
        elif hgb >= 7.0:
            return "moderate"
        else:
            return "severe"
    
    # Children 5-59 Months (6-59 months WHO category)
    elif "5-59 months" in beneficiary_str or "children 5-59 months" in beneficiary_str:
        if hgb >= 11.0:
            return "normal"
        elif hgb >= 10.0:
            return "mild"
        elif hgb >= 7.0:
            return "moderate"
        else:
            return "severe"
    
    # Children Aged 5-9 Years
    elif "5-9 years" in beneficiary_str:
        if hgb >= 11.5:
            return "normal"
        elif hgb >= 11.0:
            return "mild"
        elif hgb >= 8.0:
            return "moderate"
        else:
            return "severe"
    
    # Adolescent Girls 10-19 Years
    elif "adolescent girls" in beneficiary_str or ("adolescent" in beneficiary_str and "female" in gender_str):
        if hgb >= 12.0:
            return "normal"
        elif hgb >= 11.0:
            return "mild"
        elif hgb >= 8.0:
            return "moderate"
        else:
            return "severe"
    
    # Adolescent Boys 10-19 Years
    elif "adolescent boys" in beneficiary_str or ("adolescent" in beneficiary_str and "male" in gender_str):
        if hgb >= 12.0:
            return "normal"
        elif hgb >= 11.0:
            return "mild"
        elif hgb >= 8.0:
            return "moderate"
        else:
            return "severe"
    
    # Women Of Reproductive Age (non-pregnant)
    elif "women of reproductive age" in beneficiary_str or "reproductive age" in beneficiary_str:
        if hgb >= 12.0:
            return "normal"
        elif hgb >= 11.0:
            return "mild"
        elif hgb >= 8.0:
            return "moderate"
        else:
            return "severe"
    
    # Fallback: Use age and gender if beneficiary type doesn't match
    elif age is not None:
        # Children under 5 years
        if age < 5:
            if hgb >= 11.0:
                return "normal"
            elif hgb >= 10.0:
                return "mild"
            elif hgb >= 7.0:
                return "moderate"
            else:
                return "severe"
        
        # Children 5-11 years
        elif age < 12:
            if hgb >= 11.5:
                return "normal"
            elif hgb >= 11.0:
                return "mild"
            elif hgb >= 8.0:
                return "moderate"
            else:
                return "severe"
        
        # Adolescents and Adults (12+ years)
        else:
            # Female thresholds
            if "female" in gender_str or "f" == gender_str:
                if hgb >= 12.0:
                    return "normal"
                elif hgb >= 11.0:
                    return "mild"
                elif hgb >= 8.0:
                    return "moderate"
                else:
                    return "severe"
            # Male thresholds
            elif "male" in gender_str or "m" == gender_str:
                if hgb >= 13.0:
                    return "normal"
                elif hgb >= 11.0:
                    return "mild"
                elif hgb >= 8.0:
                    return "moderate"
                else:
                    return "severe"
            # Missing gender - use female thresholds (more conservative)
            else:
                if hgb >= 12.0:
                    return "normal"
                elif hgb >= 11.0:
                    return "mild"
                elif hgb >= 8.0:
                    return "moderate"
                else:
                    return "severe"
    # If we can't determine (missing/unclear beneficiary AND missing age), return incomplete
    return "incomplete"

def sync_data_to_sheets(df):
    """
    Sends computed data (Anemia Status, Corrected Age) back to Google Sheets.
    Only syncs rows that are new or have changed since the last session.
    """
    global LAST_SYNC_CACHE
    if not EXCEL_WRITE_URL or "PASTE_SCRIPT_URL_HERE" in EXCEL_WRITE_URL:
        return
    
    if df.empty:
        return

    sync_cols = [
        "SL.NO", "ID", "enrollment_date", "Area Code", "PSU Name", 
        "Name", "Gender", "Benificiery", "HGB", "anemia_category",
        "Length", "Height", "Weight", "Age", "whatsapp",
        "Diet 1", "Diet 2", "field_investigator", "Asha_Worker", "data_operator",
        "Sample Collected Date", "bmi_category", "BMI", "Email", "Status"
    ]
    
    # Identify which columns actually exist in the current dataframe
    cols_to_use = [c for c in sync_cols if c in df.columns]
    
    # --- Row-Level Diffing ---
    diff_rows = []
    temp_cache = LAST_SYNC_CACHE.copy()
    
    for _, row in df.iterrows():
        p_id = str(row.get("ID", "")).strip()
        if not p_id or p_id.lower() == "nan": continue
        
        # Create a unique signature for this row based on its values
        row_values = [str(row.get(c, "")).strip() for c in cols_to_use]
        row_sig = "|".join(row_values)
        
        # If ID is new OR the data has changed, mark for sync
        if p_id not in LAST_SYNC_CACHE or LAST_SYNC_CACHE[p_id] != row_sig:
            diff_rows.append(row)
            temp_cache[p_id] = row_sig
            
    if not diff_rows:
        # print("DEBUG: No changes detected at row level. Skipping background sync.")
        return
    
    # Update cache locally (we'll commit to file if the request succeeds)
    # Actually, it's safer to update internal cache only after success, but we prepared temp_cache
    # temp_cache already has the updates.

    
    print(f"DEBUG: Found {len(diff_rows)} new/updated records to sync to Sheets.")

    try:
        import requests
        # Prepare data for sync
        sync_df = pd.DataFrame(diff_rows)
        
        # Convert types for JSON compatibility
        for col in sync_df.columns:
            if pd.api.types.is_datetime64_any_dtype(sync_df[col]):
                sync_df[col] = sync_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Critical: Replace NaN with None so they become null in JSON
        payload = sync_df.replace({pd.NA: None, float('nan'): None}).to_dict("records")
        
        # Syncing...
        r = requests.post(EXCEL_WRITE_URL, json=payload, timeout=120, allow_redirects=True)
        if r.status_code != 200:
            print(f"DEBUG: Data sync failed with status {r.status_code}: {r.text[:200]}")
        else:
            print(f"DEBUG: Data sync successful: {r.json().get('message') if r.text.startswith('{') else 'OK'}")
            # Update cache only after successful delivery
            LAST_SYNC_CACHE = temp_cache
            save_sync_cache()
    except Exception as e:
        import traceback
        print(f"DEBUG: Data sync exception trace: {traceback.format_exc()}")

def load_data():
    """
    Fetches data from Google Apps Script. 
    Returns: (df, status_message, is_error)
    """
    status_msg = "Live"
    is_error = False
    try:
        import requests
        # Increased timeout to 20s to prevent 'Server did not respond' errors on slower links
        r = requests.get(DATA_SOURCE_URL, timeout=20)
        r.raise_for_status()
        
        try:
            data_json = r.json()
            # Debug: Print a snippet of the JSON to the console
            print("DEBUG: Fetched JSON Data (snippet):", str(data_json)[:500] + "...")
            
            if isinstance(data_json, dict) and 'data' in data_json:
                df = pd.DataFrame(data_json['data'])
            else:
                df = pd.DataFrame(data_json)
        except:
            from io import StringIO
            df = pd.read_csv(StringIO(r.text))
            
        if df.empty:
            return pd.DataFrame(), "No Data in Script", True

        df.columns = df.columns.str.strip()
        
        # --- Auto-generate Sl.No (Replace Excel's Sl.No) ---
        df = df.reset_index(drop=True)
        df["Sl.No"] = df.index + 1
        
        # --- PII ANONYMIZATION (DPDP Act Compliance) ---
        if not df.empty:
            # Preserve real contact for background logic (WhatsApp) but hide it from the table
            if "Aasha_Contact" in df.columns:
                df["_real_contact"] = df["Aasha_Contact"].astype(str)
                df["Aasha_Contact"] = df["Aasha_Contact"].apply(mask_contact)
            
            # Mask sensitive names
            if "Name" in df.columns:
                df["Name"] = df["Name"].apply(mask_pii_readable)
            if "Household Name" in df.columns:
                df["Household Name"] = df["Household Name"].apply(mask_pii_readable)
            if "Email" in df.columns:
                df["Email"] = df["Email"].apply(mask_pii_readable)
            
            # Mask Staff Names (Traceable format)
            if "Asha_Worker" in df.columns:
                df["Asha_Worker"] = df["Asha_Worker"].apply(mask_pii_readable)
            if "field_investigator" in df.columns:
                df["field_investigator"] = df["field_investigator"].apply(mask_pii_readable)
            if "data_operator" in df.columns:
                df["data_operator"] = df["data_operator"].apply(mask_pii_readable)
            if "Collected By" in df.columns:
                df["Collected By"] = df["Collected By"].apply(mask_pii_readable)
        # -----------------------------------------------

        required_cols = [
            "Sl.No", "ID", "enrollment_date", "BlockCode", "Area Code", "PSU Name",
            "Name", "Household Name", "Gender", "Benificiery", "Trimester", "DOB", "Age",
            "sample_status", "Sample Collected Date", "Collected By",
            "HGB", "anemia_category", "field_investigator", "data_operator",
            "Asha_Worker", "Aasha_Contact", "Diet 1", "Diet 2", "benficiery qn",
            "Length", "Height", "Weight", "Email", "Status", "_real_contact"
        ]
        df = df[[c for c in required_cols if c in df.columns]]

        date_cols = ["DATE_F", "enrollment_date", "DOB", "Sample Collected Date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        
        if "HGB" in df.columns:
            df["HGB"] = pd.to_numeric(df["HGB"], errors="coerce")

        # Numeric conversion for anthropometric data
        for col in ["Length", "Height", "Weight"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
        # Rename Diet and Asha columns for consistency
        rename_map = {}
        col_map_lower = {c.lower().replace("_", " "): c for c in df.columns}
        
        # Flexible mapping for Asha data
        if "asha_worker" not in df.columns:
            if "asha worker" in col_map_lower: rename_map[col_map_lower["asha worker"]] = "Asha_Worker"
            elif "asha" in col_map_lower: rename_map[col_map_lower["asha"]] = "Asha_Worker"
            elif "ashaworker" in col_map_lower: rename_map[col_map_lower["ashaworker"]] = "Asha_Worker"

        if "aasha_contact" not in df.columns:
            if "aasha contact" in col_map_lower: rename_map[col_map_lower["aasha contact"]] = "Aasha_Contact"
            elif "asha contact" in col_map_lower: rename_map[col_map_lower["asha contact"]] = "Aasha_Contact"
            elif "asha number" in col_map_lower: rename_map[col_map_lower["asha number"]] = "Aasha_Contact"
            elif "contact" in col_map_lower: rename_map[col_map_lower["contact"]] = "Aasha_Contact"
            elif "aasha contact number" in col_map_lower: rename_map[col_map_lower["aasha contact number"]] = "Aasha_Contact"

        # Mapping rules for Diet based on user feedback
        if "diet 1" not in df.columns:
            if "diet" in col_map_lower: rename_map[col_map_lower["diet"]] = "Diet 1"
        if "diet 2" not in df.columns:
            if "diet1" in col_map_lower: rename_map[col_map_lower["diet1"]] = "Diet 2"
        
        if rename_map:
            df = df.rename(columns=rename_map)

        # Calculate BMI: Weight(kg) / [Height(m)]²
        # Row-level fallback: Use Height if available, otherwise use Length
        if "Weight" in df.columns:
            h_vals = df["Height"] if "Height" in df.columns else (df["Length"] if "Length" in df.columns else pd.Series([None] * len(df)))
            if "Height" in df.columns and "Length" in df.columns:
                h_vals = df["Height"].fillna(df["Length"])
            
            # height in meters, ensure not zero
            valid_mask = (df["Weight"] > 0) & (h_vals > 0)
            df["BMI"] = None
            df.loc[valid_mask, "BMI"] = (df.loc[valid_mask, "Weight"] / ((h_vals.loc[valid_mask] / 100.0) ** 2)).round(1)
        else:
            df["BMI"] = None

        def classify_nutritional_status(row):
            bmi = row.get("BMI")
            age_y = row.get("Age")
            beneficiary = str(row.get("Benificiery", "")).lower()
            gender_raw = str(row.get("Gender", "")).lower().strip()
            
            # Exemption: If pregnant and BMI >= 30, classify as Obese (pre-pregnancy proxy)
            if "pregnant" in beneficiary or "(pw)" in beneficiary:
                if not pd.isna(bmi) and bmi >= 30.0:
                    return "Obese"
                return "Pregnancy"
                
            if pd.isna(bmi) or bmi is None: return "Data Missing"
            
            try:
                val = float(bmi)
            except:
                return "Data Missing"

            # Map Gender to WHO 'boys'/'girls'
            gender_who = None
            if gender_raw in ["male", "m", "boy", "boys"]:
                gender_who = "boys"
            elif gender_raw in ["female", "f", "girl", "girls"]:
                gender_who = "girls"

            # Use WHO Z-scores for children < 19 if gender is known
            if age_y is not None and not pd.isna(age_y) and gender_who:
                try:
                    age_val = float(age_y)
                    if age_val < 19:
                        age_m = age_val * 12.0
                        # Try/Except inside loop to prevent single row failure from crashing everything
                        try:
                            z = calculate_bmi_z_score(val, gender_who, age_m)
                            if z is not None:
                                return classify_who_z_score(z, age_m)
                        except Exception as z_err:
                            print(f"DEBUG: Z-score calculation failed for row: {z_err}")
                except Exception as e:
                    # Fallback to adult logic on error
                    pass

            # Adult Fallback (>= 19 or unknown gender/age)
            if val < 18.5: return "Underweight"
            if val < 25.0: return "Normal"
            if val < 30.0: return "Overweight"
            return "Obese"
        
        # We need Age to be parsed BEFORE classification
        # Parse Age with special logic FIRST
        if "Age" in df.columns:
            df["Age"] = df["Age"].apply(parse_age)
        else:
            df["Age"] = None
            
        # Cross-calculate Age from DOB if missing
        if "DOB" in df.columns:
            # Use enrollment_date as reference, fallback to today
            ref_date = df["enrollment_date"].fillna(pd.Timestamp.now())
            
            # Mask for missing Ages where DOB exists
            mask = df["Age"].isna() & df["DOB"].notna()
            
            if mask.any():
                # Ensure compatibility by removing timezones (tz-naive)
                try:
                    # Fix: Ensure ref_date is also timezone-naive to match localized(None) DOB
                    ref_dt_naive = pd.to_datetime(ref_date[mask]).dt.tz_localize(None)
                    dob_dt_naive = pd.to_datetime(df.loc[mask, "DOB"]).dt.tz_localize(None)
                    diff = (ref_dt_naive - dob_dt_naive).dt.days
                    calculated_ages = (diff / 365.25).round(2)
                    # Only apply if result is sane
                    df.loc[mask, "Age"] = calculated_ages.apply(lambda x: x if 0 <= x < 150 else None)
                except Exception as age_err:
                    print(f"DEBUG: Age calculation fallback failed: {age_err}")

        # Now apply classification using the populated Age
        df["bmi_category"] = df.apply(classify_nutritional_status, axis=1)
        
        if "Area Code" in df.columns:
            df["Area Code"] = df["Area Code"].astype(str).str.zfill(3)

        if "PSU Name" in df.columns and "Area Code" in df.columns:
            df["Location"] = df["PSU Name"].astype(str) + " (" + df["Area Code"].astype(str) + ")"
        elif "PSU Name" in df.columns:
            df["Location"] = df["PSU Name"].astype(str)
        else:
            df["Location"] = "Missing"

        if "anemia_category" in df.columns:
            df["anemia_category"] = df["anemia_category"].astype(str).str.strip()
            cat_map = {"Normal": "normal", "Mild anemia": "mild", "Moderate anemia": "moderate", "Severe anemia": "severe"}
            df["anemia_category"] = df["anemia_category"].map(cat_map).fillna(df["anemia_category"].str.lower())

        if "Benificiery" in df.columns:
            df["Benificiery"] = pd.to_numeric(df["Benificiery"], errors='coerce')
            df["Benificiery"] = df["Benificiery"].map(BENEFICIARY_MAP).fillna(df["Benificiery"])
            df["Benificiery"] = df["Benificiery"].astype(str).str.title()

        if "BlockCode" in df.columns:
            # Clean and map Block Codes
            def format_block(x):
                try:
                    # Handle float/int conversions safely
                    val = str(int(float(x)))
                    name = BLOCK_CODE_MAP.get(val, "Unknown")
                    return f"{name} ({val})"
                except:
                    return str(x)
            
            df["BlockCode"] = df["BlockCode"].apply(format_block)


        if "Name" in df.columns:
            df["Name"] = df["Name"].astype(str).str.title()
            
        if "Asha_Worker" in df.columns:
            df["Asha_Worker"] = df["Asha_Worker"].astype(str).str.title()
            
        if "Aasha_Contact" in df.columns:
            # Clean phone numbers (remove non-digits)
            df["Aasha_Contact"] = df["Aasha_Contact"].astype(str).str.replace(r'\D', '', regex=True)
            # Add country code if missing (assumed India +91)
            def fix_phone(p):
                if not p or p == "" or p == "nan": return ""
                if len(p) == 10: return "91" + p
                return p
            df["Aasha_Contact"] = df["Aasha_Contact"].apply(fix_phone)

        # Apply WHO-based automatic anemia classification
        if "HGB" in df.columns:
            df["anemia_category"] = df.apply(
                lambda row: classify_anemia_who(
                    row.get("HGB"),
                    row.get("Age"),
                    row.get("Gender"),
                    row.get("Benificiery")
                ),
                axis=1
            )
        else:
            df["anemia_category"] = None

        # FILTER: Keep rows where either Age OR Beneficiary is present
        # Check for valid Age (not None/NaN)
        has_age = df["Age"].notna()
        
        # Check for valid Beneficiary (not None/NaN/empty/"Nan")
        # Since we converted to string title case earlier, check against "Nan" and "None" strings
        has_beneficiary = (
            df["Benificiery"].notna() & 
            (df["Benificiery"] != "") & 
            (df["Benificiery"].str.lower() != "nan") & 
            (df["Benificiery"].str.lower() != "none")
        )
        
        df = df[has_age | has_beneficiary]

        return df, "Live", False

    except Exception as e:
        return pd.DataFrame(), f"Script Error: {str(e)}", True

def generate_weekly_summary(df):
    """
    Groups data by Asha Worker and creates a formatted summary for WhatsApp.
    """
    if df.empty: return []
    
    summaries = []
    # Identify anemic subjects
    anemic_df = df[df["anemia_category"].isin(["severe", "moderate", "mild"])].copy()
    if anemic_df.empty: return []
    
    # Sort for consistent output
    anemic_df = anemic_df.sort_values(["anemia_category", "HGB"], ascending=[False, True])
    
    for asha_name, asha_group in anemic_df.groupby("Asha_Worker"):
        # Handle cases where Asha name is missing or placeholder
        display_name = asha_name
        show_whatsapp = True
        
        if not asha_name or asha_name.lower() in ["nan", "none", "", "missing"]:
            display_name = "Asha Details Missing"
            show_whatsapp = False
            
        contact = str(asha_group["Aasha_Contact"].iloc[0]) if "Aasha_Contact" in asha_group.columns else ""
        if not contact or contact.lower() in ["nan", "none", ""]:
            show_whatsapp = False
            
        village = asha_group["PSU Name"].iloc[0] if "PSU Name" in asha_group.columns else "Unknown"
        
        severe = len(asha_group[asha_group["anemia_category"] == "severe"])
        moderate = len(asha_group[asha_group["anemia_category"] == "moderate"])
        
        lines = [
            f"*Weekly Summary for Asha: {display_name}*",
            f"Village: {village}",
            f"Total Severe: {severe} | Moderate: {moderate}",
            "",
            "*Subjects to Check:*"
        ]
        
        for i, (_, row) in enumerate(asha_group.iterrows(), 1):
            cat = str(row["anemia_category"]).capitalize()
            lines.append(f"{i}. {row['Name']} ({row['ID']}) - {cat} (Hb: {row['HGB']})")
        
        summary_text = "\n".join(lines)
        summaries.append({
            "asha": display_name,
            "contact": contact,
            "village": village,
            "text": summary_text,
            "count": len(asha_group),
            "show_whatsapp": show_whatsapp
        })
    
    return summaries

psu_list = []
area_list = []
anemia_list = ["normal", "mild", "moderate", "severe", "incomplete"]

def area_coordinates():
    return {
        'Kunikera': {'lat': 15.2832, 'lon': 76.2142},
        'Ojanahalli': {'lat': 15.3856, 'lon': 76.1472},
        'Bannikoppa': {'lat': 15.3877, 'lon': 75.9420},
        'Tadkal': {'lat': 15.3688, 'lon': 75.9812},
        'Hulegudda': {'lat': 15.6235, 'lon': 76.1146},
        'Konasagara': {'lat': 15.6916, 'lon': 76.1030},
        'Kawalbodur': {'lat': 15.8318, 'lon': 76.1871},
        'Balutagi': {'lat': 15.87338865573784, 'lon': 76.25665534853232},
        'HireGonnagar': {'lat': 15.8092, 'lon': 75.9539},
        'Anegundi': {'lat': 15.3507, 'lon': 76.4925},
        'Kilarhatti': {'lat': 15.8411, 'lon': 76.4359},
        'Challur': {'lat': 15.6014, 'lon': 76.5943},
        'Marlanahalli': {'lat': 15.5771, 'lon': 76.6490},
        'Gouripur': {'lat': 15.6187547, 'lon': 76.35504569999999},
        'Hatti': {'lat': 15.2117, 'lon': 75.9350},
        'Komalapur': {'lat': 15.3405, 'lon':76.0215},
        'Chikwankal Kunta': {'lat': 15.629761351168723, 'lon':76.23304865792784},
        'Hire Wankal Kunta': {'lat': 15.646960083050104, 'lon':76.238318366376871},
        'Talkere': {'lat': 15.645466597713694, 'lon': 76.26477078258641},
        'Ningalbandi': {'lat': 15.671063605028287, 'lon': 76.13794513593994},
        'Badimnhal': {'lat': 15.839823262484467, 'lon': 75.95503149946924},
        'Venkatapur': {'lat': 15.858511392991407, 'lon': 75.97308023163832},
        'Garjanhal': {'lat': 15.833697603912572, 'lon': 76.41468762354576},
        'Teggihal': {'lat': 15.849556310249351, 'lon': 76.27912911541603},
        'Mallapur': {'lat': 15.3933, 'lon': 76.4867},
        'Rampura': {'lat': 15.3822, 'lon': 76.4816},
        'Hagedal': {'lat': 15.590418925207551,  'lon':76.59839346965396},
        'Basrihal': {'lat': 15.595505073968516, 'lon':76.38104641401482},
        'Chikka Madinal': {'lat': 15.523496092485985, 'lon': 76.3778821765826},
        'Wadganhal': {'lat': 15.349168758650613, 'lon': 76.0804548913306},
        'Hirebommanahal': {'lat': 15.597423828789088 , 'lon': 76.2735258247831},
        'Hiresulikeri': {'lat': 15.52797030965004,'lon':  76.26075289964011 },
        'Jinnapur': {'lat': 15.490613192523476,'lon':  76.25717388261322},
        'Belgatti': {'lat': 15.213735760897155, 'lon': 75.9243389399449 },
        'Kawaloor': {'lat': 15.296976608396339, 'lon': 75.93461733961688},
        'Kesoor': {'lat': 15.872788521335098, 'lon': 76.19874347785046 },
        'Gangawati (CMC+OG) WARD No- 0005': {'lat': 15.424340577107621, 'lon': 76.53100417165172},
        'Gangawati (CMC+OG) WARD No- 0009': {'lat': 15.4280, 'lon': 76.5250},
        'Gangawati (CMC+OG) WARD No- 0015': {'lat': 15.4330, 'lon': 76.5350},
        'Koppal (CMC) WARD No-0008': {'lat': 15.3530, 'lon': 76.1580},
        'Koppal (CMC) WARD No-0021': {'lat': 15.3480, 'lon': 76.1520},
        'Koppal (CMC) WARD No-0001': {'lat': 15.3550, 'lon': 76.1500}
    }

# Theme Configurations
THEME_CONFIG = {
    "dark": {
        "plotly": "plotly_dark",
        "mapbox": "carto-darkmatter",
        "grid": "rgba(255,255,255,0.05)",
        "tick": "#94a3b8",
        "text": "#f8fafc",
        "hover_bg": "#1e293b",
        "hover_text": "#f8fafc",
        "legend_bg": "rgba(15, 23, 42, 0.6)",
        "table_header_bg": "rgba(99, 102, 241, 0.1)",
        "table_header_text": "#818cf8",
        "table_cell_bg": "#1e293b"
    },
    "light": {
        "plotly": "plotly_white",
        "mapbox": "carto-positron",
        "grid": "rgba(0,0,0,0.05)",
        "tick": "#475569",
        "text": "#0f172a",
        "hover_bg": "#ffffff",
        "hover_text": "#0f172a",
        "legend_bg": "rgba(255, 255, 255, 0.7)",
        "table_header_bg": "rgba(99, 102, 241, 0.05)",
        "table_header_text": "#4f46e5",
        "table_cell_bg": "#ffffff"
    }
}

def create_map(df, theme="dark"):
    t = THEME_CONFIG.get(theme, THEME_CONFIG["dark"])
    fig = go.Figure()
    
    # Defaults
    default_lat = 15.6
    default_lon = 76.15
    default_zoom = 8.3
    
    if df.empty:
        fig.add_annotation(text="No data available", showarrow=False)
        fig.update_layout(
             paper_bgcolor="rgba(0,0,0,0)",
             plot_bgcolor="rgba(0,0,0,0)",
             xaxis=dict(visible=False),
             yaxis=dict(visible=False)
        )
        return fig
        
    coords = area_coordinates()
    df = df.copy()
    if "PSU Name" in df.columns:
        df["lat"] = df["PSU Name"].astype(str).str.strip().map(lambda x: coords.get(x, {}).get("lat"))
        df["lon"] = df["PSU Name"].astype(str).str.strip().map(lambda x: coords.get(x, {}).get("lon"))
    else:
        df["lat"] = None
        df["lon"] = None
    map_df = df.dropna(subset=["lat", "lon"])
    
    # Calculate Center and Zoom
    if not map_df.empty:
        center_lat = map_df["lat"].mean()
        center_lon = map_df["lon"].mean()
        
        # Determine zoom based on spread
        lat_min, lat_max = map_df["lat"].min(), map_df["lat"].max()
        lon_min, lon_max = map_df["lon"].min(), map_df["lon"].max()
        
        lat_diff = lat_max - lat_min
        lon_diff = lon_max - lon_min
        max_diff = max(lat_diff, lon_diff)
        
        if max_diff < 0.01: # Single Point or very close
            zoom = 12
        elif max_diff < 0.1:
            zoom = 10.5
        elif max_diff < 0.5:
            zoom = 9.5
        else:
            zoom = 8.5
            
        # Construct a uirevision key based on the unique locations
        # This ensures camera resets if the set of locations changes, but preserves if just theme changes
        unique_psus = sorted(map_df["PSU Name"].unique().tolist())
        ui_rev = f"{len(unique_psus)}_{unique_psus[0] if unique_psus else ''}_{unique_psus[-1] if unique_psus else ''}"
    else:
        center_lat = default_lat
        center_lon = default_lon
        zoom = default_zoom
        ui_rev = "empty"
    
    # Calculate counts per PSU and Beneficiary
    psu_counts = map_df.groupby("PSU Name").size().to_dict() if not map_df.empty else {}
    benif_breakdown = map_df.groupby(["PSU Name", "Benificiery"]).size().unstack(fill_value=0).to_dict('index') if not map_df.empty else {}
    
    # --- Spiderification Logic (Jittering) ---
    # Add small deterministic offsets so overlapping subjects become visible on zoom
    if not map_df.empty:
        import numpy as np
        # Group by coordinates and add spread
        for (lat, lon), group in map_df.groupby(["lat", "lon"]):
            if len(group) > 1:
                # Deterministic jitter based on index
                indices = np.arange(len(group))
                # Spiral or random spread around the center
                angle = indices * (2 * np.pi / len(group))
                radius = 0.00015 * np.sqrt(indices) # Tiny offset in degrees (~15m)
                map_df.loc[group.index, "lat"] += radius * np.cos(angle)
                map_df.loc[group.index, "lon"] += radius * np.sin(angle)

    # All defined villages with their count (default 0)
    village_status = []
    for v_name, v_coord in coords.items():
        count = psu_counts.get(v_name, 0)
        breakdown_dict = benif_breakdown.get(v_name, {})
        # Create a formatted string for the tooltip
        breakdown_str = "<br>".join([f"• {k}: {v}" for k, v in breakdown_dict.items() if v > 0])
        if not breakdown_str:
            breakdown_str = "No data"
            
        status = "No Data" if count == 0 else ("In Progress" if count < 48 else "Complete")
        color = "#922b21" if count == 0 else ("#e67e22" if count < 48 else "#27ae60")
        village_status.append({
            "name": v_name, "lat": v_coord["lat"], "lon": v_coord["lon"],
            "count": count, "status": status, "color": color, "breakdown": breakdown_str
        })
    status_df = pd.DataFrame(village_status)

    # Always try to draw the boundary
    try:
        with open("koppal_district_official.geojson", "r") as f:
            geojson_data = json.load(f)
        fig.add_trace(go.Choroplethmap(
            geojson=geojson_data, locations=["Koppal"], featureidkey="properties.district",
            z=[1], colorscale=[[0, "rgba(52, 152, 219, 0.1)"], [1, "rgba(52, 152, 219, 0.1)"]],
            marker_line_width=2, marker_line_color="#2980b9", marker_opacity=0.5,
            showscale=False, name="Study Area Boundary", hoverinfo="name"
        ))
    except Exception as e:
        print(f"DEBUG: Could not load GeoJSON boundary: {e}")

    # Add Heatmap for Anemia Cases (High-Risk Focus: Moderate + Severe)
    heat_df = map_df[map_df["anemia_category"].str.lower().isin(["moderate", "severe"])].copy()
    if not heat_df.empty:
        # Weight Severe cases (3) higher than Moderate (1) for heat intensity
        heat_df["weight"] = heat_df["anemia_category"].str.lower().map({"severe": 3, "moderate": 1})
        
        fig.add_trace(go.Densitymap(
            lat=heat_df["lat"], lon=heat_df["lon"],
            z=heat_df["weight"], 
            radius=20,
            colorscale='Magma', 
            showscale=False,
            name="Anemia Hotspots",
            hoverinfo='skip',
            opacity=0.6
        ))

    # Add Progress-based Markers (Three Groups)
    categories = [
        {"name": "No Data Collected", "color": "#922b21", "filter": status_df["count"] == 0},
        {"name": "In Progress (1-47)", "color": "#e67e22", "filter": (status_df["count"] > 0) & (status_df["count"] < 48)},
        {"name": "Complete (48+ Samples)", "color": "#27ae60", "filter": status_df["count"] >= 48}
    ]
    
    for cat in categories:
        d_cat = status_df[cat["filter"]]
        if not d_cat.empty:
            fig.add_trace(go.Scattermap(
                lat=d_cat["lat"], lon=d_cat["lon"], mode="markers+text",
                marker=dict(size=14, color=cat["color"], opacity=0.9),
                name=cat["name"],
                text=d_cat["name"],
                textfont=dict(size=10, color="#2c3e50", family="-apple-system, BlinkMacSystemFont, sans-serif"),
                textposition="top center",
                hovertemplate='<b>%{text}</b><br>Total Samples: %{customdata[0]}<br>Status: %{customdata[1]}<br><br><b>Beneficiary Breakdown:</b><br>%{customdata[2]}<extra></extra>',
                customdata=d_cat[["count", "status", "breakdown"]].values
            ))
    
    fig.update_layout(
        map=dict(
            style=t["mapbox"],
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, bgcolor=t["legend_bg"], font=dict(color=t["tick"])),
        hoverlabel=dict(bgcolor=t["hover_bg"], font_size=12, font_family="var(--font-family)", font_color=t["hover_text"]),
        uirevision=ui_rev, # Reset view only when data context changes
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)"
    )
    return fig

def create_treat_map(df, theme="dark"):
    t = THEME_CONFIG.get(theme, THEME_CONFIG["dark"])
    fig = go.Figure()
    
    default_lat = 15.6
    default_lon = 76.15
    default_zoom = 8.3

    if df.empty:
        fig.add_annotation(text="No data available", showarrow=False)
        fig.update_layout(
             paper_bgcolor="rgba(0,0,0,0)",
             plot_bgcolor="rgba(0,0,0,0)",
             xaxis=dict(visible=False),
             yaxis=dict(visible=False)
        )
        return fig
        
    coords = area_coordinates()
    df = df.copy()
    if "PSU Name" in df.columns:
        df["lat"] = df["PSU Name"].astype(str).str.strip().map(lambda x: coords.get(x, {}).get("lat"))
        df["lon"] = df["PSU Name"].astype(str).str.strip().map(lambda x: coords.get(x, {}).get("lon"))
    
    map_df = df.dropna(subset=["lat", "lon"])
    
    # Calculate Center and Zoom
    if not map_df.empty:
        center_lat = map_df["lat"].mean()
        center_lon = map_df["lon"].mean()
        
        lat_min, lat_max = map_df["lat"].min(), map_df["lat"].max()
        lon_min, lon_max = map_df["lon"].min(), map_df["lon"].max()
        
        lat_diff = lat_max - lat_min
        lon_diff = lon_max - lon_min
        max_diff = max(lat_diff, lon_diff)
        
        if max_diff < 0.01:
            zoom = 12
        elif max_diff < 0.1:
            zoom = 10.5
        elif max_diff < 0.5:
            zoom = 9.5
        else:
            zoom = 8.5
            
        unique_psus = sorted(map_df["PSU Name"].unique().tolist())
        ui_rev = f"{len(unique_psus)}_{unique_psus[0] if unique_psus else ''}_{unique_psus[-1] if unique_psus else ''}"
    else:
        center_lat = default_lat
        center_lon = default_lon
        zoom = default_zoom
        ui_rev = "empty"
    
    # Calculate counts per PSU for treatment focus
    # We need: Asha Worker names, and Anemic counts (Mild, Moderate, Severe)
    treat_data = []
    
    # PSU-wise aggregates
    for psu_name, psu_group in map_df.groupby("PSU Name"):
        ashas = ", ".join(psu_group["Asha_Worker"].dropna().unique()) if "Asha_Worker" in psu_group.columns else "Missing"
        
        # Anemia breakdown
        counts = psu_group["anemia_category"].str.lower().value_counts()
        mild = counts.get("mild", 0)
        moderate = counts.get("moderate", 0)
        severe = counts.get("severe", 0)
        total_anemic = mild + moderate + severe
        
        if total_anemic > 0:
            color = "#ef4444" if severe > 0 else ("#f97316" if moderate > 0 else "#f59e0b")
            status = f"<b>{total_anemic}</b> Anemic"
        else:
            color = "#10b981"
            status = "No Anemia"
            
        hover_text = (
            f"<b>{psu_name}</b><br><br>"
            f"Asha Worker: <b>{ashas}</b><br><br>"
            f"<b>Anemia Breakdown:</b><br>"
            f"• Severe: <b>{severe}</b><br>"
            f"• Moderate: <b>{moderate}</b><br>"
            f"• Mild: <b>{mild}</b><br>"
            f"• Normal: <b>{counts.get('normal', 0)}</b>"
        )
        
        v_coord = coords.get(psu_name, {})
        if v_coord:
            # Spiderification for Treat Map: spread subjects slightly
            # In treat map, we usually show PSU level, but if we wanted subject level, we'd do it differently.
            # Keeping PSU level for Treat Map as requested for "Asha Level Focus", 
            # but adding Jitter to the base data if we decide to show individual points.
            treat_data.append({
                "name": psu_name, "lat": v_coord["lat"], "lon": v_coord["lon"],
                "color": color, "hover": hover_text, "size": 12 + (total_anemic * 0.5)
            })

    if treat_data:
        t_df = pd.DataFrame(treat_data)
        fig.add_trace(go.Scattermap(
            lat=t_df["lat"], lon=t_df["lon"], mode="markers",
            marker=dict(size=t_df["size"], color=t_df["color"], opacity=0.8),
            name="Urgent PSUs",
            text=t_df["name"],
            hovertemplate="%{customdata}<extra></extra>",
            customdata=t_df["hover"]
        ))

    # Add Geospatial boundary
    try:
        with open("koppal_district_official.geojson", "r") as f:
            geojson_data = json.load(f)
        fig.add_trace(go.Choroplethmap(
            geojson=geojson_data, locations=["Koppal"], featureidkey="properties.district",
            z=[1], colorscale=[[0, "rgba(52, 152, 219, 0.1)"], [1, "rgba(52, 152, 219, 0.1)"]],
            marker_line_width=2, marker_line_color="#2980b9", marker_opacity=0.5,
            showscale=False, name="Boundary", hoverinfo="skip"
        ))
    except: pass

    fig.update_layout(
        map=dict(
            style=t["mapbox"],
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        uirevision=ui_rev,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)"
    )
    return fig

def get_treat_layout():
    return html.Div([
        # Sidebar with Filters (Consistent with Dashboard)
        html.Div([
            # Tile 1: Project Info & Filters
            html.Div([
                html.Div([
                    html.P("Real-time Anaemia Monitoring Dashboard", 
                           style={"fontSize": "0.75rem", "fontWeight": "700", "color": "var(--text-main)", "margin": "0", "letterSpacing": "0.05em", "textTransform": "uppercase"}),
                    html.P("Koppal, Karnataka", 
                           style={"fontSize": "0.7rem", "color": "var(--text-muted)", "margin": "2px 0 0 0"})
                ], style={"padding": "0 0 15px 0", "marginBottom": "15px", "borderBottom": "1px solid var(--glass-border)"}),
                
                # Mobile Specific Extras (Nav & Theme) - Visible only on Mobile
                html.Div([
                    html.Div([
                        html.P("Navigation", className="sidebar-label"),
                        dbc.Nav([
                            dbc.NavLink("Test", href="/", active="exact", className="mobile-nav-link"),
                            dbc.NavLink("Treat", href="/treat", active="exact", className="mobile-nav-link"),
                            dbc.NavLink("Track", href="/track", active="exact", className="mobile-nav-link"),
                        ], vertical=True, pills=True),
                    ], className="filter-group"),
                    
                    html.Div([
                         html.P("Theme", className="sidebar-label"),
                         html.Div([
                            html.I(className="fas fa-sun"),
                            html.Span("Toggle Theme", style={"marginLeft": "10px"})
                         ], id="theme-toggle-mobile", className="theme-toggle-btn-mobile")
                    ], className="filter-group"),
                ], className="sidebar-tile mobile-only-extras"),

                html.Div([
                    html.Label("Block Code", className="sidebar-label"),
                    dcc.Dropdown(id="block-code-dropdown", options=[], multi=True, value=[], placeholder="All Blocks"),
                ], className="filter-group"),

                html.Div([
                    html.Label("Locality", className="sidebar-label"),
                    dcc.Dropdown(id="location-dropdown", options=[], multi=True, value=[], placeholder="All Locations"),
                ], className="filter-group"),
                
                html.Div([
                    html.Label("Beneficiary Type", className="sidebar-label"),
                    dcc.Dropdown(id="benificiery-dropdown", options=[], multi=True, value=[], placeholder="All Beneficiaries"),
                ], className="filter-group"),

                html.Div([
                    html.Label("Anemia Filter", className="sidebar-label"),
                    dcc.Dropdown(id="anemia-dropdown", options=[{"label": x.capitalize(), "value": x} for x in anemia_list], multi=True, value=[], placeholder="All Categories", className="anemia-dropdown"),
                ], className="filter-group"),

                dbc.Button("Clear All Filters", id="btn-clear", color="secondary", outline=True, size="sm", className="w-100"),
            ], className="sidebar-tile"),

            # Tile 2: Urgent Alerts
            html.Div([
                html.Div([
                    html.Label("Urgent Follow-up Subjects", className="sidebar-label", style={"color": "#ef4444"}),
                    html.Div(id="urgent-alerts-list", className="urgent-list"),
                ], className="filter-group", id="urgent-section", style={"marginBottom": "0"}),
            ], className="sidebar-tile", style={"backgroundColor": "rgba(239, 68, 68, 0.05)"}),
        ], id="sidebar", className="sidebar"),

        # Main Content
        html.Div([
            # KPI Row for Treat Page (Moved to Top)
            dbc.Row([
                # KPI Section (Styled to match Test page)
                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-users kpi-icon"), html.P("Total Enrollment", className="kpi-label")], className="kpi-header"),
                    html.H3(id="total", className="kpi-value")
                ], className="kpi-card"), xs=6, sm=4, md=True),

                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-exclamation-triangle kpi-icon", style={"color": "#ef4444"}), html.P("Severe Anemia", className="kpi-label")], className="kpi-header"),
                    html.H3(id="severe-count", className="kpi-value")
                ], className="kpi-card"), xs=6, sm=4, md=True),
                
                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-exclamation-circle kpi-icon", style={"color": "#f97316"}), html.P("Moderate Anemia", className="kpi-label")], className="kpi-header"),
                    html.H3(id="moderate-count", className="kpi-value")
                ], className="kpi-card"), xs=6, sm=4, md=True),

                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-circle-exclamation kpi-icon", style={"color": "#f59e0b"}), html.P("Mild Anemia", className="kpi-label")], className="kpi-header"),
                    html.H3(id="mild-count", className="kpi-value")
                ], className="kpi-card"), xs=6, sm=4, md=True),
                
                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-droplet kpi-icon", style={"color": "#991b1b"}), html.P("Avg Hb (g/dL)", className="kpi-label")], className="kpi-header"),
                    html.H3(id="avg-hgb", className="kpi-value")
                ], className="kpi-card"), xs=6, sm=4, md=True),
            ], className="mb-4 g-3"),

            # Geospatial Row with Bulk Notify Button
            dbc.Row([
                dbc.Col(html.H4("Geospatial High-Risk Distribution", style={"fontWeight": "700"}), width=True),
                # dbc.Col(
                #     dbc.Button([html.I(className="fas fa-paper-plane me-2"), "Bulk Notify Ashas"], 
                #                id="btn-bulk-notify", color="primary", size="sm", 
                #                className="shadow-sm", style={"borderRadius": "8px"}),
                #     width="auto"
                # )
            ], className="mb-3 align-items-center"),

            html.Div([
                html.P("Hover over markers to see assigned Asha Workers and beneficiary breakdown.", style={"color": "#64748b", "fontSize": "0.9rem"}),
                dcc.Graph(id="map", config={"responsive": True}, style=MAP_CARD_STYLE),
            ], className="graph-card", style={"padding": "30px", "marginBottom": "24px"}),
            
            # Notification Queue Section
            # html.Div(id="notification-queue-container", style={"marginTop": "30px"}),

            # Other required charts
            # Redundant hidden charts removed - now handled by get_shared_placeholders

                html.Div([
                    html.H4([html.I(className="fas fa-calendar-alt me-2", style={"color": "var(--primary-color)"}), "Supervisor Weekly Summaries"], 
                           style={"marginBottom": "10px", "fontWeight": "700", "color": "var(--text-main)"}),
                    html.P("Automated summaries grouped by Asha worker for quick distribution.", style={"fontSize": "0.85rem", "color": "var(--text-muted)"}),
                ], className="mb-4"),
                html.Div(id="weekly-summary-container")
            ], className="graph-card", style={"padding": "30px", "marginBottom": "24px"}),

            # Anemia Tables Section
            html.Div([
                html.Div([
                    html.H4([html.I(className="fas fa-exclamation-triangle me-2", style={"color": "var(--cat-severe-text)"}), "Severe Anemia Beneficiaries"], 
                           style={"marginBottom": "15px", "fontWeight": "700", "color": "var(--cat-severe-text)"}),
                    dash_table.DataTable(
                        id="severe-table",
                        style_table={"overflowX": "auto", "borderRadius": "12px", "overflow": "hidden", "border": "1px solid var(--cat-severe-bg)"},
                        style_cell={"padding": "12px", "textAlign": "left", "backgroundColor": "var(--table-cell-bg)", "color": "var(--table-cell-text)", "fontFamily": "var(--font-family)", "fontSize": "0.85rem"},
                        style_header={"backgroundColor": "var(--cat-severe-bg)", "fontWeight": "700", "color": "var(--cat-severe-text)", "borderBottom": "2px solid var(--cat-severe-text)"},
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'var(--icon-bg)'
                            },
                            {
                                'if': {'column_id': 'HGB'},
                                'color': 'var(--danger-color)',
                                'fontWeight': 'bold'
                            }
                        ],
                        page_size=15
                    )
                ], className="graph-card premium-table", style={"marginBottom": "30px", "borderLeft": "5px solid var(--cat-severe-text)"}),

                html.Div([
                    html.H4([html.I(className="fas fa-exclamation-circle me-2", style={"color": "var(--cat-moderate-text)"}), "Moderate Anemia Beneficiaries"], 
                           style={"marginBottom": "15px", "fontWeight": "700", "color": "var(--cat-moderate-text)"}),
                    dash_table.DataTable(
                        id="moderate-table",
                        # className removed
                        style_table={"overflowX": "auto", "borderRadius": "12px", "overflow": "hidden", "border": "1px solid var(--cat-moderate-bg)"},
                        style_cell={"padding": "12px", "textAlign": "left", "backgroundColor": "var(--table-cell-bg)", "color": "var(--table-cell-text)", "fontFamily": "var(--font-family)", "fontSize": "0.85rem"},
                        style_header={"backgroundColor": "var(--cat-moderate-bg)", "fontWeight": "700", "color": "var(--cat-moderate-text)", "borderBottom": "2px solid var(--cat-moderate-text)"},
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'var(--icon-bg)'
                            }
                        ],
                        page_size=15
                    )
                ], className="graph-card premium-table", style={"marginBottom": "30px", "borderLeft": "5px solid var(--cat-moderate-text)"}),

                html.Div([
                    html.H4([html.I(className="fas fa-circle-exclamation me-2", style={"color": "var(--cat-mild-text)"}), "Mild Anemia Beneficiaries"], 
                           style={"marginBottom": "15px", "fontWeight": "700", "color": "var(--cat-mild-text)"}),
                    dash_table.DataTable(
                        id="mild-table",
                        # className removed
                        style_table={"overflowX": "auto", "borderRadius": "12px", "overflow": "hidden", "border": "1px solid var(--cat-mild-bg)"},
                        style_cell={"padding": "12px", "textAlign": "left", "backgroundColor": "var(--table-cell-bg)", "color": "var(--table-cell-text)", "fontFamily": "var(--font-family)", "fontSize": "0.85rem"},
                        style_header={"backgroundColor": "var(--cat-mild-bg)", "fontWeight": "700", "color": "var(--cat-mild-text)", "borderBottom": "2px solid var(--cat-mild-text)"},
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': 'var(--icon-bg)'
                            }
                        ],
                        page_size=15
                    )
                ], className="graph-card premium-table", style={"marginBottom": "30px", "borderLeft": "5px solid var(--cat-mild-text)"}),
            ], style={"marginTop": "20px"}),

            html.Div([
                html.H5("Detailed Records", className="graph-title"),
                dash_table.DataTable(id="table", style_header={"display": "none"})
            ], style={"display": "none"}),
            
            get_footer(),
            
            # Shared placeholders for Dashboard components (Exclude what Treat page HAS)
            *get_shared_placeholders([
                "block-code-dropdown", "location-dropdown", "benificiery-dropdown", "anemia-dropdown", "btn-clear",
                "urgent-alerts-list", "total", "severe-count", "moderate-count", "mild-count", "avg-hgb", "map", 
                "severe-table", "moderate-table", "mild-table", "table", "weekly-summary-container",
                "theme-toggle-mobile"
            ])
        ], id="main-content", className="main-content")

def get_track_layout():
    return html.Div([
        # Main Content Centered
        html.Div([
            html.Div([
                html.H1("Track Page", style={"fontWeight": "800", "fontSize": "2.5rem", "marginBottom": "15px", "color": "var(--text-main)"}),
                html.P("Real-time Tracking & Longitudinal Analysis", 
                       style={"fontSize": "1.1rem", "color": "var(--text-muted)", "marginBottom": "30px"}),
                
                html.Div([
                    html.I(className="fas fa-tools", style={"fontSize": "3rem", "color": "var(--primary-color)", "marginBottom": "20px"}),
                    html.H5("Feature Under Development", style={"fontWeight": "700", "color": "var(--text-main)"}),
                    html.P("We are working hard to bring you longitudinal tracking and advanced predictive analytics for anemia management.", 
                           style={"maxWidth": "400px", "margin": "0 auto 30px auto", "color": "var(--text-muted)"}),
                    
                    dbc.Button([html.I(className="fas fa-arrow-left me-2"), "Back to Dashboard"], 
                               href="/", color="primary", className="px-4 shadow-sm", 
                               style={"borderRadius": "10px", "fontWeight": "600"})
                ], className="graph-card", style={"padding": "60px 40px", "textAlign": "center", "maxWidth": "600px", "width": "100%"}),
            ], style={"minHeight": "70vh", "display": "flex", "flexDirection": "column", "alignItems": "center", "justifyContent": "center"}),
            
            get_footer(),
            
            # Shared placeholders (Exclude Nav and Theme toggle)
            *get_shared_placeholders([])
        ], id="main-content", className="main-content", 
        style={"marginLeft": "0", "width": "100%", "padding": "0 20px"})
    ], id="track-layout-container")

def get_footer():
    return html.Footer([
        html.Hr(style={"margin": "60px 0 30px 0", "opacity": "0.05"}),
        html.Div([
            html.P([
                "Copyright © 2026 ICMR CAR MEDTECH LAB, St John's Research Institute, Bangalore"
            ], style={"textAlign": "center", "color": "var(--primary-color)", "opacity": "0.9", "fontWeight": "600", "marginTop": "20px", "fontSize": "0.9rem"})
        ], className="footer-content")
    ], className="dashboard-footer")

# Custom styles for the layout
CARD_STYLE = {"height": "350px"}
MAP_CARD_STYLE = {"height": "645px"}

def get_shared_placeholders(exclude_list):
    """
    Returns a flat list of hidden placeholders for shared IDs to prevent Dash callback errors.
    If an ID is already present in the visible page layout, it should be passed in exclude_list.
    """
    # Ensure components that don't support children (like Input/State/Graph) are handled correctly
    all_outputs = {
        "total": html.Div(id="total", style={"display": "none"}),
        "normal-count": html.Div(id="normal-count", style={"display": "none"}),
        "moderate-count": html.Div(id="moderate-count", style={"display": "none"}),
        "severe-count": html.Div(id="severe-count", style={"display": "none"}),
        "mild-count": html.Div(id="mild-count", style={"display": "none"}),
        "avg-hgb": html.Div(id="avg-hgb", style={"display": "none"}),
        "diet-count": html.Div(id="diet-count", style={"display": "none"}),
        "prevalence-val": html.Div(id="prevalence-val", style={"display": "none"}),
        "map": dcc.Graph(id="map", style={"display": "none"}),
        "benificiery-bar": dcc.Graph(id="benificiery-bar", style={"display": "none"}),
        "anemia-pie": dcc.Graph(id="anemia-pie", style={"display": "none"}),
        "anemia-village-bar": dcc.Graph(id="anemia-village-bar", style={"display": "none"}),
        "block-anemia-bar": dcc.Graph(id="block-anemia-bar", style={"display": "none"}),
        "block-prevalence-bar": dcc.Graph(id="block-prevalence-bar", style={"display": "none"}),
        "hgb-stats-bar": dcc.Graph(id="hgb-stats-bar", style={"display": "none"}),
        "bmi-bar": dcc.Graph(id="bmi-bar", style={"display": "none"}),
        "table": dash_table.DataTable(id="table", style_header={"display": "none"}, style_cell={"display": "none"}),
        "block-code-dropdown": dcc.Dropdown(id="block-code-dropdown", style={"display": "none"}),
        "location-dropdown": dcc.Dropdown(id="location-dropdown", style={"display": "none"}),
        "benificiery-dropdown": dcc.Dropdown(id="benificiery-dropdown", style={"display": "none"}),
        "anemia-dropdown": dcc.Dropdown(id="anemia-dropdown", style={"display": "none"}),
        "urgent-alerts-list": html.Div(id="urgent-alerts-list", style={"display": "none"}),
        "severe-table": dash_table.DataTable(id="severe-table", style_header={"display": "none"}, style_cell={"display": "none"}),
        "moderate-table": dash_table.DataTable(id="moderate-table", style_header={"display": "none"}, style_cell={"display": "none"}),
        "mild-table": dash_table.DataTable(id="mild-table", style_header={"display": "none"}, style_cell={"display": "none"}),
        "notification-queue-container": html.Div(id="notification-queue-container", style={"display": "none"}),
        "weekly-summary-container": html.Div(id="weekly-summary-container", style={"display": "none"}),
        "btn-clear": dbc.Button(id="btn-clear", style={"display": "none"}),
        "btn-excel": dbc.Button(id="btn-excel", style={"display": "none"}),
        "btn-csv": dbc.Button(id="btn-csv", style={"display": "none"}),
        "theme-toggle-mobile": html.Div(id="theme-toggle-mobile", style={"display": "none"})
    }
    
    return [v for k, v in all_outputs.items() if k not in exclude_list]

def get_dashboard_layout():
    return html.Div([
        html.Div([
            # Tile 1: Filters
            html.Div([
                # Sidebar Header (Context Label)
                html.Div([
                    html.P("Real-time Anaemia Monitoring Dashboard", 
                           style={"fontSize": "0.75rem", "fontWeight": "700", "color": "var(--text-main)", "margin": "0", "letterSpacing": "0.05em", "textTransform": "uppercase"}),
                    html.P("Koppal, Karnataka", 
                           style={"fontSize": "0.7rem", "color": "var(--text-muted)", "margin": "2px 0 0 0"})
                ], style={"padding": "0 0 15px 0", "marginBottom": "15px", "borderBottom": "1px solid var(--glass-border)"}),
                
                # Mobile Specific Extras (Nav & Theme) - Visible only on Mobile
                html.Div([
                    html.Div([
                        html.P("Navigation", className="sidebar-label"),
                        dbc.Nav([
                            dbc.NavLink("Test", href="/", active="exact", className="mobile-nav-link"),
                            dbc.NavLink("Treat", href="/treat", active="exact", className="mobile-nav-link"),
                            dbc.NavLink("Track", href="/track", active="exact", className="mobile-nav-link"),
                        ], vertical=True, pills=True),
                    ], className="filter-group"),
                    
                    html.Div([
                         html.P("Theme", className="sidebar-label"),
                         html.Div([
                            html.I(className="fas fa-sun"),
                            html.Span("Toggle Theme", style={"marginLeft": "10px"})
                         ], id="theme-toggle-mobile", className="theme-toggle-btn-mobile")
                    ], className="filter-group"),
                ], className="sidebar-tile mobile-only-extras"),

                # Location Selection (Always Visible)
                html.Div([
                    html.Label("Block Code", className="sidebar-label"),
                    dcc.Dropdown(id="block-code-dropdown", options=[], multi=True, value=[], placeholder="All Blocks"),
                ], className="filter-group"),

                html.Div([
                    html.Label("Location Selection", className="sidebar-label"),
                    dcc.Dropdown(id="location-dropdown", options=[], multi=True, value=[], placeholder="All Locations"),
                ], className="filter-group"),
                
                # Filter Groups
                html.Div([
                    html.Label("Beneficiary Type", className="sidebar-label"),
                    dcc.Dropdown(id="benificiery-dropdown", options=[], multi=True, value=[], placeholder="All Beneficiaries"),
                ], className="filter-group"),
                
                html.Div([
                    html.Label("Anemia Status", className="sidebar-label"),
                    dcc.Dropdown(id="anemia-dropdown", options=[{"label": x.capitalize(), "value": x} for x in anemia_list], multi=True, value=[], placeholder="All Categories", className="anemia-dropdown"),
                ], className="filter-group"),

                dbc.Button("Clear All Filters", 
                           id="btn-clear", color="secondary", outline=True, size="sm", 
                           className="w-100", style={"fontSize": "0.75rem", "borderRadius": "8px"}),
            ], className="sidebar-tile"),
            
            # Tile 2: Management & Status
            html.Div([
                html.Div([
                    html.Label("Management Tools", className="sidebar-label"),
                    dbc.ButtonGroup([
                        dbc.Button([html.I(className="fas fa-file-excel me-2"), "Excel"], id="btn-excel", color="success", outline=True, size="sm", style={"fontSize": "0.7rem"}),
                        dbc.Button([html.I(className="fas fa-file-csv me-2"), "CSV"], id="btn-csv", color="primary", outline=True, size="sm", style={"fontSize": "0.7rem"}),
                    ], className="w-100"),
                ], className="filter-group"),

                html.Div([
                    html.Div([
                        html.Div(className="status-dot"),
                        html.Span("Live Data Connection", style={"fontSize": "0.75rem", "fontWeight": "600", "marginLeft": "8px", "color": "var(--text-main)"})
                    ], className="status-badge", style={"background": "var(--glass-bg)", "padding": "8px 12px", "borderRadius": "10px", "display": "flex", "alignItems": "center", "border": "1px solid var(--glass-border)"})
                ])
            ], className="sidebar-tile")
        ], id="sidebar", className="sidebar"),
        
        # Main Content
        html.Div([
            # Main Dashboard Grid
            dbc.Row([
                # KPI Section (Moved up to top row since branding is now in fixed top bar)
                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-users kpi-icon"), html.P("Total Enrolled", className="kpi-label")], className="kpi-header"),
                    html.H3(id="total", className="kpi-value")
                ], className="kpi-card"), xs=6, sm=4, md=True),

                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-chart-line kpi-icon", style={"color": "#6366f1"}), html.P("Prevalence of Anemia", className="kpi-label")], className="kpi-header"),
                    html.H3(id="prevalence-val", className="kpi-value")
                ], className="kpi-card"), xs=6, sm=4, md=True),
                
                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-check-circle kpi-icon", style={"color": "#10b981"}), html.P("Normal", className="kpi-label")], className="kpi-header"),
                    html.H3(id="normal-count", className="kpi-value")
                ], className="kpi-card"), xs=6, sm=4, md=True),
                
                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-info-circle kpi-icon", style={"color": "#f59e0b"}), html.P("Mild", className="kpi-label")], className="kpi-header"),
                    html.H3(id="mild-count", className="kpi-value")
                ], className="kpi-card"), xs=6, sm=4, md=True),
                
                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-exclamation-circle kpi-icon", style={"color": "#f97316"}), html.P("Moderate", className="kpi-label")], className="kpi-header"),
                    html.H3(id="moderate-count", className="kpi-value")
                ], className="kpi-card"), xs=6, sm=4, md=True),
                
                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-exclamation-triangle kpi-icon", style={"color": "#ef4444"}), html.P("Severe", className="kpi-label")], className="kpi-header"),
                    html.H3(id="severe-count", className="kpi-value")
                ], className="kpi-card"), xs=6, sm=4, md=True),
                
                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-droplet kpi-icon", style={"color": "#991b1b"}), html.P("Avg Hb (g/dL)", className="kpi-label")], className="kpi-header"),
                    dcc.Loading(html.H3(id="avg-hgb", className="kpi-value"), type="dot", color="#991b1b")
                ], className="kpi-card"), xs=6, sm=4, md=True),
                
                dbc.Col(html.Div([
                    html.Div([html.I(className="fas fa-utensils kpi-icon", style={"color": "#8b5cf6"}), html.P("Dietary", className="kpi-label")], className="kpi-header"),
                    dcc.Loading(html.H3(id="diet-count", className="kpi-value"), type="dot", color="#8b5cf6")
                ], className="kpi-card"), xs=6, sm=4, md=True),
            ], className="mb-4 g-3"),


            
            # Grid Section
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("Geospatial Distribution", className="graph-title"),
                        dcc.Graph(id="map", config={"responsive": True, "displayModeBar": False}, style=MAP_CARD_STYLE),
                    ], className="graph-card")
                ], xs=12, xl=8),
                
                dbc.Col([
                    html.Div([
                        html.H5("Case Classification", className="graph-title"),
                        dcc.Graph(id="anemia-pie", config={"responsive": True, "displayModeBar": False}, style={"height": "265px"}),
                    ], className="graph-card", style={"marginBottom": "24px"}),
                    
                    html.Div([
                        html.H5("Beneficiary Distribution", className="graph-title"),
                        dcc.Graph(id="benificiery-bar", config={"responsive": True, "displayModeBar": False}, style={"height": "265px"}),
                    ], className="graph-card")
                ], xs=12, xl=4)
            ], className="mb-4 g-3"),
            


            # Block-wise Analysis Row
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("Block-wise Anemia Distribution", className="graph-title"),
                        dcc.Graph(id="block-anemia-bar", config={"responsive": True, "displayModeBar": False}, style={"height": "450px"}),
                    ], className="graph-card")
                ], xs=12, lg=6),
                
                dbc.Col([
                    html.Div([
                        html.H5("Block-wise Anemia Prevalence (%)", className="graph-title"),
                        dcc.Graph(id="block-prevalence-bar", config={"responsive": True, "displayModeBar": False}, style={"height": "450px"}),
                    ], className="graph-card")
                ], xs=12, lg=6),
            ], className="mb-4 g-3"),
            
            # PSU-wise Haemoglobin Analysis Row
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("PSU-wise Haemoglobin Analysis (Mean & SD)", className="graph-title"),
                        dcc.Graph(id="hgb-stats-bar", config={"responsive": True, "displayModeBar": False}, style={"height": "450px"}),
                    ], className="graph-card")
                ], xs=12),
            ], className="mb-4 g-3"),

            # Geospatial/Demographic Row (Renamed for clarity as Anemia Village is here now)
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("PSU-wise Anemia Classification", className="graph-title"),
                        dcc.Graph(id="anemia-village-bar", config={"responsive": True, "displayModeBar": False}, style={"height": "450px"}),
                    ], className="graph-card")
                ], xs=12),
            ], className="mb-4 g-3"),

            # Nutritional Status Analysis (Full Width)
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("Nutritional Status Analysis (BMI Distribution)", className="graph-title"),
                        dcc.Graph(id="bmi-bar", config={"responsive": True, "displayModeBar": False}, style={"height": "450px"}),
                    ], className="graph-card")
                ], xs=12),
            ], className="mb-4 g-3"),

            # Table Section
            html.Div([
                html.H5("Detailed Beneficiary Records", className="graph-title"),
                dash_table.DataTable(
                    id="table", page_size=15, filter_action="native", sort_action="native",
                    style_table={"overflowX": "auto", "minWidth": "100%"}, 
                    style_cell={"padding": "12px", "textAlign": "left", "backgroundColor": "var(--table-cell-bg)", "color": "var(--table-cell-text)", "border": "1px solid var(--table-border)", "fontFamily": "var(--font-family)", "fontSize": "0.85rem", "minWidth": "150px"},
                    style_header={"fontWeight": "700", "backgroundColor": "var(--table-header-bg)", "color": "var(--table-header-text)", "borderBottom": "2px solid var(--table-header-border)", "textTransform": "uppercase", "letterSpacing": "0.05em"},
                    fixed_rows={'headers': True},
                    style_data_conditional=[
                        {'if': {'filter_query': '{anemia_category} = "Normal"'}, 'backgroundColor': 'var(--cat-normal-bg)', 'color': 'var(--cat-normal-text)'},
                        {'if': {'filter_query': '{anemia_category} = "Mild"'}, 'backgroundColor': 'var(--cat-mild-bg)', 'color': 'var(--cat-mild-text)'},
                        {'if': {'filter_query': '{anemia_category} = "Moderate"'}, 'backgroundColor': 'var(--cat-moderate-bg)', 'color': 'var(--cat-moderate-text)'},
                        {'if': {'filter_query': '{anemia_category} = "Severe"'}, 'backgroundColor': 'var(--cat-severe-bg)', 'color': 'var(--cat-severe-text)'},
                        {'if': {'filter_query': '{anemia_category} = "Incomplete"'}, 'backgroundColor': 'var(--cat-incomplete-bg)', 'color': 'var(--cat-incomplete-text)'},
                    ]
                )
            ], className="graph-card"),
            
            get_footer(),
            
            # Shared placeholders for Treat Page components (Exclude what Dashboard page HAS)
            *get_shared_placeholders([
                "block-code-dropdown", "location-dropdown", "benificiery-dropdown", "anemia-dropdown", "btn-clear", "btn-excel", "btn-csv",
                "severe-count", "avg-hgb", "diet-count", "map", "benificiery-bar", "anemia-pie", 
                "anemia-village-bar", "block-anemia-bar", "block-prevalence-bar", "hgb-stats-bar", "bmi-bar", "table",
                "theme-toggle-mobile", "prevalence-val", "normal-count", "mild-count", "moderate-count", "total"
            ])
        ], id="main-content", className="main-content")
    ])


app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    dcc.Interval(id="interval", interval=60_000, n_intervals=0),
    dcc.Store(id="stored-data"),
    dcc.Download(id="download-data"),

    dcc.Store(id="theme-store", data="dark", storage_type="local"),
    dcc.Store(id="bulk-notification-urls"),
    dcc.Store(id="notification-queue-data", data=[], storage_type="local"),
    dcc.Store(id="reset-notification-trigger", data=0),
    html.Div(id="bulk-notification-trigger", style={"display": "none"}),
    html.Div(id="mobile-toggle-trigger", style={"display": "none"}),
    
    dbc.Toast(id="bulk-notify-toast", header="Notification", is_open=False,
              dismissable=True, icon="success", duration=5000, 
              style={"position": "fixed", "top": 66, "right": 10, "zIndex": 2000}),
    
    # Fixed Top Bar
    html.Nav([
        # Left Group: Brand & Nav (Stacked)
        html.Div([
            # Row 1: Logo & Title
            html.Div([
                html.Div([
                    html.Img(src="/assets/main_logo.svg", className="logo-img main-logo"),
                ], className="logo-container main-logo-container"),
                
                html.Div([
                    html.Span("PRAKASH", style={"fontWeight": "900", "fontSize": "1.4rem", "marginRight": "10px", "color": "var(--text-main)"}),
                    html.Span("AMB 2.0 T³", className="glowing-badge", style={"fontSize": "0.7rem", "padding": "2px 8px", "marginRight": "8px"}),
                    html.Span([html.I(className="fas fa-tag me-2"), "Baseline 1"], className="phase-badge")
                ], style={"display": "flex", "alignItems": "center"}),
            ], className="brand-row"),
            
            # Row 2: Nav Buttons
            html.Div(id="nav-buttons-container", className="nav-buttons", style={"marginTop": "5px"})
        ], className="header-left-col"),
        
        # Right Group: Tools & Partners
        html.Div([
            html.Div([
                html.I(className="fas fa-sun"),
            ], id="theme-toggle", className="theme-toggle-btn", style={"marginRight": "15px"}),
            
            html.Div([
                html.Img(src="/assets/images.png", className="logo-img partner-logo images-logo"),
                html.Img(src="/assets/government-of-karnataka.jpg", className="logo-img partner-logo gok-logo"),
                html.Img(src="/assets/khpt-logo.png", className="logo-img partner-logo khpt-logo"),
            ], className="partner-logo-group last-partner"),
        ], style={"display": "flex", "alignItems": "center"})
    ], className="top-bar"),

    # Mobile Header (Only visible on mobile)
    html.Div([
        dbc.Button(html.I(className="fas fa-bars"), id="btn-toggle", className="toggle-button"),
        html.Div([
            html.Span("PRAKASH", style={"fontWeight": "800", "fontSize": "1.1rem", "color": "var(--text-main)", "lineHeight": "1"}),
            html.Span("AMB 2.0 T³", className="glowing-badge", style={"fontSize": "0.6rem", "padding": "1px 6px", "marginTop": "2px"})
        ], className="mobile-brand-group"),
        html.Div([
            html.Img(src="/assets/main_logo.svg", className="mobile-logo main-mobile-logo"),
            html.Img(src="/assets/images.png", className="mobile-logo partner-mobile-logo"),
            html.Img(src="/assets/government-of-karnataka.jpg", className="mobile-logo gok-mobile-logo"),
            html.Img(src="/assets/khpt-logo.png", className="mobile-logo khpt-mobile-logo"),
        ], className="mobile-logo-container")
    ], className="mobile-nav"),

    # Page Content Container
    html.Div(id="page-content")
], id="main-container")

@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname")
)
def display_page(pathname):
    if pathname == "/track":
        return get_track_layout()
    elif pathname == "/treat":
        return get_treat_layout()
    else:
        # Default to the Main Dashboard (Now under 'Test' branding in Nav)
        return get_dashboard_layout()

@app.callback(
    Output("nav-buttons-container", "children"),
    Input("url", "pathname")
)
def update_nav_buttons(pathname):
    # Define buttons and their target routes
    buttons = [
        {"name": "Test", "href": "/"},
        {"name": "Treat", "href": "/treat"},
        {"name": "Track", "href": "/track"}
    ]
    
    nav_links = []
    for btn in buttons:
        is_active = pathname == btn["href"]
        # Special case for root
        if btn["href"] == "/" and pathname not in ["/treat", "/track"]:
            is_active = True
            
        full_class = "nav-btn nav-btn-standard"
        if is_active:
            full_class += " active"
            
        nav_links.append(dcc.Link(btn["name"], href=btn["href"], className=full_class))
        
    return nav_links


@app.callback(Output("stored-data", "data"), Input("interval", "n_intervals"))
def refresh_data(_):
    df, msg, is_err = load_data()
    
    # Automatically sync to sheets in a BACKGROUND THREAD to prevent blocking the UI
    if not is_err and not df.empty:
        threading.Thread(target=sync_data_to_sheets, args=(df,), daemon=True).start()
        
    return {
        "records": df.to_dict("records"),
        "status": msg,
        "is_error": is_err,
        "last_updated": datetime.now().strftime("%H:%M:%S")
    }

@app.callback(
    [
        Output("total", "children"), Output("normal-count", "children"),
        Output("moderate-count", "children"), Output("severe-count", "children"),
        Output("mild-count", "children"), Output("avg-hgb", "children"),
        Output("diet-count", "children"),
        Output("prevalence-val", "children"),
        Output("map", "figure"), Output("benificiery-bar", "figure"),
        Output("anemia-pie", "figure"), Output("anemia-village-bar", "figure"),
        Output("hgb-stats-bar", "figure"),
        Output("bmi-bar", "figure"),
        Output("block-anemia-bar", "figure"),
        Output("block-prevalence-bar", "figure"),
        Output("table", "data"), Output("table", "columns"),
        Output("block-code-dropdown", "options"), Output("location-dropdown", "options"),
        Output("benificiery-dropdown", "options"), Output("anemia-dropdown", "options"),
        Output("block-code-dropdown", "value"), Output("location-dropdown", "value"),
        Output("benificiery-dropdown", "value"), Output("anemia-dropdown", "value"),
        Output("urgent-alerts-list", "children"),
        Output("severe-table", "data"), Output("severe-table", "columns"),
        Output("moderate-table", "data"), Output("moderate-table", "columns"),
        Output("mild-table", "data"), Output("mild-table", "columns"),
        Output("weekly-summary-container", "children"),
    ],
    [
        Input("stored-data", "data"), Input("block-code-dropdown", "value"), Input("location-dropdown", "value"),
        Input("benificiery-dropdown", "value"),
        Input("anemia-dropdown", "value"), Input("interval", "n_intervals"),
        Input("map", "clickData"), Input("anemia-pie", "clickData"),
        Input("benificiery-bar", "clickData"), Input("btn-clear", "n_clicks"),
        Input("url", "pathname"), Input("reset-notification-trigger", "data"),
        Input("theme-store", "data")
    ]
)
def update_dashboard(stored_dict, block_code, location, benificiery, anemia, n_intervals, map_click, pie_click, bar_click, n_clear, pathname, reset_trigger, theme):
    try:
        return internal_update_dashboard(stored_dict, block_code, location, benificiery, anemia, n_intervals, map_click, pie_click, bar_click, n_clear, pathname, theme)
    except Exception as e:
        import traceback
        print(f"CRITICAL ERROR in update_dashboard: {str(e)}")
        print(traceback.format_exc())
        return [0]*8 + [go.Figure()]*8 + [[]]*18

def internal_update_dashboard(stored_dict, block_code, location, benificiery, anemia, n_intervals, map_click, pie_click, bar_click, n_clear, pathname, theme="dark"):
    t = THEME_CONFIG.get(theme, THEME_CONFIG["dark"])
    if not stored_dict or "records" not in stored_dict:
        # Return 30 elements to match the number of outputs
        return [0]*8 + [go.Figure()]*8 + [[]]*18
    
    records = stored_dict["records"]
    status_msg = stored_dict["status"]
    is_error = stored_dict["is_error"]
    last_upd = stored_dict.get("last_updated", "")

    if not records and is_error:
        # Return 30 elements
        return [0]*8 + [go.Figure()]*8 + [[]]*18

    df_full = pd.DataFrame(records)
    
    # DEDUPLICATION & CLEANING
    # Ensure One Record Per Component (ID) - Keep Latest
    if "ID" in df_full.columns and not df_full.empty:
        # 1. Convert Date for sorting
        if "Sample Collected Date" in df_full.columns:
            df_full["Sample Collected Date"] = pd.to_datetime(df_full["Sample Collected Date"], errors="coerce")
            df_full = df_full.sort_values(by="Sample Collected Date", ascending=True)
        
        # 2. Filter out rows with missing IDs (if any crept in)
        df_full = df_full[df_full["ID"].notna() & (df_full["ID"].astype(str).str.strip() != "")]
        
        # 3. Drop Duplicates - Keep Last (Latest)
        df_full = df_full.drop_duplicates(subset=["ID"], keep="last")
        
        # 4. Coerce numeric columns
        if "HGB" in df_full.columns:
            df_full["HGB"] = pd.to_numeric(df_full["HGB"], errors="coerce")
    
    # Count unique total for sanity check logging
    print(f"DEBUG: Total Unique Records after deduplication: {len(df_full)}")
    
    ctx = callback_context
    triggered_id = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None

    # EXTREME LOGGING: INPUTS
    print(f"\n>>> CALLBACK START: {triggered_id}")
    print(f">>> INPUT BLOCK: {block_code}")
    print(f">>> INPUT LOCATION: {location}")
    print(f">>> INPUT BENIF: {benificiery}")
    print(f">>> INPUT ANEMIA: {anemia}")
    
    # FORCED TYPE ENFORCEMENT
    block_code = [block_code] if isinstance(block_code, str) else (block_code or [])
    location = [location] if isinstance(location, str) else (location or [])
    benificiery = [benificiery] if isinstance(benificiery, str) else (benificiery or [])
    anemia = [anemia] if isinstance(anemia, str) else (anemia or [])
    
    # TRACE LOGGING

    # Handle Chart Interactions (Cross-Filtering)
    if triggered_id == "btn-clear":
        print("DEBUG: Clearing all filters via button.")
        block_code, location, benificiery, anemia = [], [], [], []
        
    elif triggered_id == "map" and map_click:
        village_clicked = map_click["points"][0].get("text")
        print(f"DEBUG: Map clicked on: {village_clicked}")
        if village_clicked and village_clicked in df_full["PSU Name"].values:
            # Find the full location string for this PSU
            loc_val = df_full[df_full["PSU Name"] == village_clicked]["Location"].iloc[0]
            if not location or loc_val not in location:
                location = [loc_val] 
                print(f"DEBUG: Location updated from map to: {location}")
            else:
                print("DEBUG: Location already contains this village, no change.")
            
    elif triggered_id == "anemia-pie" and pie_click:
        cat_clicked = pie_click["points"][0].get("label").lower()
        if cat_clicked:
            anemia = [cat_clicked]
            print(f"DEBUG: Anemia filter updated to: {anemia}")

    elif triggered_id == "benificiery-bar" and bar_click:
        benif_clicked = bar_click["points"][0].get("x")
        if benif_clicked:
            benificiery = [benif_clicked]
            print(f"DEBUG: Beneficiary filter updated to: {benificiery}")

    driver_triggers = ["stored-data", "interval"]
    # We will always update the dashboard components to ensure they stay in sync with filters
    is_full_update = True 

    # Dynamic Options (Cascading Filters)
    # 0. Block Code options: Filtered by others (less common to filter UP, but good for consistency)
    df_block = df_full.copy()
    if location: df_block = df_block[df_block["Location"].isin(location)]
    if benificiery: df_block = df_block[df_block["Benificiery"].isin(benificiery)]
    if anemia: df_block = df_block[df_block["anemia_category"].isin(anemia)]
    
    block_opts = []
    if "BlockCode" in df_block.columns:
        block_opts = [{"label": x, "value": x} for x in sorted(df_block["BlockCode"].dropna().unique()) if x != "Missing"]

    # 1. Location options: Filtered by Block, Benificiery, Anemia
    df_loc = df_full.copy()
    if block_code and "BlockCode" in df_loc.columns: 
        df_loc = df_loc[df_loc["BlockCode"].isin(block_code)]
    if benificiery: df_loc = df_loc[df_loc["Benificiery"].isin(benificiery)]
    if anemia: df_loc = df_loc[df_loc["anemia_category"].isin(anemia)]
    loc_opts = [{"label": x, "value": x} for x in sorted(df_loc["Location"].dropna().unique())]
    if not loc_opts:
        loc_opts = [{"label": "No Results Found", "value": "none", "disabled": True}]

    # Clean up Location selection if not in new options
    if location:
        valid_locs = [o["value"] for o in loc_opts]
        location = [l for l in location if l in valid_locs]

    # 2. Benificiery options: Filtered by Block, Location, Anemia
    df_benif = df_full.copy()
    if block_code and "BlockCode" in df_benif.columns: 
        df_benif = df_benif[df_benif["BlockCode"].isin(block_code)]
    if location: df_benif = df_benif[df_benif["Location"].isin(location)]
    if anemia: df_benif = df_benif[df_benif["anemia_category"].isin(anemia)]
    benif_opts = [{"label": x, "value": x} for x in sorted(df_benif["Benificiery"].dropna().unique())]
    if not benif_opts:
        benif_opts = [{"label": "No Results Found", "value": "none", "disabled": True}]

    # 3. Anemia options: Filtered by Block, Location, Benificiery
    df_anemia_opts = df_full.copy()
    if block_code and "BlockCode" in df_anemia_opts.columns: 
        df_anemia_opts = df_anemia_opts[df_anemia_opts["BlockCode"].isin(block_code)]
    if location: df_anemia_opts = df_anemia_opts[df_anemia_opts["Location"].isin(location)]
    if benificiery: df_anemia_opts = df_anemia_opts[df_anemia_opts["Benificiery"].isin(benificiery)]
    # Normalize anemia categories to capitalize for label
    anemia_opts_raw = sorted(df_anemia_opts["anemia_category"].dropna().unique())
    anemia_opts = [{"label": x.capitalize(), "value": x} for x in anemia_opts_raw]
    if not anemia_opts:
        anemia_opts = [{"label": "No Results Found", "value": "none", "disabled": True}]

    # Apply all final filters to the main df for stats/charts
    # Apply all final filters to the main df for stats/charts
    # AND for Total Enrollment (Now respecting BlockCode as per user request)
    df_total = df_full.copy()
    if block_code and "BlockCode" in df_total.columns: 
        df_total = df_total[df_total["BlockCode"].isin(block_code)]
    if location: df_total = df_total[df_total["Location"].isin(location)]
    if benificiery: df_total = df_total[df_total["Benificiery"].isin(benificiery)]
    if anemia: df_total = df_total[df_total["anemia_category"].str.lower().isin([x.lower() for x in anemia])]
    
    total = len(df_total)

    # Main df DOES respect Block Code for all other charts
    df = df_full.copy()
    if block_code and "BlockCode" in df.columns: 
        df = df[df["BlockCode"].isin(block_code)]
    if location: df = df[df["Location"].isin(location)]
    if benificiery: df = df[df["Benificiery"].isin(benificiery)]
    if anemia: 
        # Ensure case-insensitive matching for anemia category
        df = df[df["anemia_category"].str.lower().isin([x.lower() for x in anemia])]

    print(f"DEBUG: Active Filters - Block: {block_code}, Loc: {location}, Benif: {benificiery}, Anemia: {anemia}")
    print(f"DEBUG: df length after filtering: {len(df)}")
    
    # Calculate Total Enrollment based on old logic (now using df_total)
    # total = len(df_total) # Already calculated above
    # Robust case-insensitive and substring aware counting for anemia categories
    def count_anemia(status):
        if "anemia_category" not in df.columns or df.empty: return 0
        return df["anemia_category"].astype(str).str.lower().str.contains(status, na=False).sum()

    normal = count_anemia("normal")
    mild = count_anemia("mild")
    moderate = count_anemia("moderate")
    severe = count_anemia("severe")
    # Diet analytics: Specifically focus on Diet 1 (Mapped from raw 'diet1' or 'diet')
    # If Diet 2 exists (meaning raw 'diet' and 'diet1' both existed), we check if the user meant specifically diet1.
    # To be safe and follow "focus on diet1", we'll check Diet 1 which is our primary mapped column.
    if "Diet 1" in df.columns:
        diet_yes = (df["Diet 1"].astype(str).str.strip().str.lower() == "yes").sum()
    elif "Diet 2" in df.columns:
        # Fallback if diet1 was mapped to Diet 2
        diet_yes = (df["Diet 2"].astype(str).str.strip().str.lower() == "yes").sum()
    else:
        diet_yes = 0
    avg_hgb = round(df["HGB"].mean(), 2) if not df.empty else 0
    
    # Prevalence should be based on the FILTERED total (len(df)), not the District Total (total)
    filtered_total = len(df)
    anemic_count = mild + moderate + severe
    prevalence = round((anemic_count / filtered_total * 100), 1) if filtered_total > 0 else 0
    prevalence_str = f"{prevalence}%" if filtered_total > 0 else "No Data"

    # Balanced Percentage Logic for Anemia Categories (Ensure 100.0% sum)
    def get_balanced_percentages(counts_dict, total_count, target_sum=100.0):
        if total_count == 0 or target_sum == 0:
            return {k: 0.0 for k in counts_dict}
        
        # Initial rounding
        pcts = {k: round((v / total_count * 100), 1) for k, v in counts_dict.items()}
        current_sum = round(sum(pcts.values()), 1)
        
        # Adjust if sum is not exactly target_sum (due to rounding)
        if current_sum != round(target_sum, 1) and current_sum != 0:
            diff = round(target_sum - current_sum, 1)
            # Adjust the category with the highest count to minimize visual impact
            max_cat = max(counts_dict, key=lambda k: (counts_dict[k], k))
            pcts[max_cat] = round(pcts[max_cat] + diff, 1)
            
        return pcts

    # --- Prevalence-First Strategy ---
    # 1. Normal is strictly the remainder of 100.0 - Prevalence
    normal_pct = 100.0 - prevalence if filtered_total > 0 else 0
    
    # 2. Sub-categories (Mild, Moderate, Severe) must sum exactly to Prevalence
    anemic_counts_map = {"mild": mild, "moderate": moderate, "severe": severe}
    # We pass total_count=filtered_total so the initial pct calculation is correct, 
    # but the adjustment target is 'prevalence'
    balanced_anemic_pcts = get_balanced_percentages(anemic_counts_map, filtered_total, target_sum=prevalence)
    
    # Store all in one map for the KPI display function
    balanced_pcts = {
        "normal": normal_pct,
        "mild": balanced_anemic_pcts["mild"],
        "moderate": balanced_anemic_pcts["moderate"],
        "severe": balanced_anemic_pcts["severe"]
    }

    def kpi_text(count, pct, t_count):
        if t_count == 0: return "No Data"
        return f"{count} ({pct}%)"

    normal_kpi = kpi_text(normal, balanced_pcts["normal"], filtered_total)
    mild_kpi = kpi_text(mild, balanced_pcts["mild"], filtered_total)
    moderate_kpi = kpi_text(moderate, balanced_pcts["moderate"], filtered_total)
    severe_kpi = kpi_text(severe, balanced_pcts["severe"], filtered_total)


    color_map = {"normal": "#10b981", "mild": "#f59e0b", "moderate": "#f97316", "severe": "#f43f5e", "incomplete": "#475569"}

    table_order = [
        "Sl.No", "ID", "enrollment_date", "BlockCode", "Area Code", "PSU Name",
        "Name", "Household Name", "Gender", "Benificiery", "Trimester", "DOB", "Age",
        "Length", "Height", "Weight", "BMI", "bmi_category",
        "sample_status", "Sample Collected Date", "Collected By",
        "HGB", "anemia_category", "Asha_Worker", "whatsapp", 
        "field_investigator", "Diet 1", "Diet 2", "data_operator"
    ]
    available_cols = [c for c in table_order if c in df.columns or c == "whatsapp"]
    df_table = df.copy()

    # Pre-calculate grouped WhatsApp messages for each Asha Worker
    asha_summaries = {}
    high_risk_df = df[df["anemia_category"].str.lower().isin(["mild", "moderate", "severe"])]
    if not high_risk_df.empty and "Asha_Worker" in df.columns:
        for asha, group in high_risk_df.groupby("Asha_Worker"):
            summary_parts = []
            # Group by category for a cleaner message
            for cat in ["Severe", "Moderate", "Mild"]:
                cat_group = group[group["anemia_category"].str.capitalize() == cat]
                if not cat_group.empty:
                    # Each ID on a new line with a bullet
                    id_list = "\n- ".join(cat_group["ID"].astype(str).unique().tolist())
                    summary_parts.append(f"*{cat}*:\n- {id_list}")
            
            summary_text = "\n\n".join(summary_parts)
            asha_summaries[asha] = f"Hello {asha}, here is the combined list of anemic subjects for follow-up:\n\n{summary_text}\n\nPlease check on them today."

    # Generate WhatsApp Links for all derived tables
    def generate_wa_link(row):
        asha_name = row.get("Asha_Worker")
        # Use unmasked contact for WhatsApp link if available, otherwise fallback
        contact = str(row.get("_real_contact", row.get("Aasha_Contact", "")))
        cat = str(row.get("anemia_category", "")).lower()
        
        if cat in ["mild", "moderate", "severe"] and contact != "" and contact != "nan" and asha_name in asha_summaries:
            msg = asha_summaries[asha_name]
            encoded_msg = urllib.parse.quote(msg)
            link = f"https://wa.me/{contact}?text={encoded_msg}"
            return f"[![WA](https://img.shields.io/badge/Notify-WhatsApp-25D366?style=flat-square&logo=whatsapp)]({link})"
        return ""

    # Apply to main dataframe so all tables benefit
    df["whatsapp"] = df.apply(generate_wa_link, axis=1)
    df_table = df.copy()
    
    # ---------------------------------------------------------
    # DPDP COMPLIANCE: MASK PII FOR DISPLAY (Main Table)
    # ---------------------------------------------------------
    def mask_pii_display(val, is_phone=False):
        if pd.isna(val) or val == "":
            return val
        val = str(val)
        if is_phone:
            if len(val) > 4:
                return "*" * (len(val) - 4) + val[-4:]
            return val
        else:
            if len(val) > 1:
                return val[0] + "*" * (len(val) - 1)
            return "*"

    if "Aasha_Contact" in df_table.columns:
        df_table["Aasha_Contact"] = df_table["Aasha_Contact"].apply(lambda x: mask_pii_display(x, is_phone=True))
        
    if "Name" in df_table.columns:
         df_table["Name"] = df_table["Name"].apply(lambda x: mask_pii_display(x))
         
    if "Household Name" in df_table.columns:
         df_table["Household Name"] = df_table["Household Name"].apply(lambda x: mask_pii_display(x))
    # ---------------------------------------------------------

    # df_table = df_table[available_cols].copy() # Moved down
    date_cols_to_format = ["enrollment_date", "Sample Collected Date", "DOB"]
    for col in date_cols_to_format:
        if col in df_table.columns:
            df_table[col] = pd.to_datetime(df_table[col], errors='coerce').dt.strftime('%d-%m-%Y').fillna("")

    for col in df_table.columns:
        if df_table[col].dtype == 'object':
            df_table[col] = df_table[col].astype(str).str.title()

    # Ensure sequential Sl.No for current main table view
    df_table = df_table.reset_index(drop=True)
    df_table["Sl.No"] = df_table.index + 1

    # Removed is_full_update check to ensure dashboard always reflects current filter state

    if pathname == "/treat":
        map_fig = create_treat_map(df, theme=theme)
    else:
        map_fig = create_map(df, theme=theme)
    
    # Age-wise breakdown for Benificiery Hover
    def get_age_bucket(age):
        if pd.isna(age): return "Missing"
        if age < 1: return f"{int(round(age*12))} Months"
        if age < 5: return "1-4 Years"
        if age <=9: return "5-9 Years"
        if age < 18: return "10-17 Years"
        if age < 30: return "18-29 Years"
        if age < 40: return "30-39 Years"
        if age < 50: return "40-49 Years"
        return "50+ Years"

    # Inverse map to get codes from names
    NAME_TO_CODE = {v: k for k, v in BENEFICIARY_MAP.items()}

    benif_counts = df["Benificiery"].value_counts().sort_index()
    age_hover_data = []
    labels_with_codes = []
    
    for b_group in benif_counts.index:
        # Get numeric code
        b_code = NAME_TO_CODE.get(b_group, b_group)
        labels_with_codes.append(str(b_code))
        
        # Get age breakdown for hover
        sub = df[df["Benificiery"] == b_group]
        buckets = sub["Age"].apply(get_age_bucket).value_counts()
        b_str = "<br>".join([f"• {b}: {c}" for b, c in buckets.items()])
        
        # Build the full hover text
        hover_label = f"<span style='font-size:14px; color:{t['hover_text']}'><b>{b_code}: {b_group}</b></span><br>"
        age_hover_data.append(hover_label + f"Total: <b>{len(sub)}</b><br><br><b>Age Breakdown:</b><br>" + b_str)

    # Beneficiary Distribution (Vertical Bar with Codes)
    benif_bar = go.Figure(go.Bar(
        x=labels_with_codes,
        y=benif_counts.values,
        marker=dict(
            color="#6366f1",
            line=dict(color="#312e81", width=2)
        ),
        customdata=age_hover_data,
        hovertemplate="%{customdata}<extra></extra>",
        opacity=0.9
    ))
    benif_bar.update_layout(
        template=t["plotly"],
        hoverlabel=dict(bgcolor=t["hover_bg"], font_size=13, font_family="var(--font-family)", font_color=t["hover_text"], bordercolor="rgba(99, 102, 241, 0.2)"),
        margin=dict(t=40, b=110, l=40, r=20),
        xaxis=dict(
            title=dict(text="Beneficiary Code", standoff=0), 
            automargin=True, 
            showgrid=False, 
            tickfont=dict(size=12, color=t["tick"])
        ),
        yaxis=dict(title="Count", automargin=True, showgrid=True, gridcolor=t["grid"], tickfont=dict(color=t["tick"])),
        height=360,
        uirevision=True, # Preserve selection/zoom state
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color=t["text"])
    )
    benif_bar.update_xaxes(showgrid=False, zeroline=False)
    benif_bar.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.05)", zeroline=False)

    # Anemia pie
    anemia_counts = df["anemia_category"].value_counts()
    anemia_pie = go.Figure(go.Pie(
        labels=[str(l).capitalize() for l in anemia_counts.index],
        values=anemia_counts.values,
        hole=0.6,
        marker=dict(colors=[color_map.get(str(l).lower(), "#cbd5e1") for l in anemia_counts.index],
                    line=dict(color='white', width=3)), # Wider border for pie focus
        textinfo="percent",
        hovertemplate="<b>%{label}</b><br>Count: <b>%{value}</b> (%{percent})<extra></extra>",
        opacity=0.95
    ))
    anemia_pie.update_layout(
        template=t["plotly"],
        hoverlabel=dict(bgcolor=t["hover_bg"], font_size=13, font_family="var(--font-family)", font_color=t["hover_text"], bordercolor="rgba(99, 102, 241, 0.2)"),
        height=250,
        uirevision=True, # Preserve slice selection state
        font=dict(color=t["text"]),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=0, b=0, l=0, r=0),
        legend=dict(font=dict(color=t["tick"]), bgcolor="rgba(0,0,0,0)")
    )
    # Give the pie more room
    anemia_pie.update_traces(domain=dict(y=[0.2, 1.0]))

    # Village-wise Anemia Classification (Stacked Bar with Area Codes)
    psu_to_code = df.set_index("PSU Name")["Area Code"].to_dict() if not df.empty else {}
    
    village_anemia = df.groupby(["PSU Name", "anemia_category"]).size().unstack(fill_value=0)
    village_area_codes = [str(psu_to_code.get(psu, psu)) for psu in village_anemia.index]
    
    # Pre-calculate a "dialogue box" summary for each PSU
    psu_summaries = []
    for psu in village_anemia.index:
        counts = village_anemia.loc[psu]
        summary = f"<span style='font-size:16px; color:#1e293b'><b>{psu}</b></span><br>"
        # Using Category names the user requested
        summary += f"Severe: <b>{counts.get('severe', 0)}</b><br>"
        summary += f"Moderate: <b>{counts.get('moderate', 0)}</b><br>"
        summary += f"Mild: <b>{counts.get('mild', 0)}</b><br>"
        summary += f"Normal: <b>{counts.get('normal', 0)}</b>"
        psu_summaries.append(summary)

    anemia_village_bar = go.Figure()
    for cat in ["normal", "mild", "moderate", "severe", "incomplete"]:
        if cat in village_anemia:
            anemia_village_bar.add_bar(
                name=cat.capitalize(), 
                x=village_anemia.index, # Setting X to Name for Header
                y=village_anemia[cat], 
                customdata=psu_summaries, 
                hovertemplate="%{customdata}<extra></extra>",
                marker=dict(
                    color=color_map.get(cat),
                    line=dict(color='white', width=1.5)
                ),
                opacity=0.95
            )
            
    anemia_village_bar.update_layout(
        template=t["plotly"],
        barmode="stack", 
        hovermode="closest",
        margin=dict(t=30, b=80, l=40, r=20),
        xaxis=dict(
            title=dict(text="Area Code", standoff=0), 
            tickvals=village_anemia.index, # Map Names to Ticks
            ticktext=village_area_codes, # Show Codes on Ticks
            automargin=True, 
            showgrid=False, 
            tickfont=dict(size=11, color=t["tick"]),
            showline=True, linecolor=t["grid"],
        ),
        yaxis=dict(
            title="Beneficiaries", 
            automargin=True, 
            showgrid=True, gridcolor=t["grid"],
            tickfont=dict(color=t["tick"]),
            zeroline=False
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5, font=dict(size=11, color=t["tick"]), bgcolor="rgba(0,0,0,0)"),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        hoverlabel=dict(bgcolor=t["hover_bg"], font_size=13, font_family="var(--font-family)", font_color=t["hover_text"], bordercolor="rgba(99, 102, 241, 0.2)"),
        height=450,
        bargap=0.2,
        uirevision=True # Preserve zoom/pan state
    )

    # --- Village-wise Bar Chart (Mean & SD STATS) ---
    hgb_data = df.dropna(subset=["HGB", "PSU Name"])
    hgb_stats_fig = go.Figure()

    if not hgb_data.empty:
        # Calculate stats per village
        stats = hgb_data.groupby("PSU Name")["HGB"].agg(["mean", "std", "count"]).reset_index().round(2)
        
        # Calculate Anemic Count (Mild + Moderate + Severe)
        anemic_df = df[df["anemia_category"].str.lower().isin(["mild", "moderate", "severe"])]
        anemic_counts = anemic_df.groupby("PSU Name").size().reset_index(name="anemic_count")
        
        # Merge to ensure alignment
        stats = pd.merge(stats, anemic_counts, on="PSU Name", how="left").fillna(0)
        stats = stats.sort_values("PSU Name")
        
        # Bar Chart with Tooltip info (Area Codes for labels)
        stats["area_code"] = stats["PSU Name"].map(psu_to_code).astype(str)
        
        hgb_stats_fig.add_trace(go.Bar(
            x=stats["PSU Name"],
            y=stats["mean"],
            error_y=dict(type='data', array=stats["std"], visible=True, color="#312e81", thickness=2, width=6),
            marker=dict(
                color="#6366f1",
                line=dict(color="#312e81", width=2),
            ),
            opacity=0.9,
            name="Mean HGB",
            text=stats["mean"],
            textposition="auto",
            textfont=dict(color="white", size=10, family="-apple-system, BlinkMacSystemFont, sans-serif"),
            customdata=stats[["PSU Name", "area_code", "std", "count", "anemic_count"]].values.tolist(),
            hovertemplate=(
                "<span style='font-size:16px;'><b>%{customdata[1]} - %{customdata[0]}</b></span><br>" +
                "Mean HGB: <b>%{y} g/dL</b><br>" +
                "Std Dev: <b>%{customdata[2]}</b><br>" +
                "Total Samples: <b>%{customdata[3]}</b><br>" +
                "Anemic Count: <b>%{customdata[4]}</b><extra></extra>"
            )
        ))
        
        group_avg = hgb_data["HGB"].mean()
        # Add the reference line 
        hgb_stats_fig.add_hline(y=group_avg, line_dash="dash", line_color="#10b981", line_width=2)
        
        # Add legend-style annotation
        hgb_stats_fig.add_annotation(
            xref="paper", yref="paper",
            x=1.0, y=1.08,
            text=f"<span style='color:#10b981'><b>--</b></span> Dataset Average: <b>{group_avg:.2f}</b>",
            showarrow=False,
            font=dict(size=12, family="-apple-system, BlinkMacSystemFont, sans-serif", color=t["text"]),
            xanchor="right", yanchor="bottom"
        )

    hgb_stats_fig.update_layout(
        template=t["plotly"],
        margin=dict(t=50, b=80, l=50, r=20),
        hovermode="closest",
        xaxis=dict(
            title=dict(text="Area Code", standoff=0), 
            tickvals=stats["PSU Name"] if not hgb_data.empty else [],
            ticktext=stats["area_code"] if not hgb_data.empty else [],
            automargin=True, 
            showgrid=False, 
            tickfont=dict(size=11, color=t["tick"]),
            showline=True, linecolor=t["grid"],
        ),
        yaxis=dict(
            title="Avg Haemoglobin (g/dL)", 
            automargin=True, 
            showgrid=True, gridcolor=t["grid"],
            tickfont=dict(color=t["tick"]),
            zeroline=False
        ),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        hoverlabel=dict(bgcolor=t["hover_bg"], font_size=13, font_family="var(--font-family)", font_color=t["hover_text"], bordercolor="rgba(99, 102, 241, 0.2)"),
        height=450,
        showlegend=False,
        bargap=0.2,
        uirevision=True # Preserve zoom/pan state
    )
    
    # BMI Distribution Bar Chart (Stacked by Beneficiary)
    if "Benificiery" in df.columns and "bmi_category" in df.columns:
        # Exclude Pregnant Women as they follow different clinical benchmarks
        df_bmi = df[df["Benificiery"] != "Pregnant Women"]
        bmi_ben_counts = df_bmi.groupby(["Benificiery", "bmi_category"]).size().unstack(fill_value=0)
    else:
        bmi_ben_counts = pd.DataFrame()

    bmi_colors = {
        "Severe Underweight": "#7f1d1d", # Darkest Red
        "Underweight": "#ef4444",        # Standard Red
        "Normal": "#10b981",             # Emerald
        "Risk of Overweight": "#3b82f6", # Ocean Blue (for children)
        "Overweight": "#f59e0b",         # Amber
        "Obese": "#450a0a",              # Deep Blood Red
        "Pregnancy": "#8b5cf6",
        "Data Missing": "#94a3b8"
    }
    
    # Unified stacking order
    stack_order = ["Severe Underweight", "Underweight", "Normal", "Risk of Overweight", "Overweight", "Obese", "Pregnancy", "Data Missing"]
            
    bmi_fig = go.Figure()
    
    if not bmi_ben_counts.empty:
        # Pre-calculate summaries for each Beneficiary
        ben_summaries = {}
        for ben in bmi_ben_counts.index:
            row = bmi_ben_counts.loc[ben]
            parts = []
            # Use stack_order for consistent ordering in tooltip
            for c in stack_order:
                if c in row and row[c] > 0:
                    parts.append(f"{c}: <b>{row[c]}</b>")
            # Also add extra categories not in stack_order
            for c in row.index:
                if c not in stack_order and row[c] > 0:
                    parts.append(f"{c}: <b>{row[c]}</b>")
            ben_summaries[ben] = "<br>".join(parts)

        # Map summaries to the x-axis order
        custom_data_list = [ben_summaries.get(b, "") for b in bmi_ben_counts.index]

        # Ensure all columns exist for consistent coloring even if count is 0
        present_cats = [c for c in stack_order if c in bmi_ben_counts.columns]
        # Also add any unexpected categories found in data
        extra_cats = [c for c in bmi_ben_counts.columns if c not in stack_order]
        final_order = present_cats + extra_cats
        
        for cat in final_order:
            if cat in bmi_ben_counts:
                bmi_fig.add_trace(go.Bar(
                    name=cat,
                    x=bmi_ben_counts.index,
                    y=bmi_ben_counts[cat],
                    marker=dict(
                        color=bmi_colors.get(cat, "#cbd5e1"),
                        line=dict(color="white", width=1)
                    ),
                    customdata=custom_data_list,
                    # Hover: Show current segment + Full Summary
                    hovertemplate="<b>%{x}</b><br>" + cat + ": <b>%{y}</b><br><br><b>Total Breakdown:</b><br>%{customdata}<extra></extra>"
                ))
    else:
        # Fallback empty chart 
        bmi_fig.add_annotation(text="No Data", showarrow=False, xref="paper", yref="paper", x=0.5, y=0.5)

    bmi_fig.update_layout(
        template=t["plotly"],
        barmode="stack",
        margin=dict(t=60, b=50, l=50, r=20),
        xaxis=dict(title="Beneficiary Type", showgrid=False, tickfont=dict(color=t["tick"])),
        yaxis=dict(title="Count", showgrid=True, gridcolor=t["grid"], tickfont=dict(color=t["tick"])),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        hoverlabel=dict(bgcolor=t["hover_bg"], font_size=13, font_family="var(--font-family)", font_color=t["hover_text"], bordercolor="rgba(99, 102, 241, 0.2)"),
        height=450,
        bargap=0.3,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5, font=dict(size=10, color=t["tick"])),
        uirevision=True,
        annotations=[
            dict(
                x=1.0, y=1.15,
                xref="paper", yref="paper",
                text="ⓘ",
                showarrow=False,
                font=dict(size=20, color=t["tick"]),
                hovertext="Terminology: 'Underweight' corresponds to WHO 'Thinness/Wasted' categories. Pregnant Women are excluded from this chart.",
                align="right"
            )
        ]
    )
    bmi_fig.update_xaxes(showline=True, linecolor=t["grid"])
    # ----------------------------------------------

    # Urgent Alerts (Severe Anemia)
    urgent_df = df_full[df_full["anemia_category"] == "severe"].head(10)
    urgent_list = []
    for _, row in urgent_df.iterrows():
        # Generate WP link for sidebar [Grouped Version]
        # Use unmasked contact for WhatsApp link if available
        contact = str(row.get("_real_contact", row.get("Aasha_Contact", "")))
        asha_name = row.get("Asha_Worker")
        p_id = str(row.get("ID", "Missing"))
        
        wa_btn = None
        if contact != "" and contact != "nan" and asha_name in asha_summaries:
            # Conditional check for valid asha name
            is_valid_asha = asha_name and str(asha_name).lower() not in ["nan", "none", "", "missing"]
            if is_valid_asha:
                msg = asha_summaries[asha_name]
                encoded_msg = urllib.parse.quote(msg)
                link = f"https://wa.me/{contact}?text={encoded_msg}"
                wa_btn = html.A(html.I(className="fab fa-whatsapp", style={"color": "#25D366", "marginLeft": "10px", "fontSize": "1.1rem"}), 
                                href=link, target="_blank")

        urgent_list.append(html.Div([
            html.Div([
                html.Span(f"ID: {p_id}", style={"fontWeight": "600"}),
                html.Span(f" | Hb: {row.get('HGB', 'N/A')}", style={"color": "#ef4444"}),
            ], style={"display": "flex", "alignItems": "center", "justifyContent": "space-between"}),
            html.Div([
                html.P(f"{row.get('PSU Name', 'Missing')}", style={"margin": 0, "fontSize": "0.65rem", "color": "#64748b"}),
                wa_btn if wa_btn else html.Span()
            ], style={"display": "flex", "alignItems": "center", "justifyContent": "space-between"})
        ], className="urgent-item"))
    
    if not urgent_list:
        urgent_list = [html.P("No urgent cases found.", className="text-muted", style={"fontSize": "0.75rem"})]

    # Define display names for specific columns
    col_names = {
        "whatsapp": "Notify Asha",
        "HGB": "HGB (g/dL)",
        "Length": "Length (Age < 2 years)",
        "Height": "Height (cm)",
        "Weight": "Weight (kg)",
        "bmi_category": "Nutritional Status"
    }
    
    table_cols = [
        {
            "name": col_names.get(c, c), 
            "id": c, 
            "presentation": "markdown" if c == "whatsapp" else "input"
        } for c in available_cols 
        if not (pathname in ["/", None] and c in ["Asha_Worker", "whatsapp"])
    ]

    print(f">>> RETURNING LOCATION: {location}")
    print(f">>> CALLBACK END: {triggered_id}\n")

    # --- Treat Page Specific Tables ---
    treat_cols = [
        {"name": "Notify Asha", "id": "whatsapp", "presentation": "markdown"},
        {"name": "Subject ID", "id": "ID"},
        {"name": "Village", "id": "PSU Name"},
        {"name": "Hb Level", "id": "HGB"},
        {"name": "Classification", "id": "Benificiery"},
        {"name": "Asha Worker", "id": "Asha_Worker"},
        {"name": "Reset", "id": "reset_btn", "presentation": "markdown"}
    ]

    severe_data = []
    moderate_data = []
    mild_data = []

    if pathname == "/treat":
        # We use the filtered 'df' to populate these tables
        df_severe = df[df["anemia_category"].str.lower() == "severe"].copy()
        df_moderate = df[df["anemia_category"].str.lower() == "moderate"].copy()
        df_mild = df[df["anemia_category"].str.lower() == "mild"].copy()

        # DPDP COMPLIANCE: MASK PII
        for d in [df_severe, df_moderate, df_mild]:
            if "Aasha_Contact" in d.columns:
                d["Aasha_Contact"] = d["Aasha_Contact"].apply(lambda x: mask_pii_display(x, is_phone=True))
            if "Name" in d.columns:
                 d["Name"] = d["Name"].apply(lambda x: mask_pii_display(x))
            if "Household Name" in d.columns:
                 d["Household Name"] = d["Household Name"].apply(lambda x: mask_pii_display(x))

        # Generate Status based on cache
        def get_notify_status(row):
            # Same key as used in bulk notify
            key = f"{row.get('Asha_Worker')}_{row.get('ID')}"
            if key in NOTIFIED_CACHE:
                ts = NOTIFIED_CACHE[key]
                return f"Sent({ts})"
            return "Pending"

        df_severe["notify_status"] = df_severe.apply(get_notify_status, axis=1)
        df_moderate["notify_status"] = df_moderate.apply(get_notify_status, axis=1)
        df_mild["notify_status"] = df_mild.apply(get_notify_status, axis=1)

        # Populate Reset button (Markdown link that triggers active_cell)
        def get_reset_icon(row):
            key = f"{row.get('Asha_Worker')}_{row.get('ID')}"
            if key in NOTIFIED_CACHE:
                return "❌"
            return ""

        df_severe["reset_btn"] = df_severe.apply(get_reset_icon, axis=1)
        df_moderate["reset_btn"] = df_moderate.apply(get_reset_icon, axis=1)
        df_mild["reset_btn"] = df_mild.apply(get_reset_icon, axis=1)

        # Ensure sequential Sl.No for Treat page tables 1, 2, 3...
        for d in [df_severe, df_moderate, df_mild]:
            if not d.empty:
                d["Sl.No"] = range(1, len(d) + 1)

        severe_data = df_severe.to_dict("records")
        moderate_data = df_moderate.to_dict("records")
        mild_data = df_mild.to_dict("records")

    # --- Weekly Summaries for Supervisor ---
    summaries = generate_weekly_summary(df)
    summary_cards = []
    if summaries:
        for s in summaries:
            encoded_text = urllib.parse.quote(s["text"])
            wa_link = f"https://wa.me/{s['contact']}?text={encoded_text}"
            
            card = dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.H6(s["asha"], className="mb-0", style={"fontWeight": "700", "color": "var(--text-main)"}),
                            html.P(f"{s['village']} | {s['count']} cases" if s["asha"] != "Asha Details Missing" else f"{s['count']} cases", 
                                   className="mb-0", style={"fontSize": "0.75rem", "color": "var(--text-muted)"})
                        ], md=8, xs=8),
                        dbc.Col([
                            dbc.Button([html.I(className="fab fa-whatsapp")], 
                                       id={"type": "btn-notify-asha", "index": s["asha"]},
                                       href=wa_link, target="_blank", color="success", outline=True, size="sm", className="w-100 mb-1, notify-btn"),
                            dbc.Button([html.I(className="fas fa-undo")], 
                                       id={"type": "btn-reset-asha", "index": s["asha"]},
                                       color="secondary", outline=True, size="sm", className="w-100", title="Reset Notification Status")
                        ], md=4, xs=4) if s.get("show_whatsapp", True) else None
                    ])
                ])
            ], className="mb-2 shadow-sm", style={"borderRadius": "10px", "border": "1px solid var(--glass-border)", "background": "var(--card-bg)"})
            summary_cards.append(dbc.Col(card, md=4, sm=6))
        
        weekly_summary_content = dbc.Row(summary_cards)
    else:
        weekly_summary_content = html.P("No anemic cases found for summary.", style={"color": "var(--text-muted)", "fontSize": "0.85rem", "fontStyle": "italic"})

    # Block-wise Anemia Distribution Chart
    block_fig = go.Figure()
    block_prev_fig = go.Figure()
    if "BlockCode" in df.columns and not df.empty:
        # Aggregate data
        block_anemia_counts = df.groupby(["BlockCode", "anemia_category"]).size().unstack(fill_value=0)
        
        # Ensure all categories exist
        for cat in ["normal", "mild", "moderate", "severe"]:
            if cat not in block_anemia_counts.columns:
                block_anemia_counts[cat] = 0
                
        # Sort blocks code-wise if possible, or alphabetical
        # Since we mapped them to "Name (Code)", sorting index should work well
        block_anemia_counts = block_anemia_counts.sort_index()

        # Prepare Custom Hover Data (Dialogue Box Style)
        block_summaries = []
        for block in block_anemia_counts.index:
            row = block_anemia_counts.loc[block]
            summary = f"<span style='font-size:16px;'><b>{block}</b></span><br>"
            summary += f"Severe: <b>{row.get('severe', 0)}</b><br>"
            summary += f"Moderate: <b>{row.get('moderate', 0)}</b><br>"
            summary += f"Mild: <b>{row.get('mild', 0)}</b><br>"
            summary += f"Normal: <b>{row.get('normal', 0)}</b><br>" 
            summary += f"Total: <b>{row.sum()}</b>"
            block_summaries.append(summary)

        # Add Traces
        colors = {"normal": "#10b981", "mild": "#f59e0b", "moderate": "#f97316", "severe": "#ef4444"}
        for cat in ["normal", "mild", "moderate", "severe"]: 
            if cat in block_anemia_counts.columns:
                block_fig.add_trace(go.Bar(
                    x=block_anemia_counts.index,
                    y=block_anemia_counts[cat],
                    name=cat.capitalize(),
                    marker_color=colors.get(cat, "#ccc"),
                    customdata=block_summaries,
                    hovertemplate="%{customdata}<extra></extra>"
                ))

        # Add Total Count Labels on Top
        block_totals = block_anemia_counts.sum(axis=1)
        block_fig.add_trace(go.Scatter(
            x=block_totals.index,
            y=block_totals.values,
            text=block_totals.values,
            mode='text',
            textposition='top center',
            textfont=dict(color=t["text"], size=12, weight='bold'),
            showlegend=False,
            hoverinfo='text',
            customdata=block_summaries,
            hovertemplate="%{customdata}<extra></extra>"
        ))

        block_fig.update_layout(
            barmode='stack',
            template=t["plotly"],
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color=t["tick"])),
            font=dict(family="Outfit, sans-serif", color=t["text"]),
            hoverlabel=dict(bgcolor=t["hover_bg"], font_size=13, font_family="var(--font-family)", font_color=t["hover_text"], bordercolor="rgba(99, 102, 241, 0.2)"),
            xaxis=dict(showgrid=False, tickfont=dict(color=t["tick"])),
            yaxis=dict(showgrid=True, gridcolor=t["grid"], tickfont=dict(color=t["tick"]))
        )

        # Block-wise Prevalence Chart Logic
        # Calculate Anemic Count (Mild + Moderate + Severe)
        anemic_cols = [c for c in ["mild", "moderate", "severe"] if c in block_anemia_counts.columns]
        if anemic_cols:
            block_anemic = block_anemia_counts[anemic_cols].sum(axis=1)
        else:
            block_anemic = pd.Series(0, index=block_anemia_counts.index)

        # Calculate Percentage
        # Handle division by zero
        block_prevalence = (block_anemic / block_totals * 100).fillna(0).round(1)

        # Create Custom Data for Tooltip
        prev_summaries = []
        for block in block_prevalence.index:
            b_total = block_totals.loc[block]
            anemic = block_anemic.loc[block]
            prev = block_prevalence.loc[block]
            
            summary = f"<span style='font-size:16px;'><b>{block}</b></span><br>"
            summary += f"Prevalence: <b>{prev}%</b><br>"
            summary += f"Anemic Cases: <b>{int(anemic)}</b><br>"
            summary += f"Total Assessed: <b>{int(b_total)}</b>"
            prev_summaries.append(summary)

        # Add Bar Trace
        block_prev_fig.add_trace(go.Bar(
            x=block_prevalence.index,
            y=block_prevalence.values,
            text=[f"{v}%" for v in block_prevalence.values],
            textposition='auto',
            name="Prevalence",
            marker_color="#8b5cf6", # Violet for prevalence
            customdata=prev_summaries,
            hovertemplate="%{customdata}<extra></extra>"
        ))

        block_prev_fig.update_layout(
            template=t["plotly"],
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=20, r=20, t=20, b=20),
            font=dict(family="Outfit, sans-serif", color=t["text"]),
            hoverlabel=dict(bgcolor=t["hover_bg"], font_size=13, font_family="var(--font-family)", font_color=t["hover_text"], bordercolor="rgba(99, 102, 241, 0.2)"),
            xaxis=dict(showgrid=False, tickfont=dict(color=t["tick"])),
            yaxis=dict(showgrid=True, gridcolor=t["grid"], tickfont=dict(color=t["tick"]), range=[0, 100], title="Prevalence (%)")
        )

    print(f"DEBUG: FINAL RETURN -> Total: {total}, Prev: {prevalence_str}, Normal: {normal_kpi}")
    print(f"DEBUG: anemia_opts: {anemia_opts[:2]}... (len: {len(anemia_opts)})")
    
    return (total, normal_kpi, moderate_kpi, severe_kpi, mild_kpi, avg_hgb, diet_yes, prevalence_str, map_fig, benif_bar, anemia_pie, anemia_village_bar, hgb_stats_fig, bmi_fig, block_fig, block_prev_fig, df_table.to_dict("records"), table_cols, block_opts, loc_opts, benif_opts, anemia_opts, block_code, location, benificiery, anemia, urgent_list, severe_data, treat_cols, moderate_data, treat_cols, mild_data, treat_cols, weekly_summary_content)


# =========================
# EXPORT CALLBACKS
# =========================
@app.callback(
    Output("reset-notification-trigger", "data", allow_duplicate=True),
    Input({"type": "btn-reset-asha", "index": ALL}, "n_clicks"),
    prevent_initial_call=True
)
def reset_asha_status(n_clicks):
    ctx = callback_context
    if not ctx.triggered:
        return no_update
        
    # Get the button that was clicked
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    triggered_value = ctx.triggered[0]["value"]
    
    # HARDENING: Ignore if n_clicks is None or 0 (Ghost trigger on creation)
    if not triggered_value:
        return no_update
        
    try:
        id_dict = json.loads(button_id)
        asha_to_reset = id_dict.get("index")
    except:
        return no_update
        
    if not asha_to_reset:
        return no_update
        
    print(f"DEBUG: RESET TRIGGERED for {asha_to_reset}. n_clicks={triggered_value}")
    
    # Clear cache entries for this Asha
    # Handle "Asha Details Missing" which maps to empty string prefix "_"
    if asha_to_reset == "Asha Details Missing":
        keys_to_remove = [k for k in NOTIFIED_CACHE.keys() if k.startswith("_")]
    else:
        keys_to_remove = [k for k in NOTIFIED_CACHE.keys() if k.startswith(f"{asha_to_reset}_")]
        
    for k in keys_to_remove:
        del NOTIFIED_CACHE[k]
        
    save_notified_cache()
    
    # Trigger dashboard update
    return datetime.now().timestamp()

@app.callback(
    Output("reset-notification-trigger", "data", allow_duplicate=True),
    Input({"type": "btn-notify-asha", "index": ALL}, "n_clicks"),
    [State("stored-data", "data")],
    prevent_initial_call=True
)
def update_asha_notification_status(n_clicks, stored_dict):
    ctx = callback_context
    if not ctx.triggered:
        return no_update
        
    # Get the button that was clicked
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    triggered_value = ctx.triggered[0]["value"]
    
    # HARDENING: Ignore if n_clicks is None or 0
    if not triggered_value:
        return no_update

    try:
        id_dict = json.loads(button_id)
        asha_clicked = id_dict.get("index")
    except:
        return no_update
        
    # Check if a click actually happened (n_clicks > 0)
    # Since we use 'href', we need to be careful. But 'n_clicks' usually increments.
    # We iterate n_clicks to see if any are > 0, but since ALL is used, n_clicks is a list.
    # ctx.triggered gives us the specific one.
    
    # Check simple validity
    if not asha_clicked or not stored_dict:
        return no_update

    print(f"DEBUG: NOTIFY TRIGGERED for {asha_clicked}. n_clicks={triggered_value}")
    
    df = pd.DataFrame(stored_dict["records"])
    
    # Filter for this Asha
    # Handle "Asha Details Missing"
    if asha_clicked == "Asha Details Missing":
        # Missing or empty Asha name
        # We need to find rows where Asha_Worker is missing/nan/empty
        # AND are anemic (since summary only includes anemic)
        mask_asha = df["Asha_Worker"].isna() | (df["Asha_Worker"] == "") | (df["Asha_Worker"].str.lower() == "nan")
    else:
        mask_asha = df["Asha_Worker"] == asha_clicked
        
    mask_anemia = df["anemia_category"].str.lower().isin(["mild", "moderate", "severe"])
    
    target_rows = df[mask_asha & mask_anemia]
    
    now_str = datetime.now().strftime("%d/%m %H:%M")
    count_updated = 0
    
    load_notified_cache()
    
    for _, row in target_rows.iterrows():
        p_id = row.get("ID")
        a_name = row.get("Asha_Worker", "")
        if not a_name or str(a_name).lower() in ["nan", "none", "missing"]:
            a_name = ""
            
        key = f"{a_name}_{p_id}"
        
        # Mark as notified
        NOTIFIED_CACHE[key] = now_str
        count_updated += 1
        
    if count_updated > 0:
        save_notified_cache()
        print(f"DEBUG: Automatically marked {count_updated} subjects as Sent.")
        return datetime.now().timestamp()
        
    return no_update

@app.callback(
    Output("reset-notification-trigger", "data", allow_duplicate=True),
    [Input("severe-table", "active_cell"),
     Input("moderate-table", "active_cell"),
     Input("mild-table", "active_cell")],
    [State("severe-table", "derived_virtual_data"),
     State("moderate-table", "derived_virtual_data"),
     State("mild-table", "derived_virtual_data")],
    prevent_initial_call=True
)
def handle_table_actions(severe_cell, moderate_cell, mild_cell, severe_data, moderate_data, mild_data):
    ctx = callback_context
    if not ctx.triggered:
        return no_update

    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
    
    # Determine which table and cell triggered
    active_cell = None
    data = None
    
    if trigger_id == "severe-table":
        active_cell = severe_cell
        data = severe_data
    elif trigger_id == "moderate-table":
        active_cell = moderate_cell
        data = moderate_data
    elif trigger_id == "mild-table":
        active_cell = mild_cell
        data = mild_data
        
    if not active_cell or not data:
        return no_update
        
    col_id = active_cell.get("column_id")
    
    # Only react to Reset or WhatsApp columns
    if col_id not in ["reset_btn", "whatsapp"]:
        return no_update
        
    # Get the row data
    row_idx = active_cell.get("row")
    if row_idx is None or row_idx >= len(data):
        return no_update
        
    row = data[row_idx]
    asha = row.get("Asha_Worker", "")
    p_id = row.get("ID")
    
    # Reload cache to ensure we have the latest state
    load_notified_cache()
    
    # Handle missing/nan asha
    if not asha or str(asha).lower() in ["nan", "none", "missing"]:
        asha = ""
        
    # ACTION: RESET STATUS
    if col_id == "reset_btn":
        id_suffix = f"_{p_id}"
        keys_to_delete = []
        
        # Aggressive search: Find ANY key ending with the ID
        for k in NOTIFIED_CACHE.keys():
            if k.endswith(id_suffix):
                keys_to_delete.append(k)
                
        with open("debug_reset.txt", "a") as f:
            f.write(f"\n--- RESET ACTION: {datetime.now()} ---\n")
            f.write(f"ID: {p_id}, Keys found: {keys_to_delete}\n")
        
        if keys_to_delete:
            for k in keys_to_delete:
                del NOTIFIED_CACHE[k]
            save_notified_cache()
            return datetime.now().timestamp()

    # ACTION: MARK AS SENT (click on WhatsApp link)
    elif col_id == "whatsapp":
        # Construct key using current Asha name (or empty)
        key = f"{asha}_{p_id}"
        now_str = datetime.now().strftime("%d/%m %H:%M")
        
        # Only update if not already there (or update timestamp? User might want to re-notify)
        # Let's always update to show latest action
        NOTIFIED_CACHE[key] = now_str
        
        with open("debug_reset.txt", "a") as f:
            f.write(f"\n--- NOTIFY ACTION: {datetime.now()} ---\n")
            f.write(f"ID: {p_id}, Key set: {key}\n")
            
        save_notified_cache()
        return datetime.now().timestamp()
        
    return no_update

@app.callback(
    Output("download-data", "data"),
    [Input("btn-excel", "n_clicks"), Input("btn-csv", "n_clicks")],
    [State("stored-data", "data"), State("block-code-dropdown", "value"), State("location-dropdown", "value"),
     State("benificiery-dropdown", "value"), State("anemia-dropdown", "value")],
    prevent_initial_call=True
)
def export_data(n_excel, n_csv, stored_dict, block_code, location, benif, anemia):
    try:
        ctx = callback_context
        if not ctx.triggered:
            return no_update
            
        trigger = ctx.triggered[0]["prop_id"].split(".")[0]
        
        # Robust verification: Only proceed if a button was actually clicked (n_clicks > 0)
        if trigger == "btn-excel" and (n_excel is None or n_excel == 0):
            return no_update
        if trigger == "btn-csv" and (n_csv is None or n_csv == 0):
            return no_update
            
        if not stored_dict or "records" not in stored_dict:
            return no_update
        
        df = pd.DataFrame(stored_dict["records"])
        
        # Robust Type Enforcement for Filters
        block_code = [block_code] if isinstance(block_code, str) else (block_code or [])
        location = [location] if isinstance(location, str) else (location or [])
        benif = [benif] if isinstance(benif, str) else (benif or [])
        anemia = [anemia] if isinstance(anemia, str) else (anemia or [])
        
        # Apply filters
        if block_code:
            df = df[df["BlockCode"].isin(block_code)]
        if location:
            df = df[df["Location"].isin(location)]
        if benif:
            df = df[df["Benificiery"].isin(benif)]
        if anemia:
            anemia_lower = [str(x).lower() for x in anemia]
            df = df[df["anemia_category"].str.lower().isin(anemia_lower)]

        # ---------------------------------------------------------
        # DPDP COMPLIANCE: MASK PII BEFORE EXPORT
        # ---------------------------------------------------------
        def mask_pii(val, is_phone=False):
            if pd.isna(val) or val == "":
                return val
            val = str(val)
            if is_phone:
                if len(val) > 4:
                    return "*" * (len(val) - 4) + val[-4:]
                return val
            else:
                if len(val) > 1:
                    return val[0] + "*" * (len(val) - 1)
                return "*"

        if "Aasha_Contact" in df.columns:
            df["Aasha_Contact"] = df["Aasha_Contact"].apply(lambda x: mask_pii(x, is_phone=True))
            
        if "Name" in df.columns:
             df["Name"] = df["Name"].apply(lambda x: mask_pii(x))
             
        if "Household Name" in df.columns:
             df["Household Name"] = df["Household Name"].apply(lambda x: mask_pii(x))
             
        # Remove internal/sensitive columns from export
        # Note: Using case-insensitive check for robustness
        cols_to_remove = ["_real_contact", "email", "status"]
        # Find actual columns that match (ignoring case if needed, but for now exact or mapped)
        # Actually, let's just drop them if they exist exactly, or check standard variants
        actual_cols_to_drop = [c for c in df.columns if c in cols_to_remove or c.lower() in ["email", "status"]]
        
        if actual_cols_to_drop:
            df = df.drop(columns=actual_cols_to_drop)
        # ---------------------------------------------------------

        # Format dates for export (DD-MM-YYYY)
        date_cols = ["enrollment_date", "Sample Collected Date", "DOB"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d-%m-%Y').fillna("")

        print(f"DEBUG: Exporting {len(df)} records. Trigger: {trigger}")

        if trigger == "btn-csv":
            return dcc.send_data_frame(df.to_csv, "prakash_data_export.csv", index=False)
        else:
            return dcc.send_data_frame(df.to_excel, "prakash_data_export.xlsx", index=False, engine="openpyxl")
    except Exception as e:
        print(f"CRITICAL ERROR in export_data: {e}")
        return no_update

# Bulk Notification Callback
@app.callback(
    [Output("bulk-notification-urls", "data"), 
     Output("bulk-notify-toast", "is_open"),
     Output("bulk-notify-toast", "children"),
     Output("bulk-notify-toast", "icon"),
     Output("notification-queue-data", "data", allow_duplicate=True)],
    Input("btn-bulk-notify", "n_clicks"),
    [State("stored-data", "data"), State("block-code-dropdown", "value"), State("location-dropdown", "value"),
     State("benificiery-dropdown", "value"), State("anemia-dropdown", "value"),
     State("notification-queue-data", "data")],
    prevent_initial_call=True
)
def trigger_bulk_notify(n, stored_dict, block_code, location, benif, anemia, current_queue):
    if n is None or n == 0 or not stored_dict or "records" not in stored_dict:
        return no_update, False, no_update, no_update, no_update
    
    print(f"DEBUG: Bulk Notify Triggered. n_clicks={n}")
    df = pd.DataFrame(stored_dict["records"])
    
    current_queue = current_queue or []
    print(f"DEBUG: DF columns available: {df.columns.tolist()}")

    # Check for required columns
    required = ["Asha_Worker", "Aasha_Contact"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        msg = f"Missing columns in data source: {', '.join(missing)}. Please add them to your sheet."
        print(f"DEBUG: {msg}")
        return no_update, True, msg, "danger", no_update

    # Standard filters
    block_code = [block_code] if isinstance(block_code, str) else (block_code or [])
    location = [location] if isinstance(location, str) else (location or [])
    benif = [benif] if isinstance(benif, str) else (benif or [])
    anemia = [anemia] if isinstance(anemia, str) else (anemia or [])
    
    if block_code: df = df[df["BlockCode"].isin(block_code)]
    if location: df = df[df["Location"].isin(location)]
    if benif: df = df[df["Benificiery"].isin(benif)]
    
    # Filter for anemia - default to Moderate/Severe if no filter
    if anemia:
        anemia_lower = [str(x).lower() for x in anemia]
        df = df[df["anemia_category"].str.lower().isin(anemia_lower)]
    else:
        # Default to all anemic categories for notifications
        df = df[df["anemia_category"].str.lower().isin(["mild", "moderate", "severe"])]

    print(f"DEBUG: Filtered records count: {len(df)}")

    # Group by Asha Worker
    asha_groups = df.dropna(subset=["Asha_Worker", "Aasha_Contact"])
    print(f"DEBUG: Records with Asha contacts: {len(asha_groups)}")
    
    links_to_open = []
    now_str = datetime.now().strftime("%d/%m %H:%M")
    notified_any = False
    
    ashas = asha_groups["Asha_Worker"].unique()
    print(f"DEBUG: Unique Ashas found: {len(ashas)}")

    for asha in ashas:
        asha_df = asha_groups[asha_groups["Asha_Worker"] == asha]
        contact = str(asha_df.iloc[0]["Aasha_Contact"]).strip()
        
        # Validate contact number
        if not contact or contact.lower() == "nan" or contact == "":
            continue

        # Filter out subjects already notified in this session/cache
        unnotified_subjects = []
        for _, row in asha_df.iterrows():
            key = f"{asha}_{row['ID']}"
            if key not in NOTIFIED_CACHE:
                unnotified_subjects.append(row)
        
        if unnotified_subjects:
            summary_parts = []
            for i, row in enumerate(unnotified_subjects, 1):
                cat = str(row.get("anemia_category", "Unknown")).capitalize()
                summary_parts.append(f"{i}. {row.get('Name')} ({row.get('ID')}) - {cat} Anemia (Hb: {row.get('HGB')})")
                NOTIFIED_CACHE[f"{asha}_{row['ID']}"] = now_str
            
            summary_text = "\n".join(summary_parts)
            msg = f"Hello {asha}, follow-up needed for these subjects:\n\n{summary_text}\n\nPlease check today."
            
            # Instead of opening immediately, we add to queue
            queue_item = {
                "id": f"{asha}_{datetime.now().timestamp()}_{len(links_to_open)}",
                "asha": asha,
                "contact": contact,
                "msg": msg,
                "summary": summary_text,
                "count": len(unnotified_subjects),
                "timestamp": now_str
            }
            current_queue.append(queue_item)
            notified_any = True

    if notified_any:
        # We no longer save cache here immediately because user might not actually SEND.
        # But for simplicity, we'll follow the same logic or let user reset.
        # Actually, let's keep the cache update so they don't reappear in "Bulk Notify" search.
        save_notified_cache() 
        success_msg = f"Added {len(ashas)} workers to the Notification Queue! Scroll down to manage."
        return None, True, success_msg, "success", current_queue
    
    fail_msg = "No new eligible subjects found for notification."
    return None, True, fail_msg, "warning", no_update

# Render Notification Queue Callback
@app.callback(
    Output("notification-queue-container", "children"),
    Input("notification-queue-data", "data"),
    prevent_initial_call=False
)
def render_notification_queue(queue):
    if not queue:
        return []
    
    cards = []
    # Header with Clear All
    cards.append(html.Div([
        html.H4([html.I(className="fas fa-list-ol me-2"), "Pending Notifications Queue"], 
               style={"fontWeight": "700", "margin": "0"}),
        dbc.Button("Clear All", id="btn-clear-queue", color="danger", outline=True, size="sm")
    ], className="d-flex justify-content-between align-items-center mb-3"))

    for item in queue:
        encoded_msg = urllib.parse.quote(item["msg"])
        wa_link = f"https://wa.me/{item['contact']}?text={encoded_msg}"
        
        card = dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.H5([html.I(className="fas fa-user-nurse me-2"), item["asha"]], className="mb-1", style={"fontWeight": "700"}),
                        html.P(f"Contact: {item['contact']}", className="text-muted mb-2", style={"fontSize": "0.85rem"}),
                        html.Div(item["summary"].replace("\n", " | "), style={"fontSize": "0.8rem", "color": "#475569", "whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis"})
                    ], width=8),
                    dbc.Col([
                        html.Div([
                            dbc.Button([html.I(className="fab fa-whatsapp me-2"), "Send"], 
                                       href=wa_link, target="_blank", color="success", size="sm", className="mb-2 w-100"),
                            dbc.Button("Remove", id={"type": "remove-queue", "index": item["id"]}, 
                                       color="secondary", outline=True, size="sm", className="w-100")
                        ])
                    ], width=4, className="text-end")
                ])
            ])
        ], className="mb-2 shadow-sm", style={"borderLeft": "5px solid #22c55e", "borderRadius": "10px"})
        cards.append(card)
        
    return cards

# Manage Queue (Remove/Clear) Callback
@app.callback(
    Output("notification-queue-data", "data", allow_duplicate=True),
    [Input("btn-clear-queue", "n_clicks"),
     Input({"type": "remove-queue", "index": ALL}, "n_clicks")],
    State("notification-queue-data", "data"),
    prevent_initial_call=True
)
def manage_queue(n_clear, n_removes, current_queue):
    if not callback_context.triggered:
        return no_update
        
    triggered_id = callback_context.triggered_id
    
    if triggered_id == "btn-clear-queue":
        return []
    
    # Handle pattern-matching callbacks (remove-queue button)
    if isinstance(triggered_id, dict) and triggered_id.get("type") == "remove-queue":
        target_id = triggered_id.get("index")
        
        # Ensure current_queue is a list before filtering
        if not isinstance(current_queue, list):
            return []
            
        new_queue = [item for item in current_queue if item["id"] != target_id]
        return new_queue
        
    return no_update

# Theme Toggle Logic
app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="toggle_theme"),
    Output("theme-store", "data"),
    Input("theme-toggle", "n_clicks"),
    Input("theme-toggle-mobile", "n_clicks"),
    State("theme-store", "data"),
    prevent_initial_call=True
)

# Clientside initialization to apply persistent theme from dcc.Store
app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="init_theme"),
    Output("theme-toggle", "children"),
    Input("theme-store", "data")
)

# Clientside callback - Modified to NOT trigger on data change anymore
app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="null_handler"),
    Output("bulk-notification-trigger", "children"),
    Input("bulk-notification-urls", "data")
)

# Clientside mobile menu toggle
app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="toggle_mobile_menu"),
    Output("mobile-toggle-trigger", "children"), # Unique dummy output
    Input("btn-toggle", "n_clicks"),
    prevent_initial_call=True
)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8090)

