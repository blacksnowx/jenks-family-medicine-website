import pandas as pd
import numpy as np
import os

try:
    from . import data_loader
except ImportError:
    import data_loader

ESTABLISHED_EM = {'99211', '99212', '99213', '99214', '99215'}
NEW_PATIENT_EM = {'99202', '99203', '99204', '99205'}
PHYSICAL_CODES = {
    '99381', '99382', '99383', '99384', '99385', '99386', '99387',
    '99391', '99392', '99393', '99394', '99395', '99396', '99397',
    'G0402', 'G0438', 'G0439'
}
PROVIDER_FACETIME_CODES = ESTABLISHED_EM | NEW_PATIENT_EM | PHYSICAL_CODES

def analyze_provider(df_pc, provider_name):
    """
    Computes provider-specific metrics around E&M encounters, new vs recurring patient counts, and collections.
    """
    if 'Date Of Service' not in df_pc.columns:
        return {}
        
    df = df_pc.copy()
    if 'Week' not in df.columns:
        df['Week'] = data_loader.get_week_ending_friday(df['Date Of Service'])
        
    # Handle the fact that Provider name might have trailing spaces or casing changes
    df['Rendering Provider'] = df['Rendering Provider'].astype(str).str.upper().str.strip()
    search_prov = str(provider_name).upper().strip()
    provider_df = df[df['Rendering Provider'] == search_prov].copy()
    
    if len(provider_df) == 0:
        return {}
        
    provider_df['Is_Facetime_Code'] = provider_df['Procedure Code'].astype(str).isin(PROVIDER_FACETIME_CODES)
    
    if 'Encounter ID' in provider_df.columns:
        provider_facetime_encounters = provider_df[provider_df['Is_Facetime_Code']]['Encounter ID'].unique()
        provider_df['Is_Provider_Encounter'] = provider_df['Encounter ID'].isin(provider_facetime_encounters)
        
        visits = provider_df[provider_df['Is_Provider_Encounter']].drop_duplicates(subset='Encounter ID').copy()
        
        if 'Patient ID' in df.columns:
            first_clinic_visit = df.groupby('Patient ID')['Date Of Service'].min().reset_index()
            first_clinic_visit.columns = ['Patient ID', 'First_Clinic_Visit']
            visits = visits.merge(first_clinic_visit, on='Patient ID', how='left')
            
            encounter_codes = provider_df.groupby('Encounter ID')['Procedure Code'].apply(lambda x: set(x.astype(str))).reset_index()
            encounter_codes.columns = ['Encounter ID', 'Code_Set']
            visits = visits.merge(encounter_codes, on='Encounter ID', how='left')
            
            def is_new_patient(row):
                codes = row['Code_Set'] if isinstance(row['Code_Set'], set) else set()
                if codes & NEW_PATIENT_EM:
                    return True
                if codes & PHYSICAL_CODES:
                    if row['Date Of Service'] == row['First_Clinic_Visit']:
                        return True
                return False
            
            visits['Is_New'] = visits.apply(is_new_patient, axis=1)
        else:
            visits['Is_New'] = False
            
        weekly_visits = visits.groupby('Week').agg(
            Total_Visits=('Encounter ID', 'count'),
            New_Patients=('Is_New', 'sum'),
        ).reset_index()
        weekly_visits['Recurring_Patients'] = weekly_visits['Total_Visits'] - weekly_visits['New_Patients']
        weekly_visits = weekly_visits.sort_values('Week').reset_index(drop=True)
        
        # Make sure PC_Allowed / PC_Revenue exist
        if 'PC_Allowed' not in provider_df.columns:
            provider_df['PC_Allowed'] = 0.0
        if 'PC_Revenue' not in provider_df.columns:
            provider_df['PC_Revenue'] = 0.0
            
        weekly_financial = provider_df.groupby('Week').agg(
            Total_Charges=('Service Charge Amount', 'sum'),
            Total_Allowed=('PC_Allowed', 'sum'),
            Total_Collected=('PC_Revenue', 'sum'),
        ).reset_index()
        
        weekly_merged = weekly_visits.merge(weekly_financial, on='Week', how='outer').fillna(0)
        weekly_merged = weekly_merged.sort_values('Week')
        
        return {
            'total_encounters_tagged': provider_df['Encounter ID'].nunique(),
            'provider_visits': len(provider_facetime_encounters),
            'unique_patients': provider_df['Patient ID'].nunique(),
            'weekly_data': weekly_merged
        }
    return {}
