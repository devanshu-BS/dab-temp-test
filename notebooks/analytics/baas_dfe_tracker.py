import pandas as pd
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from geopy.distance import geodesic
import pandas as pd
import numpy as np
from datetime import datetime
import h3
import time
import multiprocessing as mp
from functools import partial
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
import os, sys
sys.path.append('../../')
sys.path.append('../../src/utils')
from queryHelper import *
from sheetHelper import *
from databaseHelper import *
from geospatialHelper import *
from tqdm import tqdm
import logging
import time
from functools import partial
import json



def wkb_to_latlon(wkb_hex):
    point = wkb.loads(bytes.fromhex(wkb_hex))
    return point.y, point.x

def summarize_day_entries(df):
    if (df['locationTime'] == 'Driver onboarded').any():
        return 'Driver onboarded'
    elif (df['locationTime'] == 'absent').all():
        return 'Absent'
    else:
        time_sum = pd.to_timedelta(df['locationTime'], errors='coerce').sum()
        return round(time_sum.total_seconds() / 3600, 2)

def compute_efficiency_loss(val1, val2):
    if isinstance(val1, (int, float)) and isinstance(val2, (int, float)) and val2 != 0:
        return round((1 - (val1 / val2)) * 100, 2)
    elif val1 in ['Driver onboarded', 'Absent']:
        return val1
    else:
        return np.nan

def opsProd(query):
    import psycopg2
    sqlConnection = psycopg2.connect(
        host='operation.replica.upgrid.in',
        user='deepak_singh1',
        password='Deepak@12345',
        dbname='operations_manager_prod'
    )
    cursor = sqlConnection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=colnames)
    cursor.close()
    sqlConnection.close()
    return df

def main():
    today = pd.Timestamp.today().normalize()
    start_date = pd.Timestamp('2025-08-01').normalize()
    date_range = pd.date_range(start=start_date, end=today, freq='D')
    
    # Read FE data
    df1 = read("https://docs.google.com/spreadsheets/d/1cavrsMh9m0GdoYPkuLBDHbinYvX4qChHtPudPDP2xYQ/edit?gid=278491000#gid=278491000", "FE Tracker")
    df2 = read("https://docs.google.com/spreadsheets/d/1cavrsMh9m0GdoYPkuLBDHbinYvX4qChHtPudPDP2xYQ/edit?gid=278491000#gid=278491000", "GM Mapping")
    
    df_merge = df1.merge(df2, left_on="Zone Id", right_on='Zone', how='left')
    df_merge.columns = df_merge.columns.str.replace('\n', ' ', regex=False).str.strip()
    
    filtered_df = df_merge[
        ((df_merge['Designation'] == 'DTFE - BaaS') | (df_merge['Designation'] == 'Demand FE - BaaS')) & 
        (df_merge['FE Status'] == 'Active')
    ]
    
    selected_columns = filtered_df[['Emp. Code', 'Name', 'Designation', 'Zone Id', 'DOJ', 'AON','Reporting  Manager', 'Growth Manager','Mobile Number']]
    sorted_df = selected_columns.sort_values(by='Zone Id', ascending=True).reset_index(drop=True)
    sorted_df = sorted_df.drop_duplicates(subset=['Emp. Code']) 

    # Fetch leads and onboarding data
    lead_query = '''select dl.id,dl.entryById, dl.zoneId, dl.vehicleType,dl.driverLeadType, date(DATE_ADD(dl.createdAt, interval 330 minute))createdAt , dl.isBaaSDriver 
    from driverLeads dl
    WHERE date(dl.createdAt) >= DATE_SUB(CURDATE(),INTERVAL 60 DAY)
    and dl.source not in ('client') 
    and dl.entryById like '%USC%' '''
    
    ob_query = '''select d.id as driverid,
    case when d.vehicleType in ('E-2w') then '2w' else '3w' end vehicletype,d.liveDate,
    dl.sourceId,t.assignedTo,d.zoneId,d.isBaaSDriver 
    from drivers d
    left join driverLeads dl  on dl.driverId  = d.id
    left JOIN tasks t on d.id = t.customerId and t.taskTypeId in (3,7)
    where date(d.liveDate) >=  DATE_SUB(CURDATE(),INTERVAL 2 MONTH)
    and d.clientId = "BS00" and d.isDeliveryDriver =0  
    ORDER by d.liveDate'''
    
    new_db_OB = '''select sc.personalized_for AS smart_card_driver_id,
    v.zone, v.lead_id, a1.employee_id as kyc_by
    from vehicle_owner_onboardings v
    join tasks t on v.task_id = t.id
    join tasks PhoneNumberVerification on PhoneNumberVerification.parent_id  = t.id and PhoneNumberVerification.type = 'Tasks::PhoneNumberVerification'
    join assignees a1 on a1.task_id  = PhoneNumberVerification.id
    JOIN tasks assign_smart_card_task ON assign_smart_card_task.parent_id = t.id AND assign_smart_card_task.type = 'Tasks::AssignSmartCard'
    JOIN smart_cards sc ON sc.task_id = assign_smart_card_task.id
    where v.deleted_at IS null 
    and (v.created_at + INTERVAL '330 MINUTE')::DATE >= (CURRENT_DATE - INTERVAL '2 MONTH')::DATE 
    AND t.status = 'qc_passed'
    ORDER by v.created_at DESC'''
    
    # NEW: Fetch retro data
    ob_query_retro = '''select d.id as driverid,
    case when d.vehicleType in ('E-2w') then '2w' else '3w' end vehicletype,d.liveDate,
    dl.sourceId,t.assignedTo,d.zoneId,d.isBaaSDriver 
    from drivers d
    left join driverLeads dl on dl.driverId = d.id
    left JOIN tasks t on d.id = t.customerId and t.taskTypeId in (3,7)
    where date(d.liveDate) >= DATE_SUB(CURDATE(),INTERVAL 60 day)
    and d.clientId = "BS00" 
    ORDER by d.liveDate'''
    
    new_db_OB_retro = '''select sc.personalized_for AS driver_id,
    v.zone,
    v.lead_id,
    a1.employee_id as kyc_by,
    a2.employee_id as retro_by
    from vehicle_owner_onboardings v
    join tasks t on v.task_id = t.id
    join tasks PhoneNumberVerification on PhoneNumberVerification.parent_id = t.id and PhoneNumberVerification.type = 'Tasks::PhoneNumberVerification'
    join assignees a1 on a1.task_id = PhoneNumberVerification.id
    join tasks VehicleRetrofitment on VehicleRetrofitment.parent_id = t.id and VehicleRetrofitment.type = 'Tasks::VehicleRetrofitment'
    join assignees a2 on a2.task_id = VehicleRetrofitment.id
    JOIN tasks assign_smart_card_task ON assign_smart_card_task.parent_id = t.id AND assign_smart_card_task.type = 'Tasks::AssignSmartCard'
    JOIN smart_cards sc ON sc.task_id = assign_smart_card_task.id
    where v.deleted_at IS null 
    and (v.created_at + INTERVAL '330 MINUTE')::DATE >= (CURRENT_DATE - INTERVAL '60 day')::DATE 
    AND t.status = 'qc_passed'
    ORDER by v.created_at DESC'''
    
    ob_query1 = prodFetch(ob_query)
    lead_query1 = prodFetch(lead_query)
    new_db_OB1 = opsProd(new_db_OB)
    
    # NEW: Fetch retro data
    ob_query1_retro = prodFetch(ob_query_retro)
    new_db_OB1_retro = opsProd(new_db_OB_retro)
    
    ob_query2 = ob_query1.merge(new_db_OB1[['smart_card_driver_id','kyc_by']], how='left', left_on='driverid', right_on='smart_card_driver_id')
    ob_query2['newassignedto'] = np.where(ob_query2['kyc_by'].isnull(), ob_query2['assignedTo'], ob_query2['kyc_by'])
    ob_query2 = ob_query2.drop(columns=['assignedTo', 'kyc_by', 'smart_card_driver_id'])
    
    # NEW: Process retro data
    ob_query2_retro = ob_query1_retro.merge(new_db_OB1_retro[['driver_id','kyc_by','retro_by']], 
                                          how='left', 
                                          left_on='driverid', 
                                          right_on='driver_id')
    ob_query2_retro['newassignedto'] = np.where(ob_query2_retro['kyc_by'].isnull(), 
                                              ob_query2_retro['assignedTo'], 
                                              ob_query2_retro['kyc_by'])
    ob_query2_retro = ob_query2_retro.drop(columns=['assignedTo', 'kyc_by', 'driver_id'])
    
    # Read Valid Lead data
    try:
        valid_leads = read("https://docs.google.com/spreadsheets/d/1MzhruxWEtbkqT9UBCkxg1ZiLvlCO3rviI-keYBnDCGk/edit?gid=1509336840#gid=1509336840", "Valid")
        if valid_leads is None or valid_leads.empty:
            print("Warning: Valid leads data is empty or None")
            valid_leads = pd.DataFrame(columns=['Entry By ID', 'Lead Remark', 'Created at'])
    except Exception as e:
        print(f"Error reading valid leads data: {e}")
        valid_leads = pd.DataFrame(columns=['Entry By ID', 'Lead Remark', 'Created at'])
    
    # Preprocess data for fast computation
    lead_query1['createdAt'] = pd.to_datetime(lead_query1['createdAt'])
    lead_query1['createdAt_norm'] = lead_query1['createdAt'].dt.normalize()
    lead_query1['entryById_str'] = lead_query1['entryById'].astype(str).str.strip()
    lead_query1['vehicleType_str'] = lead_query1['vehicleType'].astype(str).str.strip()
    lead_query1['vehicleType_isna'] = lead_query1['vehicleType'].isna()
    lead_query1['driverLeadType_str'] = lead_query1['driverLeadType'].astype(str).str.strip()

    
    ob_query2['liveDate'] = pd.to_datetime(ob_query2['liveDate'])
    ob_query2['liveDate_norm'] = ob_query2['liveDate'].dt.normalize()
    ob_query2['sourceId_str'] = ob_query2['sourceId'].astype(str).str.strip()
    ob_query2['newassignedto_str'] = ob_query2['newassignedto'].astype(str).str.strip()
    ob_query2['vehicletype_str'] = ob_query2['vehicletype'].astype(str).str.strip()
    
    # NEW: Preprocess retro data
    ob_query2_retro['liveDate'] = pd.to_datetime(ob_query2_retro['liveDate'])
    ob_query2_retro['liveDate_norm'] = ob_query2_retro['liveDate'].dt.normalize()
    ob_query2_retro['retro_by_str'] = ob_query2_retro['retro_by'].astype(str).str.strip()
    ob_query2_retro['vehicletype_str'] = ob_query2_retro['vehicletype'].astype(str).str.strip()
    
    def count_leads_fast(emp_code, vehicle_type_values, is_baas, date):
        emp_str = str(emp_code).strip()
        mask = lead_query1['entryById_str'] == emp_str
        
        if is_baas is not None:
            mask &= lead_query1['driverLeadType_str'].isin(['leaseToOwn', 'baas','swap'])
        
        if vehicle_type_values:
            vehicle_mask = (lead_query1['vehicleType_isna'] & ('NaN' in vehicle_type_values))
            for vt in [v for v in vehicle_type_values if v != 'NaN']:
                vehicle_mask |= (lead_query1['vehicleType_str'] == str(vt).strip())
            mask &= vehicle_mask
        
        date_norm = pd.to_datetime(date).normalize()
        mask &= lead_query1['createdAt_norm'] == date_norm
        
        return mask.sum()
    
    def count_ob_fast(emp_code, vehicle_type, is_baas, date):
        emp_str = str(emp_code).strip()
        vt_str = str(vehicle_type).strip()
        mask = (ob_query2['sourceId_str'] == emp_str) & (ob_query2['vehicletype_str'] == vt_str) & (ob_query2['isBaaSDriver'] == is_baas)
        
        date_norm = pd.to_datetime(date).normalize()
        mask &= ob_query2['liveDate_norm'] == date_norm
        
        return mask.sum()
    
    def count_kyc_fast(emp_code, vehicle_type, is_baas, date):
        emp_str = str(emp_code).strip()
        vt_str = str(vehicle_type).strip()
        mask = (ob_query2['newassignedto_str'] == emp_str) & (ob_query2['vehicletype_str'] == vt_str) & (ob_query2['isBaaSDriver'] == is_baas)
        
        date_norm = pd.to_datetime(date).normalize()
        mask &= ob_query2['liveDate_norm'] == date_norm
        
        return mask.sum()
    
    # NEW: Function to count retro
    def count_retro_fast(emp_code, vehicle_type, is_baas, date):
        emp_str = str(emp_code).strip()
        vt_str = str(vehicle_type).strip()
        mask = (ob_query2_retro['retro_by_str'] == emp_str) & (ob_query2_retro['vehicletype_str'] == vt_str) & (ob_query2_retro['isBaaSDriver'] == is_baas)
        
        date_norm = pd.to_datetime(date).normalize()
        mask &= ob_query2_retro['liveDate_norm'] == date_norm
        
        return mask.sum()
    
    def count_invalid_leads(emp_code, date):
        try:
            if valid_leads is None or valid_leads.empty:
                return 0
            
            if 'Entry By ID' not in valid_leads.columns or 'Lead Remark' not in valid_leads.columns or 'Created at' not in valid_leads.columns:
                return 0
            
            emp_str = str(emp_code).strip()
            date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
            
            valid_leads_clean = valid_leads.copy()
            valid_leads_clean['Created at'] = pd.to_datetime(valid_leads_clean['Created at'], errors='coerce')
            valid_leads_clean = valid_leads_clean.dropna(subset=['Created at'])
            valid_leads_clean['Created at'] = valid_leads_clean['Created at'].dt.strftime('%Y-%m-%d')
            
            mask = (valid_leads_clean['Entry By ID'].astype(str).str.strip() == emp_str) & \
                   (valid_leads_clean['Lead Remark'].astype(str).str.strip() == 'Invalid') & \
                   (valid_leads_clean['Created at'] == date_str)
            
            return mask.sum()
        except Exception as e:
            print(f"Error calculating invalid leads for {emp_code} on {date}: {e}")
            return 0
    
    def calculate_employee_metrics_batch(batch_data):
        results = []
        
        for idx, row in batch_data:
            emp_code = row['Emp. Code']
            employee_results = {}
            
            for date in date_range:
                date_str = date.strftime('%d/%m/%Y')
                
                leads_count = count_leads_fast(emp_code, ['E-3w', 'NaN'], True, date)
                ob_count = count_ob_fast(emp_code, '3w', 1, date)
                kyc_count = count_kyc_fast(emp_code, '3w', 1, date)
                retro_count = count_retro_fast(emp_code, '3w', 1, date)  # NEW: Count retro
                invalid_leads_count = count_invalid_leads(emp_code, date)
                
                employee_results[(date_str, 'Lead')] = leads_count
                employee_results[(date_str, 'OB')] = ob_count
                employee_results[(date_str, 'KYC')] = kyc_count
                employee_results[(date_str, 'Retro')] = retro_count  # NEW: Add retro
                employee_results[(date_str, 'Valid Lead')] = leads_count - invalid_leads_count
            
            results.append((idx, employee_results))
        return results
    
    # Calculate metrics using threading
    metrics_columns = []
    for date in date_range:
        date_str = date.strftime('%d/%m/%Y')
        metrics_columns.extend([
            (date_str, 'Lead'),
            (date_str, 'OB'),
            (date_str, 'KYC'),
            (date_str, 'Retro'),  # NEW: Add retro column
            (date_str, 'Valid Lead')
        ])
    
    columns = pd.MultiIndex.from_tuples([('', col) for col in sorted_df.columns] + metrics_columns)
    merged_df = pd.DataFrame(columns=columns)
    
    for col in sorted_df.columns:
        merged_df[('', col)] = sorted_df[col]
    
    employee_data = list(sorted_df.iterrows())
    batch_size = max(1, len(employee_data) // 16)
    batches = [employee_data[i:i + batch_size] for i in range(0, len(employee_data), batch_size)]
    
    all_results = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(calculate_employee_metrics_batch, batch) for batch in batches]
        for future in tqdm(futures, desc="Processing batches"):
            all_results.extend(future.result())
    
    for idx, employee_results in all_results:
        for key, value in employee_results.items():
            merged_df.loc[idx, key] = value
    
    # Add Days and Age Cohort columns
    merged_df[('', 'Days')] = (today - pd.to_datetime(sorted_df['DOJ'])).dt.days
    
    def get_age_cohort(days):
        if days < 15:
            return "0-15"
        elif days < 30:
            return "15-30"
        elif days < 60:
            return "30-60"
        else:
            return "60+"
    
    merged_df[('', 'Age Cohort')] = merged_df[('', 'Days')].apply(get_age_cohort)
    
    # Update fillna to include Retro column
    merged_df = merged_df.fillna(0).astype({col: int for col in merged_df.columns if col[1] in ['Lead', 'OB', 'KYC', 'Retro', 'Valid Lead']})
    
    # Calculate efficiency (rest of the efficiency calculation code remains the same)
    today_date = datetime.now().date()
    start_date_dt = datetime(2025, 8, 1).date()
    
    temp = visionFetch(f"""
        SELECT 
          employee_id, 
          location, 
          set_at AS currentTime 
        FROM 
          userlocationlogs 
        WHERE 
          employee_id LIKE 'USC%' 
          AND set_at BETWEEN DATE '{start_date_dt}' AND DATE '{today_date}' + INTERVAL '1 day' - INTERVAL '1 second'
          AND deleted_at IS NULL
    """)
    
    if not temp.empty:
        tf = databricksDevFetch("select * from deltalake.silver.driver_agg_locations")
        
        temp[['lat', 'lon']] = temp['location'].apply(lambda wkb_hex: pd.Series(wkb_to_latlon(wkb_hex)))
        temp['h3_res10'] = temp.apply(lambda row: h3.latlng_to_cell(row['lat'], row['lon'], 10), axis=1)
        temp['currenttime'] = pd.to_datetime(temp['currenttime']).dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
        temp = temp[temp['currenttime'].dt.hour >= 7]
        temp['date'] = temp['currenttime'].dt.date
        temp = temp.sort_values(['employee_id', 'currenttime'])
        
        temp['h3_change'] = temp.groupby('employee_id')['h3_res10'].transform(lambda x: x != x.shift())
        temp['streak_id'] = temp.groupby('employee_id')['h3_change'].cumsum()
        
        result = (temp.groupby(['employee_id', 'h3_res10', 'streak_id', 'date'])
                  .agg(start_time=('currenttime', 'min'), end_time=('currenttime', 'max'))
                  .reset_index(drop=False))
        
        result['locationTime'] = result['end_time'] - result['start_time']
        result['locationTime'] = result['locationTime'].fillna(pd.Timedelta(0))
        
        flat_zones = []
        for _, row in tf.iterrows():
            for loc in row['locations']:
                flat_zones.append({
                    'driverId': row['driverId'],
                    'latitude': loc['lat'],
                    'longitude': loc['lon']
                })
        
        demand_zones = pd.DataFrame(flat_zones)
        demand_zones['h3_res10'] = demand_zones.apply(lambda row: h3.latlng_to_cell(row['latitude'], row['longitude'], 10), axis=1)
        demand_zones['h3_parent_res9'] = demand_zones['h3_res10'].apply(lambda x: h3.cell_to_parent(x, 9))
        
        result['h3_parent_res9'] = result['h3_res10'].apply(lambda x: h3.cell_to_parent(x, 9))
        dz_parent_counts = demand_zones['h3_parent_res9'].value_counts().to_dict()
        result['parent_zone_count'] = result['h3_parent_res9'].map(dz_parent_counts).fillna(0).astype(int)
        
        filtered_result = result[result['parent_zone_count'] < 20].copy()
        filtered_result = filtered_result.drop(columns=['h3_parent_res9', 'parent_zone_count'])
        
        # Create summary table from filtered result
        summary_table = (filtered_result.groupby(['employee_id', 'date'])
                         .apply(summarize_day_entries)
                         .unstack(fill_value=0))
        summary_table = summary_table.reset_index()
        summary_table.columns = summary_table.columns.map(str)
        
        # Handle onboarding status for efficiency
        daily_onboardings = pd.DataFrame()
        
        for idx, row in sorted_df.iterrows():
            emp_code = row['Emp. Code']
            dfe_data = {'employee_id': emp_code}
            
            for single_date in date_range:
                date_key = single_date.strftime('%Y-%m-%d')
                daily_total = (count_ob_fast(emp_code, '3w', 0, single_date) + 
                              count_ob_fast(emp_code, '3w', 1, single_date) + 
                              count_ob_fast(emp_code, '2w', 0, single_date))
                dfe_data[date_key] = daily_total
            
            daily_onboardings = pd.concat([daily_onboardings, pd.DataFrame([dfe_data])], ignore_index=True)
        
        daily_onboardings = daily_onboardings.fillna(0)
        date_cols = [col for col in daily_onboardings.columns if col not in ['employee_id']]
        daily_onboardings[date_cols] = daily_onboardings[date_cols].astype(int)
        
        df_onboard = daily_onboardings.copy()
        date_cols = [col for col in df_onboard.columns if pd.to_datetime(col, errors='coerce') is not pd.NaT]
        filtered_df_onboard = df_onboard[['employee_id'] + date_cols].copy()
        filtered_df_onboard[date_cols] = filtered_df_onboard[date_cols].applymap(lambda x: 'Driver onboarded' if x != 0 else 0)
        
        summary_table['employee_id'] = summary_table['employee_id'].astype(str)
        filtered_df_onboard['employee_id'] = filtered_df_onboard['employee_id'].astype(str)
        summary_table.columns = [str(col) for col in summary_table.columns]
        filtered_df_onboard.columns = [str(col) for col in filtered_df_onboard.columns]
        
        date_columns = [col for col in summary_table.columns if col != 'employee_id' and col in filtered_df_onboard.columns]
        
        summary_melted = summary_table.melt(id_vars='employee_id', value_vars=date_columns, var_name='date', value_name='value')
        df_melted = filtered_df_onboard.melt(id_vars='employee_id', value_vars=date_columns, var_name='date', value_name='onboard_status')
        
        merged_summary = pd.merge(summary_melted, df_melted, on=['employee_id', 'date'], how='left')
        merged_summary['final_value'] = merged_summary.apply(
            lambda row: 'Driver onboarded' if row['onboard_status'] == 'Driver onboarded' else row['value'], axis=1)
        
        updated_summary = merged_summary.pivot(index='employee_id', columns='date', values='final_value').reset_index()
        sorted_dates = sorted(date_columns, key=lambda x: str(x))
        updated_summary = updated_summary[['employee_id'] + sorted_dates]
        summary_table = updated_summary.copy()
        
        ping_window = (temp.groupby(['employee_id', 'date'])['currenttime']
                       .agg(first_ping='min', last_ping='max').reset_index())
        ping_window['ping_duration_hrs'] = ((ping_window['last_ping'] - ping_window['first_ping']).dt.total_seconds() / 3600).round(2)
        
        pivoted = ping_window.pivot(index='employee_id', columns='date', values='ping_duration_hrs').reset_index()
        
        summary_table.columns = [str(col) for col in summary_table.columns]
        pivoted.columns = [str(col) for col in pivoted.columns]
        summary_table['employee_id'] = summary_table['employee_id'].astype(str)
        pivoted['employee_id'] = pivoted['employee_id'].astype(str)
        
        merged_eff = pd.merge(summary_table, pivoted, on='employee_id', suffixes=('_summary', '_pivoted'))
        date_cols = [col for col in summary_table.columns if col != 'employee_id']
        common_cols = [col for col in date_cols if col in pivoted.columns]
        
        efficiency_result = pd.DataFrame()
        efficiency_result['employee_id'] = merged_eff['employee_id']
        
        for col in common_cols:
            summary_col = f"{col}_summary"
            pivoted_col = f"{col}_pivoted"
            efficiency_result[col] = merged_eff.apply(lambda row: compute_efficiency_loss(row[summary_col], row[pivoted_col]), axis=1)
        
        efficiency_result = efficiency_result[['employee_id'] + common_cols]
        efficiency_result = efficiency_result[efficiency_result['employee_id'].isin(sorted_df['Emp. Code'])]
        efficiency_result = efficiency_result.fillna("No Ping : Not using Battman")
        efficiency_result = efficiency_result.replace("No data", "No Ping : Not using Battman")
        efficiency_result = efficiency_result.replace(0, "No Ping : Not using Battman")
        
        # Add efficiency columns to main dataframe
        for date in date_range:
            date_str = date.strftime('%d/%m/%Y')
            date_key = date.strftime('%Y-%m-%d')
            
            if date_key in efficiency_result.columns:
                eff_dict = dict(zip(efficiency_result['employee_id'], efficiency_result[date_key]))
                merged_df[(date_str, 'Efficiency')] = merged_df[('', 'Emp. Code')].map(eff_dict).fillna("No Ping : Not using Battman")
            else:
                merged_df[(date_str, 'Efficiency')] = "No Ping : Not using Battman"
    
    # Get attendance data
    attendance = prodFetch("select employeeId,status,date from dailyAttendances")
    attendance['date'] = pd.to_datetime(attendance['date']).dt.date
    attendance = attendance.rename(columns={'employeeId': 'employee_id'})
    
    # Add attendance columns
    for date in date_range:
        date_str = date.strftime('%d/%m/%Y')
        date_value = date.date()
        
        attendance_dict = dict(zip(
            attendance[attendance['date'] == date_value]['employee_id'],
            attendance[attendance['date'] == date_value]['status']
        ))
        
        merged_df[(date_str, 'Attendance')] = merged_df[('', 'Emp. Code')].map(attendance_dict).fillna("No Data")
        
        # UPDATED: Add new derived attendance columns with Retro included
        valid_lead_col = (date_str, 'Valid Lead')
        kyc_col = (date_str, 'KYC')
        ob_col = (date_str, 'OB')
        retro_col = (date_str, 'Retro')  # NEW: Add retro column reference
        
        # NEW: Derived Attendance: 1 if any of KYC, Lead, OB, or Retro >= 1
        merged_df[(date_str, 'Derived Attendance')] = merged_df.apply(
            lambda row: 1 if (row[kyc_col] >= 1 or row[valid_lead_col] >= 1 or row[ob_col] >= 1 or row[retro_col] >= 1) else 0, axis=1
        )
        
        # Derived Attendance with 3 valid leads: 1 if valid leads >=3 or KYC >=1 or OB >=1 or Retro >=1
        merged_df[(date_str, 'Derived Attendance with 3 valid leads')] = merged_df.apply(
            lambda row: 1 if (row[valid_lead_col] >= 3 or row[kyc_col] >= 1 or row[ob_col] >= 1 or row[retro_col] >= 1) else 0, axis=1
        )
        
        # Derived Attendance with 5 valid leads: 1 if valid leads >=5 or KYC >=1 or OB >=1 or Retro >=1
        merged_df[(date_str, 'Derived Attendance with 5 valid leads')] = merged_df.apply(
            lambda row: 1 if (row[valid_lead_col] >= 5 or row[kyc_col] >= 1 or row[ob_col] >= 1 or row[retro_col] >= 1) else 0, axis=1
        )

    # Reorganize columns - UPDATED to include Retro
    base_columns = [
        ('', 'Emp. Code'), ('', 'Name'), ('', 'Designation'), ('', 'Zone Id'),
        ('', 'DOJ'), ('', 'Days'), ('', 'Age Cohort'), ('', 'AON'), ('', 'Reporting  Manager')
    ]
    
    date_columns = []
    for date in date_range:
        date_str = date.strftime('%d/%m/%Y')
        date_columns.extend([
            (date_str, 'Lead'),
            (date_str, 'OB'),
            (date_str, 'KYC'),
            (date_str, 'Retro'),  # NEW: Add retro to column order
            (date_str, 'Valid Lead'),
            (date_str, 'Efficiency'),
            (date_str, 'Attendance'),
            (date_str, 'Derived Attendance'),  # NEW: Add base derived attendance
            (date_str, 'Derived Attendance with 3 valid leads'),
            (date_str, 'Derived Attendance with 5 valid leads')
        ])
    
    final_columns = base_columns + date_columns
    merged_df = merged_df[final_columns]
    
    # Sort by Age Cohort
    cohort_order = {"0-15": 1, "15-30": 2, "30-60": 3, "60+": 4}
    merged_df['sort_order'] = merged_df[('', 'Age Cohort')].map(cohort_order)
    merged_df = merged_df.sort_values('sort_order').drop('sort_order', axis=1).reset_index(drop=True)
    
    print("Final dataframe shape:", merged_df.shape)
    print("Sample data:")
    print(merged_df.head())
    
    # Write to output location or clipboard
    write("https://docs.google.com/spreadsheets/d/1MzhruxWEtbkqT9UBCkxg1ZiLvlCO3rviI-keYBnDCGk/edit?gid=1018237481#gid=1018237481","Sheet14",merged_df)
    
    return merged_df

if __name__ == "__main__":
    result = main()
