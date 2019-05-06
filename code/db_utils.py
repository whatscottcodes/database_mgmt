#!/usr/bin/env python3

import os
import pandas as pd
import numpy as np
import warnings
import sqlite3
from functools import reduce
import datetime
from db_rename_cols import *
from geomap import geolocate_addresses
import shutil
from titlecase import titlecase
import distutils.dir_util

warnings.simplefilter(action='ignore', category=FutureWarning)


def create_id_col(df, pk, id_col, create_col=True):

    member_ints = df[pk[0]].astype(int)
    date_ints = df[pk[1]].dt.strftime("%Y%m%d").astype(int)
    
    if create_col:
        df[id_col] = member_ints + date_ints
       
    if df[id_col].duplicated().sum() != 0:
        df[id_col] += df[id_col].duplicated()
        create_id_col(df, pk, id_col, create_col=False)
    
    df.drop_duplicates(subset = [col for col in df.columns if col != id_col], inplace=True)
    
    return df


def code_y_n(df):
    """
    Takes a pandas dataframe and codes any columns containing Yes/No as 1/0
    """
    for col in df.columns:
        if 'No' in df[col].unique():
            df[col] = df[col].str.title()
            df[col].replace({'Yes':1, 'No':0}, inplace=True)

def build_immunization_status(df, contra, pneumo=True):
    #code administered as 1/not administered as 0
    df['immunization_status'] = np.where(df['Immunization: Dose Status'] == 'Administered',
                                         1, 0)

    #change status of ppts alergic to the vaccine
    contra_df = df[df.member_id.isin(contra['member_id'].unique())]
    admin_contra = contra_df[contra_df.immunization_status == 1]['member_id'].unique()
    contra_ppts = [member_id for member_id in contra['member_id'].unique() if member_id not in admin_contra]
    
    contra_index = df[df['member_id'].isin(contra_ppts)].index
    
    for i in contra_index:
        if df.at[i, 'immunization_status'] == 0:
            df.at[i, 'immunization_status'] = 99
    
    #list of ppts who have had the vaccine
    admin_members = df[df['immunization_status'] == 1].member_id.unique()

    #list of ppts who have had a not administered interaction
    not_admin_members = df[df['immunization_status'] == 0].member_id

    #check for any ppts who have had both a not administered & administered interaction
    not_admin_now_admin = not_admin_members[not_admin_members.isin(admin_members)].values
    
    if pneumo:
        mask = ((df.member_id.isin(not_admin_now_admin)) 
                & (df['immunization_status'] == 0))

        #remove any the not administered record for any ppts who have
        #had both records indicated
    
        if len(not_admin_now_admin) != 0:
            df.drop(df[mask].index, inplace=True)
    
    df.sort_values('date_administered', inplace=True)
    
    return df

def discharge_admit_diff(df, sql_table='', update=False, admit_diff=False, admit_type=''):
    dff = df.copy()
    if update:
        if admit_type != '':
            admission_sql = f"AND admission_type == '{admit_type}'"
        else:
            admission_sql = ''
        conn = sqlite3.connect(db_name)

        q = f"""SELECT {','.join([col for col in df.columns if col != 'merge'])} FROM {sql_table}
        WHERE member_id IN {tuple(df.member_id.unique())}
        {admission_sql};"""

        current_table = pd.read_sql(q, conn, parse_dates = [col for col in df.columns if 'date' in col])

        df = current_table.append(df, sort=False)
        df.reset_index(inplace=True, drop=True)
        
    if admit_diff:
        diff_date = 'admission_date'
        sorted_df = df.sort_values(['member_id', 'admission_date'], 
                               ascending=False).reset_index(drop=True).copy()
    else:
        diff_date = 'discharge_date'
        sorted_df = df.sort_values(['member_id', 'admission_date', 'discharge_date'], 
                               ascending=False).reset_index(drop=True).copy()
    #sort dataframe by member_id and then admission date
    
    
    sorted_df['days_since_last_admission'] = np.nan

    #iterate through unique member_ids
    for mem_id in sorted_df.member_id.unique():
        #if the member_id appears more than once in the df
        if sorted_df[sorted_df.member_id == mem_id].shape[0] > 1:
            #iterate through each occurrence, first occurrence
            #will be the most recent admission_date
            for i in sorted_df[sorted_df.member_id == mem_id].index[:-1]:
                #find difference between current admission_date and
                #most recent discharge_date
                sorted_df.at[i, 'days_since_last_admission'] = (sorted_df.at[i, 'admission_date'] - sorted_df.at[(i+1), diff_date]) / np.timedelta64(1, 'D')

    sorted_df.reset_index(drop=True, inplace=True)
    if update:
        dff = dff.merge(sorted_df[['member_id', 'admission_date', 'days_since_last_admission']],
                        on=['member_id', 'admission_date'], how='left')
        return dff
    
    return sorted_df

def get_csv_files():
    
    files = os.listdir('.\\data')

    folders = [folder for folder in files if 'csv' not in folder and folder != 'archive']
    files = [file for file in files if 'csv' in file and 'statewide' not in file]
    tables = {}

    for file in files:
        tables[file[:-4]] = pd.read_csv(f".\\data\\{file}", low_memory=False)

    for table in tables.keys():
        date_cols = [col for col in tables[table].columns if 'date' in col.lower()]
        for col in date_cols:
            try:
                tables[table][col] = pd.to_datetime(tables[table][col])
            except ValueError:
                pass
        
    incident_dict={}
    utl_dict = {}
    vacc_dict={}

    folder_dicts = [incident_dict, utl_dict, vacc_dict]

    for folder, folder_dict in zip(folders, folder_dicts):
        files = os.listdir(f".\\data\\{folder}")
        for file in files:
            folder_dict[file[:-4]] = pd.read_csv(f".\\data\\{folder}\\{file}", low_memory=False)

    for folder in folder_dicts:
        for df in folder.keys():
            date_cols = [col for col in folder[df].columns if 'date' in col.lower()]
            for col in date_cols:
                try:
                    folder[df][col] = pd.to_datetime(folder[df][col])                    
                except ValueError:
                    pass
            
    return tables, incident_dict, utl_dict, vacc_dict

def archive_files():
    pathName = os.getcwd()

    shutil.make_archive(f"C:\\Users\\snelson\\repos\\db_mgmt\\data_archive\\{pd.datetime.today().date()}_update", "zip", f".\\data")
       
    files = os.listdir('.\\data')

    folders = [folder for folder in files if 'csv' not in folder and folder != 'archive']
    files = [file for file in files if 'csv' in file and 'statewide' not in file]

    for file in files:
        os.remove(f".\\data\\{file}")


    for folder in folders:
        files = os.listdir(f".\\data\\{folder}")
        for file in files:
            os.remove(f".\\data\\{folder}\\{file}")

def clean_addresses(tables, one_file=False):

    #create dataframe of all Cognify addrsses
    #they come out of Cognify by center
    if one_file:
        geocode_cols = ['coordinates',	'full_address',	'geocode_address', 'local']
        tables['addresses'].drop(geocode_cols, axis=1, inplace=True)
        tables['addresses'].dropna(subset=['address'], inplace=True)
        return tables['addresses']

    tables['addresses_to_add']['as_of'] = pd.to_datetime('today').date()

    tables['addresses_to_add']['active'] = 1

    return tables['addresses_to_add']

def clean_demos(tables):

    tables['demographics'].drop(demographics_drop, axis=1, inplace=True)

    latino_mask = ((tables['demographics']['ethnicity'] == 'Hispanic or Latino') & 
                      (tables['demographics']['race'] == 'Unknown'))

    tables['demographics'].at[latino_mask, 'race'] = 'Hispanic or Latino'

    #short Other Race
    tables['demographics']['race'].replace({'Other Race':'Other'}, inplace=True)

    #drop ethnicity col
    tables['demographics'].drop('ethnicity', axis=1, inplace=True)

    #code Female/Male as 1/0
    tables['demographics']['gender'] = tables['demographics']['gender'].str.strip()
    tables['demographics']['gender'].replace({'Female': 1, 'Male': 0}, inplace=True)

    #Create datetime col of date of birth column
    tables['demographics']['dob'] = pd.to_datetime(tables['demographics']['dob'])

    return tables['demographics']

def clean_incidents(incident_dict, drop_cols, update=False):

    #loop through each dataset and
    #drop cols/replace characters
    #code yes/no cols as 1/0
    #create datetime cols
    #create location and location details cols
    for key in incident_dict.keys():
        incident_dict[key].drop(drop_cols[key], axis=1, inplace=True)

        repls = (' - ', '_'), ('/', '_'), ('(', ''), (')', ''), (' ', '_'), ("'", '')

        incident_dict[key].columns = [reduce(lambda a, kv: a.replace(*kv), repls, col.lower()) for col in incident_dict[key].columns]
        incident_dict[key].columns = [col.replace('-', '_') for col in incident_dict[key].columns]

        code_y_n(incident_dict[key])

        incident_dict[key].dropna(axis=1, how='all', inplace=True)

        incident_dict[key]['date_time_occurred'] = pd.to_datetime(
            incident_dict[key]['date_time_occurred'])
        
        #date_discovered is not in each report
        #this will keep script from breaking in that case
        try:
            incident_dict[key]['date_discovered'] = pd.to_datetime(incident_dict[key]['date_discovered'])
        except KeyError:
            pass
        #location is not in each report
        #this will keep script from breaking in that case
        try:
            #creates a location details sections and allows us to have a col for just PACE/NF/ALF/Home
            #location_details contains Home-Living Room, NF-Living Room and so forth
            incident_dict[key]['location_details'] = incident_dict[key]['location'].str.replace('Participant', '')
            incident_dict[key]['location_details'] = incident_dict[key]['location_details'].str.replace('PACE Center', 'PACE')
            incident_dict[key]['location_details'] = incident_dict[key]['location_details'].str.replace('Nursing Facility', 'NF')
            incident_dict[key]['location_details'] = incident_dict[key]['location_details'].str.replace('Assisted Living Facility', 'ALF')
            incident_dict[key]['location'] = incident_dict[key]['location_details'].str.split(' - ', expand=True)[0]
        except KeyError:
            pass
        incident_dict[key].reset_index(inplace=True, drop=True)

        incident_dict[key] = create_id_col(incident_dict[key], ['member_id', 'date_time_occurred'], 'incident_id')

    return incident_dict

def clean_vacc(vacc_dict, rename_dict):
    
    for df in vacc_dict.keys():
        vacc_dict[df].rename(columns=rename_dict, inplace=True)

    #create immunization status cols
    vacc_dict['influ'] = build_immunization_status(vacc_dict['influ'], vacc_dict['influ_contra'], pneumo=False)
    vacc_dict['pneumo'] = build_immunization_status(vacc_dict['pneumo'], vacc_dict['pneumo_contra'])

    #only keep the id and status cols
    vacc_dict['influ'] = vacc_dict['influ'][['member_id', 'immunization_status', 'date_administered']]
    vacc_dict['pneumo'] = vacc_dict['pneumo'][['member_id', 'immunization_status', 'date_administered']]

    #only keep the id of those with contra
    vacc_dict['influ_contra'] = vacc_dict['influ_contra']['member_id']
    vacc_dict['pneumo_contra'] = vacc_dict['pneumo_contra']['member_id']

    vacc_dict['influ'].drop_duplicates(inplace=True)
    vacc_dict['pneumo'].drop_duplicates(inplace=True)

    vacc_dict['influ'].dropna(subset=['immunization_status'], inplace=True)
    vacc_dict['pneumo'].dropna(subset=['immunization_status'], inplace=True)


def clean_utlization(utl_dict, inpatient_cols, inpatient_drop, er_cols, er_drop, ut_cols, utl_drop_cols, tables, update=False, conn=None):
    enrollment_new = tables['enrollment'][['member_id', 'enrollment_date']]
    enrollment = enrollment_new.copy()

    if update:
        enrollment_db = pd.read_sql("SELECT member_id, enrollment_date FROM enrollment", conn, parse_dates = ['enrollment_date'])
        enrollment = enrollment_db.append(enrollment_new)
    conn.close()
        
    #drop and rename cols for Cognify reports
    utl_dict['inpatient'].drop(inpatient_drop, axis=1, inplace=True)
    utl_dict['inpatient'].rename(columns=inpatient_cols, inplace=True)

    utl_dict['er_adm'].drop(er_drop, axis=1, inplace=True)
    utl_dict['er_adm'].rename(columns=er_cols, inplace=True)

    utl_dict['er_non'].drop(er_drop, axis=1, inplace=True)
    utl_dict['er_non'].rename(columns=er_cols, inplace=True)

    cognify_faciliity_fix = {'Our Lady of Fatima Hospital': 'Fatima Hospital',
                            'The Miriam Hospital' : 'Miriam Hospital',
                            'Westerly Hospital ': 'Westerly Hospital',
                            'Roger Williams Hospital': 'Roger Williams Medical Center',
                            'Roger Williams Cancer Center': 'Roger Williams Medical Center'
                        }
    
    #replace some Cognify report facility quirks
    #can these be changed when being input?
    utl_dict['inpatient']['facility'].replace(cognify_faciliity_fix,
                                          inplace=True)
    utl_dict['er_adm']['facility'].replace(cognify_faciliity_fix,
                                          inplace=True)
    utl_dict['er_non']['facility'].replace(cognify_faciliity_fix,
                                          inplace=True)

    utl_dict['inpatient']['merge'] = (utl_dict['inpatient']['member_id'].astype(str) +
                                  utl_dict['inpatient']['admission_date'].astype(str) +
                                  utl_dict['inpatient']['facility'])

    utl_dict['er_adm']['merge'] = (utl_dict['er_adm']['member_id'].astype(str) +
                                  utl_dict['er_adm']['admission_date'].astype(str) +
                                  utl_dict['er_adm']['facility'])

    utl_dict['er_non']['merge'] = (utl_dict['er_non']['member_id'].astype(str) +
                                  utl_dict['er_non']['admission_date'].astype(str) +
                                  utl_dict['er_non']['facility'])

    #Cognify inpatetient does not indicate ER or not
    #check if inpatient stay is in the ER admited report
    utl_dict['inpatient']['er'] = np.where(utl_dict['inpatient']['merge'].isin(utl_dict['er_adm']['merge'].tolist()),
                                                       1, 0)

    #change some hospital names in the manual utl grids
    utl_hospital = {'Kent' : 'Kent Hospital',
                'kent': 'Kent Hospital',
                'RIH' : 'Rhode Island Hospital', 
                'Landmark' : 'Landmark Medical Center', 
                'Miriam' : 'Miriam Hospital',
                'RWMC' : 'Roger Williams Medical Center',
                'Butler' : 'Butler Hospital',
                'SCH' : 'South County Hospital',
                'Fatima' : 'Fatima Hospital',
                'FirstHealth*' : 'FirstHealth Moore Reginal Hospital',
                'East Side urgent care' : 'East Side Urgent Care',
                'Westerly': 'Westerly Hospital',
                'SCC *': 'Carolinas Hosptial System',
                'W&I' : 'Women & Infants Hospital'}

    utl_dict['ut_grid_inp']['Hospital'].replace(utl_hospital, inplace=True)

    utl_dict['ut_grid_er']['Hospital'].replace(utl_hospital, inplace=True)

    #converting the xls file to csv if giving us some ghost rows
    utl_dict['ut_grid_inp'].dropna(subset=['Member ID'], inplace=True)
    utl_dict['ut_grid_er'].dropna(subset=['Member ID'], inplace=True)

    utl_dict['ut_grid_inp']['Member ID'] = utl_dict['ut_grid_inp']['Member ID'].astype(int)
    utl_dict['ut_grid_er']['Member ID'] = utl_dict['ut_grid_er']['Member ID'].astype(int)

    utl_dict['ut_grid_inp']['merge'] = (utl_dict['ut_grid_inp']['Member ID'].astype(str) +
                                  utl_dict['ut_grid_inp']['Date of visit'].astype(str) +
                                  utl_dict['ut_grid_inp']['Hospital'])

    #utl_dict['ut_grid_inp']['merge'] = utl_dict['ut_grid_inp']['merge'].str.replace('.', '')

    utl_dict['ut_grid_er']['merge'] = (utl_dict['ut_grid_er']['Member ID'].astype(str) +
                                  utl_dict['ut_grid_er']['Date of visit'].astype(str) +
                                  utl_dict['ut_grid_er']['Hospital'])
    #utl_dict['ut_grid_er']['merge'] = utl_dict['ut_grid_inp']['merge'].str.replace('.', '')
                                  
    #check that all UR Grid visits are in Cognify correctly
    grid_vs_cognify = utl_dict['ut_grid_inp'][(-utl_dict['ut_grid_inp']['merge'].isin(utl_dict['inpatient']['merge'].tolist()) &
                        utl_dict['ut_grid_inp']['Hospital'].notnull())][['Member ID',
                                                                          'Ppt Name', 
                                                                          'Date of visit', 
                                                                          'Date of Discharge',
                                                                          'Hospital', 'merge']]
    
    if not grid_vs_cognify.empty:
        grid_vs_cognify.to_csv('..\\output\\ut_grid_cognify_diffs.csv', index=False)
        inp_mask = (utl_dict['inpatient']['member_id'].isin(grid_vs_cognify['Member ID']) &
                    -utl_dict['inpatient']['merge'].isin(utl_dict['ut_grid_inp']['merge']))
        utl_dict['inpatient'][inp_mask].to_csv('..\\output\\inp_id_in_diffs.csv', index=False)
        raise AssertionError('All UR Grid Inpatient Visits are not in Cognify correctly.')

    #check that all UR Grid ER visits are either in the Cognify Inpatient Report
    #or in the ER Non Admit Report
    #this is where there are Cognify inpatient stays in the UT Grid ER Tab
    er_inp_check = utl_dict['inpatient'][(utl_dict['inpatient']['merge'].isin(utl_dict['ut_grid_er']['merge'].tolist()))]['merge']

    #this is all UT Grid ER Visits that are not in the Congify ER Only List
    missing_cog = utl_dict['ut_grid_er'][(-(utl_dict['ut_grid_er']['merge'].isin(utl_dict['er_non']['merge'].tolist())) &
                                      utl_dict['ut_grid_er']['Hospital'].notnull())][['Member ID', 'Ppt Name',
                                                                                       'Date of visit', 'Date of Discharge',
                                                                                       'Hospital', 'merge']]
    #this is any UR Grid ER Visits that is not in either Cognify report
    missing_ur_er_visits = missing_cog[-(missing_cog['merge'].isin(er_inp_check))]
    
    if not missing_ur_er_visits.empty:
        missing_ur_er_visits.to_csv('..\\output\\ut_grid_cognify_diffs_er.csv', index=False)
        er_mask = (utl_dict['er_non']['member_id'].isin(missing_ur_er_visits['Member ID']) &
                    -utl_dict['er_non']['merge'].isin(utl_dict['ut_grid_er']['merge']))
        utl_dict['er_non'][er_mask].to_csv('..\\output\\er_id_in_diffs.csv', index=False)
        raise AssertionError('All UR Grid ER Visits are not in Cognify correctly.')

    #drop cols
    utl_dict['ut_grid_inp'].drop(utl_drop_cols, axis=1, inplace=True)
    utl_dict['ut_grid_er'].drop(utl_drop_cols, axis=1, inplace=True)

    #rename cols
    utl_dict['ut_grid_inp'].rename(columns=utl_rename_dict, inplace=True)
    utl_dict['ut_grid_er'].rename(columns=utl_rename_dict, inplace=True)

    #replace weird excel blank values with np.nan
    utl_dict['ut_grid_inp']['days_MD'] = np.where(utl_dict['ut_grid_inp']['days_MD'].astype(float)>=1000, np.nan, utl_dict['ut_grid_inp']['days_MD'])
    utl_dict['ut_grid_inp']['days_RN'] = np.where(utl_dict['ut_grid_inp']['days_RN'].astype(float)>=1000, np.nan, utl_dict['ut_grid_inp']['days_RN'])

    utl_dict['ut_grid_er']['days_MD'] = np.where(utl_dict['ut_grid_er']['days_MD'].astype(float)>=1000, np.nan, utl_dict['ut_grid_er']['days_MD'])
    utl_dict['ut_grid_er']['days_RN'] = np.where(utl_dict['ut_grid_er']['days_RN'].astype(float)>=1000, np.nan, utl_dict['ut_grid_er']['days_RN'])

    #drop any scheduled visits, data is not tracked for these
    drop_sched = utl_dict['ut_grid_inp'][utl_dict['ut_grid_inp']['visit_type'] == 'Scheduled'].index.tolist()
    utl_dict['ut_grid_inp'].drop(drop_sched, inplace=True)

    utl_dict['ut_grid_inp'].drop('visit_type', axis=1, inplace=True)

    #create OBS column
    utl_dict['ut_grid_er']['visit_type'] = np.where(utl_dict['ut_grid_er']['visit_type'] == 'OBS', 1, 0)
    utl_dict['ut_grid_er'].rename(columns={'visit_type':'observation'}, inplace=True)

    inp_obs = utl_dict['ut_grid_er'][(utl_dict['ut_grid_er']['merge'].isin(er_inp_check)) & 
                       (utl_dict['ut_grid_er']['observation'] == 1)]['merge'].tolist()

    utl_dict['inpatient']['observation'] = np.where(utl_dict['inpatient']['merge'].isin(inp_obs), 1, 0)

    #removal of UR Grid ER Visits that are not ER Only but instead
    #are ER to inpatient
    utl_dict['ut_grid_er'] = utl_dict['ut_grid_er'][-utl_dict['ut_grid_er']['merge'].isin(er_inp_check)]

    #Seperate out inpatient Hospital and Inpatient Psych Visits
    #this is so days since last visit refers to a visit of a certain type

    hosp_list = ['Rhode Island Hospital', 'Landmark Medical Center', 
                'Kent Hospital', 'Roger Williams Hospital', 'Fatima Hospital',
                'Miriam Hospital', 'Westerly Hospital', 'Women & Infants Hospital']

    fill_admission_type_na = ['Acute Hospital' if facility in hosp_list else np.nan for facility in utl_dict['inpatient']['facility']]

    utl_dict['inpatient']['admission_type'] = np.where(utl_dict['inpatient']['admission_type'].isnull(),
                                                        fill_admission_type_na,
                                                        utl_dict['inpatient']['admission_type'])

    inpatient_hosp = utl_dict['inpatient'][utl_dict['inpatient'].admission_type.isin(['Acute Hospital',
                                                                                  np.nan])].copy().reset_index(drop=True)

    inpatient_pysch = utl_dict['inpatient'][utl_dict['inpatient'].admission_type.isin(['Psych Unit / Facility',
                                                                                  np.nan])].copy().reset_index(drop=True)

    #Create datetime Cols
    inpatient_hosp['admission_date'] = pd.to_datetime(inpatient_hosp['admission_date'])
    inpatient_hosp['discharge_date'] = pd.to_datetime(inpatient_hosp['discharge_date'])

    inpatient_pysch['admission_date'] = pd.to_datetime(inpatient_pysch['admission_date'])
    inpatient_pysch['discharge_date'] = pd.to_datetime(inpatient_pysch['discharge_date'])

    inpatient_hosp.drop_duplicates(inplace=True)
    inpatient_pysch.drop_duplicates(inplace=True)

    #Calculate Day Since Last Admit
    inpatient_hosp = discharge_admit_diff(inpatient_hosp, sql_table='inpatient', admit_type='Acute Hospital', update=update)
    inpatient_pysch = discharge_admit_diff(inpatient_pysch, sql_table='inpatient', admit_type='Psych Unit / Facility', update=update)

    #these are all weird copies that are checking the Admission Date against the same stays Discharge Date
    inpatient_hosp['days_since_last_admission'] = np.where(inpatient_hosp['days_since_last_admission'] <= 0, np.nan,
                                                           inpatient_hosp['days_since_last_admission'])

    inpatient_pysch['days_since_last_admission'] = np.where(inpatient_pysch['days_since_last_admission'] <= 0, np.nan,
                                                            inpatient_pysch['days_since_last_admission'])
       
    #put them back together
    inpatient = inpatient_hosp.append(inpatient_pysch)

    #merge inpatient Cognify report with the UR Grid Inpatient
    inpatient = inpatient.merge(utl_dict['ut_grid_inp'], on='merge', how='left').drop_duplicates()
  
    #merge with enrollment to create a column indicating
    #if the visit was within 6 months of enrollment

    inpatient = inpatient.merge(enrollment[['member_id','enrollment_date']])

    inpatient['enrollment_date'] = pd.to_datetime(inpatient['enrollment_date'])
    inpatient['admission_date'] = pd.to_datetime(inpatient['admission_date'])

    inpatient['w_six_months'] = np.where((inpatient['admission_date'].dt.to_period('M') -
                                           inpatient['enrollment_date'].dt.to_period('M')).apply(lambda x: x.freqstr[:-1]).replace('',
                                                                                                                                       np.nan).astype(float) <= 6,
                                          1, 0)

    #Create day of the week column
    inpatient['dow'] = inpatient['admission_date'].dt.weekday
    inpatient['dow'].replace({0:'Monday', 1:'Tuesday', 2:'Wednesday',
    3:'Thursday', 4:'Friday', 5:'Saturday', 6:'Sunday'}, inplace=True)
    
    #Code any Yes/No cols as 1/0
    code_y_n(inpatient)

    #Drop left over enrollment data and the merge column - we are done with them
    inpatient.drop(['enrollment_date', 'merge'], axis=1, inplace=True)

    #create datetime cols
    inpatient['admission_date'] = pd.to_datetime(inpatient['admission_date'])
    inpatient['discharge_date'] = pd.to_datetime(inpatient['discharge_date'])

    inpatient.drop_duplicates(subset = ['member_id', 'admission_date', 'facility'], inplace=True)

    for col in inpatient.columns:
        if inpatient[col].dtype == 'O':
            inpatient[col] = inpatient[col].apply(lambda x: titlecase(str(x)) if x is not None else None)

    inpatient = create_id_col(inpatient,
                            ['member_id', 'admission_date', 'facility'],
                            'visit_id')


    #Merge Cognify ER Only and UR Grid ER reports
    er_only = utl_dict['er_non'].merge(utl_dict['ut_grid_er'], on='merge', how='left').drop_duplicates()

    er_only = er_only.merge(enrollment[['member_id','enrollment_date']])
    
    #merge with enrollment to create a column indicating
    #if the visit was within 6 months of enrollment
    er_only['enrollment_date'] = pd.to_datetime(er_only['enrollment_date'])
    er_only['admission_date'] = pd.to_datetime(er_only['admission_date'])

    er_only['w_six_months'] = np.where((er_only['admission_date'].dt.to_period('M') - 
                                    er_only['enrollment_date'].dt.to_period('M')).apply(lambda x: x.freqstr[:-1]).replace('',
                                                                                                                         np.nan).astype(float) <= 6,
                                          1, 0)
    
    #Create day of the week column
    er_only['dow'] = er_only['admission_date'].dt.weekday
    er_only['dow'].replace({0:'Monday', 1:'Tuesday', 2:'Wednesday',
    3:'Thursday', 4:'Friday', 5:'Saturday', 6:'Sunday'}, inplace=True)
    
    #Code any Yes/No cols as 1/0
    code_y_n(er_only)

    #Drop left over enrollment data and the merge column - we are done with that
    er_only.drop(['enrollment_date', 'merge'], axis=1, inplace=True)

    #Create datetime cols
    er_only['admission_date'] = pd.to_datetime(er_only['admission_date'])

    er_only.drop_duplicates(inplace=True)

    #find days between visits for participants
    er_only = discharge_admit_diff(er_only, sql_table='er_only', admit_diff=True, update=update)

    #these are all weird copies that are checking the Admission Date against the same stays Discharge Date
    er_only['days_since_last_admission'] = np.where(er_only['days_since_last_admission'] <= 0, np.nan,
                                                    er_only['days_since_last_admission'])

    for col in er_only.columns:
        if er_only[col].dtype == 'O':
            er_only[col] = er_only[col].apply(lambda x: titlecase(str(x)) if x is not None else None)


    er_only.drop_duplicates(subset = ['member_id', 'admission_date', 'facility'], inplace=True)

    er_only = create_id_col(er_only,
                            ['member_id', 'admission_date', 'facility'],
                            'visit_id')

    #start on Nursing Facility table
    inpatient_snf = utl_dict['inpatient'][utl_dict['inpatient']['admission_type'].isin(['Nursing Home',
                                                                                    'Rehab Unit / Facility',
                                                                                    'End of Life'])].copy()
    #replace long admit reasons with the core reason custodial/respite/skilled/other
    snf_reasons = inpatient_snf[inpatient_snf['admit_reason'].notnull()]['admit_reason'].copy()
    skilled = snf_reasons[snf_reasons.str.contains('skilled', case=False)].unique()
    custodial = snf_reasons[snf_reasons.str.contains('custodial|EOL|end of life|long term', case=False)].unique()
    respite = snf_reasons[snf_reasons.str.contains('respite', case=False)].unique()
    other = [reason for reason in snf_reasons.unique() if reason not in skilled.tolist() + custodial.tolist() + respite.tolist()]

    skill = {x:'Skilled' for x in skilled}
    cust = {x:'Custodial' for x in custodial}
    res = {x:'Respite' for x in respite}
    oth = {x:'Other' for x in other}

    reason_dict = {**cust, **res, **oth, **skill}

    inpatient_snf['admit_reason'].replace(reason_dict, inplace=True)

    #calculate LOS
    inpatient_snf['los'] = np.where(inpatient_snf.discharge_date.isnull(), np.nan, inpatient_snf.los)

    #create datetime cols
    inpatient_snf['admission_date'] = pd.to_datetime(inpatient_snf['admission_date'])
    inpatient_snf['discharge_date'] = pd.to_datetime(inpatient_snf['discharge_date'])

    #merge with enrollment to create a column indicating
    #if the visit was within 6 months of enrollment
    inpatient_snf = inpatient_snf.merge(enrollment[['member_id',
                                                          'enrollment_date']])

    inpatient_snf['enrollment_date'] = pd.to_datetime(inpatient_snf['enrollment_date'])

    inpatient_snf['w_six_months'] = np.where((inpatient_snf['admission_date'].dt.to_period('M') - 
                                          inpatient_snf['enrollment_date'].dt.to_period('M')).apply(lambda x: x.freqstr[:-1]).replace('',
                                                                                                                                     np.nan).astype(float) <= 6,
                                          1, 0)

    inpatient_snf.drop(['enrollment_date', 'er', 'merge'], axis=1, inplace=True)

    inpatient_snf.drop_duplicates(inplace=True)

    inpatient_snf = discharge_admit_diff(inpatient_snf,  sql_table='inpatient_snf', update=update)

    #these are all weird copies that are checking the Admission Date against the same stays Discharge Date
    inpatient_snf['days_since_last_admission'] = np.where(inpatient_snf['days_since_last_admission'] <= 0, np.nan,
                                                          inpatient_snf['days_since_last_admission'])

    inpatient_snf.drop_duplicates(subset = ['member_id', 'admission_date', 'facility'], inplace=True)

    for col in inpatient_snf.columns:
        if inpatient_snf[col].dtype == 'O':
            inpatient_snf[col] = inpatient_snf[col].apply(lambda x: titlecase(str(x)) if x is not None else None)

    inpatient_snf = create_id_col(inpatient_snf,
                            ['member_id', 'admission_date', 'facility'],
                            'visit_id')

    return inpatient, er_only, inpatient_snf

def clean_enrollment(tables, rename_dict, drop_cols):

    #create first and last name cols
    tables['enrollment'][['last', 'first']] = tables['enrollment']['ParticipantName'].str.split(',', expand=True)

    #drop information that is in other tables/cols
    #or not needed (SSN)

    tables['enrollment'].drop(drop_cols, axis=1, inplace=True)

    tables['enrollment'].rename(columns=rename_dict, inplace=True)

    #code medicare/medicaid as 1 for has 0 for does not
    tables['enrollment']['medicare']  = np.where(tables['enrollment']['medicare'].notnull(),
                                            1, 0)
    tables['enrollment']['medicaid']  = np.where(tables['enrollment']['medicaid'].notnull(),
                                            1, 0)

    #disenroll_reasons begins with the type (volunatry/non)
    #Split that info out in to a new column
    tables['enrollment']['disenroll_type'] = tables['enrollment']['disenroll_reason'].str.split(' ',
                                                                                            expand=True)[0].replace('',
                                                                                                                    np.nan)

    tables['enrollment']['disenroll_reason'] = tables['enrollment']['disenroll_reason'].apply(lambda x: ' '.join(str(x).split(' ')[1:]))
    #replace blank reasons with null values
    
    tables['enrollment']['disenroll_reason'].replace('', np.nan, inplace=True)
    #dissatified with is implied in all of these reasons
    tables['enrollment']['disenroll_reason'] = tables['enrollment']['disenroll_reason'].str.replace('Dissatisfied with ', '')

    #create datetime cols
    tables['enrollment']['enrollment_date'] = pd.to_datetime(tables['enrollment']['enrollment_date'])
    tables['enrollment']['disenrollment_date'] = pd.to_datetime(tables['enrollment']['disenrollment_date'])

    return tables['enrollment']

def clean_center_days(tables, rename_dict, drop_cols):
    #drop cols that are not needed
    #or show up in the enrollment table

    tables['center_days'].drop(drop_cols, axis=1, inplace=True)

    tables['center_days'].rename(columns=rename_dict, inplace=True)

    #create an as of column, so we can keep track of historic changes
    tables['center_days']['as_of'] = pd.to_datetime('today').date()

    return tables['center_days']

def clean_dx(tables, dx_cols):
    tables['dx'].dropna(thresh=2, inplace=True)

    tables['dx'].drop(dx_drop_cols, axis=1, inplace=True)

    tables['dx'].columns = dx_cols

    tables['dx']['icd_simple'] = tables['dx']['icd10'].str.split('.', expand=True)[0]
    #PrimeSuite has duplicates sometimes
    
    tables['dx']['as_of'] = pd.to_datetime('today').date()

    tables['dx'].drop_duplicates(subset=['member_id', 'as_of', 'icd10'],
                                inplace=True)

    tables['dx'].dropna(inplace=True)

def clean_grievances(tables):

    #works with Pauline's grid
    #looks for where she indicates the providers/types start
    #this is in the current first row
    provider_start = tables['grievances'].columns.tolist().index("Provider")
    provider_end_type_start = tables['grievances'].columns.tolist().index("TYPE")
    #type_end = tables['grievances'].columns.tolist().index("EOT")
    type_end = tables['grievances'].iloc[0].values.tolist().index("full resolution for internal tracking")
    #actual col names are in the second row
    tables['grievances'].columns = tables['grievances'].iloc[1].values.tolist()

    tables['grievances'].drop([0, 1], inplace=True)

    tables['grievances'].reset_index(drop=True, inplace=True)

    #fix one column that needs the long title for others that use the grid
    tables['grievances'].rename(columns = {'grievance # (if highlighted, indicates letter was sent)':
                                            'grievance_num'}, inplace=True)

    tables['grievances']['grievance_num'] = tables['grievances']['grievance_num'].str.split("-", expand =True)[0]

    #fix some odd formating in the col names
    tables['grievances'].columns = [str(col).lower().replace(' ', '_').replace('\n', '').replace('\r', '') for col in tables['grievances'].columns]
    tables['grievances'].columns = [col.replace('/', '_').replace('-', '_') for col in tables['grievances'].columns]
    tables['grievances'].rename(columns={'participant_id':'member_id'}, inplace=True)
    #get cols that indicate if a grievances is attributed to
    #a specific provider
    providers = tables['grievances'].columns[provider_start:provider_end_type_start].tolist()

    #or a specific type
    types = tables['grievances'].columns[provider_end_type_start:type_end].tolist()

    #create column that indicates the has the name of the provider
    #the grievance is attributed to
    tables['grievances']['providers'] = np.nan

    for provider in providers:
        tables['grievances'][provider] = tables['grievances'][provider].replace('0.5',"1")
        tables['grievances']['providers'] = np.where(tables['grievances'][provider] == "1", provider, tables['grievances']['providers'])

    #create column that indicates the has the type of each grievance
    tables['grievances']['types'] = np.nan

    for type_griev in types:
        tables['grievances'][provider] = tables['grievances'][provider].replace('0.5',"1")
        tables['grievances']['types'] = np.where(tables['grievances'][type_griev] == "1", type_griev, tables['grievances']['types'])

    #below we clean up some common data entry issuses we saw
    tables['grievances']['providers'] = tables['grievances']['providers'].str.replace('(', '')
    tables['grievances']['providers'] = tables['grievances']['providers'].str.replace(')', '')
    tables['grievances']['providers'] = tables['grievances']['providers'].str.replace('transportation', 'transport')

    tables['grievances']['providers'] = tables['grievances']['providers'].str.replace('snfs_hospitals_alfs', 'facilities')
    tables['grievances']['providers'] = tables['grievances']['providers'].str.replace('.', '_')

    tables['grievances']['types'] = tables['grievances']['types'].str.replace('(products)', '')
    tables['grievances']['types'] = tables['grievances']['types'].str.replace('equip_supplies()', 'equip_supplies') 
    tables['grievances']['types'] = tables['grievances']['types'].str.replace('(', '') 
    tables['grievances']['types'] = tables['grievances']['types'].str.replace(')', '') 
    tables['grievances']['types'] = tables['grievances']['types'].str.replace('commun-ication', 'communication')
    tables['grievances']['types'] = tables['grievances']['types'].str.replace('person-nel', 'personnel')

    tables['grievances']['category_of_the_grievance'] = np.where(tables['grievances']['category_of_the_grievance'].str.contains('Contracted'),
    'Contracted Facility', tables['grievances']['category_of_the_grievance'])

    #drop cols that we do not need, includes all the provider
    #and types cols that we have essentially "un" one hot encoded
    grievances_drop = ['participant_first_name', 'participant_last_name',
                   'year_and_qtr_received', 'quarter_reported', 'nan'] + providers + types

    tables['grievances'].drop(grievances_drop, axis=1, inplace=True)

    tables['grievances']['description_of_the_grievance'] = np.where(tables['grievances']['description_of_the_grievance'].str.contains('Other'),
        'Other', tables['grievances']['description_of_the_grievance'])

    #turn quality analysis col to binary 1/0 for Y/N
    tables['grievances']['quality_analysis'].replace(['Y', 'N'], [1, 0], inplace=True)

    #create datetime cols
    tables['grievances']['date_grievance_received'] = pd.to_datetime(tables['grievances']['date_grievance_received'])
    tables['grievances']['date_of_resolution'] = pd.to_datetime(tables['grievances']['date_of_resolution'])
    tables['grievances']['date_of_oral_notification'] = pd.to_datetime(tables['grievances']['date_of_oral_notification'])
    tables['grievances']['date_of_written_notification'] = pd.to_datetime(tables['grievances']['date_of_written_notification'])
    
    tables['grievances'].dropna(subset=['member_id', 'date_grievance_received'], inplace=True)

    tables['grievances'] = create_id_col(tables['grievances'],
                            ['member_id', 'date_grievance_received'],
                            'griev_id')

    return tables['grievances']

def create_table(df, table_name, primary_key, conn, foreign_key=None, ref_table=None, ref_col=None):
    #create dictionary that will map pandas types to sqlite types
    pd2sql = {'flo' : 'FLOAT',
          'int' : 'INTEGER',
          'dat' : 'DATETIME',
          'tim' : 'DATETIME',
          'cat' : 'TEXT',
          'obj' : 'TEXT'}
    conn.execute("PRAGMA foreign_keys = 1")

    #build sql query to create tables
    sql_query = """"""
    sql_query += f"CREATE TABLE IF NOT EXISTS {table_name} ("

    #dtype_dict = {}

    if (foreign_key is None) and (len(primary_key) ==1):
        for col, dtype in df.dtypes.iteritems():
            sql_type = pd2sql[str(dtype)[:3]]
            if col == df.columns[-1]:
                end = ''
            else:
                end = ','
            if col in primary_key:
                sql_query += f"{col} {sql_type} PRIMARY KEY{end}"
            else:
                sql_query += f"{col} {sql_type}{end}"

    elif (foreign_key is not None) and (len(primary_key) == 1):
        for col, dtype in df.dtypes.iteritems():
            sql_type = pd2sql[str(dtype)[:3]]
            if col in primary_key:
                sql_query += f"{col} {sql_type} PRIMARY KEY," 
            else:
                sql_query += f"{col} {sql_type},"
    
        #append foreign key creation sql if needed
        for fk, rtb, rcol in zip(foreign_key, ref_table, ref_col):
            sql_query += f"FOREIGN KEY ({fk}) REFERENCES {rtb} ({rcol}) "
    else:
        for col, dtype in df.dtypes.iteritems():
            sql_type = pd2sql[str(dtype)[:3]]
            sql_query += f"{col} {sql_type},"
        #create primary key SQL

        if foreign_key is not None:
            for fk, rtb, rcol in zip(foreign_key, ref_table, ref_col):
                sql_query += f"FOREIGN KEY ({fk}) REFERENCES {rtb} ({rcol}) "
        pk = f"PRIMARY KEY ({primary_key[0]}"

        try:
            for i, k in enumerate(primary_key[1:]):
                if (i+1) == len(primary_key)-1:
                    pk += f", {k})"
                else:
                    pk += f", {k}"
        except IndexError:
            primary_key = f"PRIMARY KEY ({primary_key[0]}))"

        sql_query += pk

    sql_query += ');'
    c = conn.cursor()

    c.execute(sql_query)
    conn.commit()
    #take pandas dataframe and append all rows to our sql table
    df.drop_duplicates(subset=primary_key, inplace=True)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.commit()
    
def update_sql_table(df, table_name, conn, primary_key):
    c = conn.cursor()

    #create temp table with possibly new data from cognify
    df.to_sql('temp', conn, index=False, if_exists='replace')

    #filters sql table for non new rows
    #and updates the cols
    filter_sql = f"""WHERE {primary_key[0]} = {table_name}.{primary_key[0]}"""

    try:
        for col in primary_key[1:]:
            filter_sql += f"""
                AND {col} = {table_name}.{col}
                """
    except IndexError:
        pass

    set_cols = [df_col for df_col in df.columns if df_col not in primary_key]

    set_sql = ', '.join([f"""{col} = (SELECT {col} FROM temp {filter_sql})""" for col in set_cols])

    exists_sql = f"""(SELECT {', '.join(set_cols)} FROM temp {filter_sql})"""

    c.execute(
        f"""
        UPDATE {table_name}
        SET {set_sql}
        WHERE EXISTS {exists_sql};
        """
    )
    conn.commit()

    #inserts new data if there is a primary key in the pandas df
    #that is not in the sql table
    insert_cols = ', '.join(col for col in df.columns)

    compare_pk_sql = ' AND '.join([ f"""f.{col} = t.{col}""" for col in primary_key])

    c.execute(
        f"""
        INSERT INTO {table_name} ({insert_cols})
        SELECT {insert_cols} FROM temp t
        WHERE NOT EXISTS 
            (SELECT {insert_cols} from {table_name} f
            WHERE {compare_pk_sql});
        """    
    )
    conn.commit()

#start main
def create_or_update_table(db_name, update_table=True):
    #load and clean all csv files
    tables, incident_dict, utl_dict, vacc_dict = get_csv_files()
    tables['addresses'] = clean_addresses(tables)
    if update_table:
        del tables['addresses_to_add']
    clean_enrollment(tables, enrollment_rename_dict, enrollment_drop)
    clean_demos(tables)
    clean_incidents(incident_dict, incident_drop_cols)
    clean_vacc(vacc_dict, vacc_rename_dict)
    tables['inpatient'], tables['er_only'], tables['inpatient_snf'] = clean_utlization(utl_dict, inpatient_rename_dict, inpatient_drop,
                                                                                    er_rename_dict, er_drop_cols, utl_rename_dict,
                                                                                    utl_drop_cols, tables, update=update_table, conn=sqlite3.connect(db_name))
    
    clean_center_days(tables, center_days_rename_dict, center_days_drop)
    clean_dx(tables, dx_cols)
    clean_grievances(tables)

    #append sub dictionary dfs to tables dictionary
    for df in incident_dict.keys():
        tables[df] = incident_dict[df] 

    for df in ['influ', 'pneumo']:
        tables[df] = vacc_dict[df] 

    tables['ppts'] = tables['enrollment'][['member_id', 'last', 'first']].copy()
    tables['ppts'].drop_duplicates(subset=['member_id'], inplace=True)

    tables['enrollment'].drop(['last', 'first'], axis=1, inplace=True)

    for table_name in tables.keys():
        df = tables[table_name].copy()
        tables[table_name] = df[df.member_id != 1003].copy()

    #build dictionary of table info for the sql tables includes
    #table name:
    #primary key
    #foriegn key
    sql_table_info = {
                    'ppts': {'primary_key': ['member_id'],
                                    'foreign_key':None,
                                    'ref_table': None,
                                    'ref_col': None},
                    'center_days': {'primary_key': ['member_id', 'days'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'dx': {'primary_key': ['member_id', 'icd10'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'enrollment': {'primary_key': ['member_id', 'enrollment_date'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'],
                                   'ref_col': ['member_id']},
                    'addresses': {'primary_key': ['member_id', 'address'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'demographics': {'primary_key': ['member_id'],
                                  'foreign_key': ['member_id'],
                                     'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'burns': {'primary_key': ['incident_id'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'falls': {'primary_key': ['incident_id'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'infections': {'primary_key': ['incident_id'],
                                  'foreign_key': ['member_id'],
                                   'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'med_errors': {'primary_key': ['incident_id'],
                                  'foreign_key': ['member_id'],
                                   'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'influ': {'primary_key': ['member_id', 'date_administered'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'] ,
                                  'ref_col': ['member_id']},
                    'pneumo': {'primary_key': ['member_id', 'date_administered'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'inpatient': {'primary_key':['visit_id'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'er_only': {'primary_key':['visit_id'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'inpatient_snf': {'primary_key':['visit_id'],
                                  'foreign_key': ['member_id'],
                                      'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'grievances': {'primary_key':['griev_id'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'],
                                  'ref_col': ['member_id']},
                    'wounds': {'primary_key':['incident_id'],
                                  'foreign_key': ['member_id'],
                                  'ref_table': ['ppts'],
                                  'ref_col': ['member_id']}}

    conn = sqlite3.connect(db_name)
    #if we only need to update the tables
    if update_table:
        shutil.copy(db_name,
        f"V:\\Databases\\PaceDashboard_{pd.to_datetime('today').date()}.db")
        for table_name in tables.keys():
            table_keys = sql_table_info[table_name]
            update_sql_table(tables[table_name], table_name, conn,
            table_keys['primary_key'])
            print(f"{table_name} updated...")
        
        c = conn.cursor()
        c.execute("DROP TABLE temp;")
        text_file = open("V:\\Databases\\update_log.txt", "w")
        text_file.write(str(pd.to_datetime('today')))
        text_file.close()
    else:
        #for first time creation only
        table_keys = sql_table_info['ppts']
        create_table(tables['ppts'], 'ppts',
                 table_keys['primary_key'],
                 conn,
                 table_keys['foreign_key'],
                 table_keys['ref_table'],
                 table_keys['ref_col'])
        for table_name in [table for table in tables.keys() if table != 'ppts']:
            table_keys = sql_table_info[table_name]
            create_table(tables[table_name], table_name,
                 table_keys['primary_key'],
                 conn,
                 table_keys['foreign_key'],
                 table_keys['ref_table'],
                 table_keys['ref_col'])
    
    archive_files()

    conn.commit()
    conn.close()
    
    shutil.copy2(db_name, report_db)

    print('All Set!')

def create_db():
    return create_or_update_table(db_name, False)

def update_db():
    return create_or_update_table(db_name)
