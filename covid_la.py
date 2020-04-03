# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 19:30:07 2020

@author: jeff adelson

This script will access the ArcGIS Rest endpoints of the dashboard used by
the Louisiana Department of Health to track the coronavirus/COVID-19 pandemic
and add the latest data to csv files to preserve this data for time-series
analysis.

As of this update, LDH provides the following data:
Cases (Parish-level)
Deaths (Parish-level)
Tests (Statewide)
Age groups of those who tested positve (Statewide)
Sex of those who tested positive (Statewide)
Age groups of those who died statewide. 

LDH is currently updating their dashboard twice a day,
at 9:30 a.m. and 5:30 p.m. This script should be run after the last update of
the day to capture the final tallies for each day. If it is run multiple 
times per day, it will overwrite any previous data for the day with the updated
data.

"""

import pandas as pd
import json
from urllib.request import urlopen
from datetime import datetime

def esri_cleaner(url):
    data = urlopen(url).read()
    raw_json = json.loads(data)
    formatted_json = [feature['attributes'] for feature in raw_json['features']]
    return formatted_json

def capacity_table(df, file, date):
    r_type = ['Ventilators', 'Hospital Beds', 'ICU']
    r_table = pd.DataFrame()
    for r in r_type:
        resource = df[['Category', 'LDH_Region', r]].rename(columns = {r : date, 
                                                                       'LDH_Region' : 'LDH Region'})
        resource['Category'] = r+' '+resource['Category']
        resource_total = resource.groupby('LDH Region').sum().reset_index()
        resource_total['Category'] = r+' Total'
        resource = resource.append(resource_total, sort=False)
        # print(resource)
        # print('')   
        r_table = r_table.append(resource)
        # print(r_table)
    if date in file.columns:
        file = file.drop(columns=date)
    file = file.merge(r_table,
                      left_on = ['Category',
                                 'LDH Region'],
                      right_on = ['Category',
                                  'LDH Region'],
                      how = 'outer')
    return file


def la_covid(parish_url, state_url, capacity_url, date):
    cases = pd.DataFrame(esri_cleaner(parish_url))
    cases = cases.rename(columns = {'PFIPS' : 'FIPS'})
    cases.loc[cases['FIPS'] == '0', 'FIPS'] = '22000'
    deaths = cases.copy()
    tests_detail = cases.copy()
    cases = cases.rename(columns = {'Cases' : date})
    case_file = pd.read_csv('data/cases.csv', dtype = {'FIPS' : object})
    if date in case_file.columns:
        case_file = case_file.drop(columns = date)
    case_file.merge(cases[['FIPS', date]], 
                    on='FIPS',  
                    how='outer').to_csv('data/cases.csv', index=False)
    deaths = deaths.rename(columns = {'Deaths' : date, 'PFIPS' : 'FIPS'})
    death_file = pd.read_csv('data/deaths.csv', dtype = {'FIPS' : object})
    if date in death_file.columns:
        death_file = death_file.drop(columns = date)
    death_file.merge(deaths[['FIPS', date]],
                      on='FIPS',
                      how='outer').to_csv('data/deaths.csv', index=False)
    tests_detail_file = pd.read_csv('data/tests.csv', dtype={'FIPS' : object})
    if date in tests_detail_file.columns:
        tests_detail_file = tests_detail_file.drop(columns = date)
    tests_public = tests_detail[['FIPS', 'State_Tests']]
    tests_public = tests_public.rename(columns = {'State_Tests' : date, 'PFIPS' : 'FIPS'})
    tests_public['Category'] = 'Public'
    tests_private = tests_detail[['FIPS', 'Commercial_Tests']]
    tests_private = tests_private.rename(columns = {'Commercial_Tests' : date, 'PFIPS' : 'FIPS'})
    tests_private['Category'] = 'Private'
    tests = tests_public.append(tests_private)
    tests_detail_file.merge(tests, 
                            left_on=['FIPS', 'Category'], 
                            right_on=['FIPS', 'Category'], 
                            how='outer').to_csv('data/tests.csv', index=False)
    state = pd.DataFrame(esri_cleaner(la_state_url))
    # tests = state[state['Category'] == 'Test Completed'].rename(columns = ({'Value' : 'Public', 'Value2' : 'Private'}))
    # print(tests.head())
    # tests['date'] = update_date
    # tests = tests[['date', 'Public', 'Private']].set_index('date').transpose().reset_index().rename(columns={'index' : 'Category'})
    # test_file = pd.read_csv('data/tests.csv')
    # if date in test_file.columns:
    #     test_file = test_file.drop(columns = date)
    # test_file.merge(tests[['Category', date]], 
    #                 on='Category', 
    #                 how='outer').to_csv('data/tests.csv', index=False)
    case_demo = state[state['Category'] != 'Test Completed'].rename(columns=({'Value' : date}))
    case_demo_file = pd.read_csv('data/case_demo.csv')
    if date in case_demo_file.columns:
        case_demo_file = case_demo_file.drop(columns = date)
    case_demo_file.merge(case_demo[['Category', date]], on='Category', how='outer').to_csv('data/case_demo.csv', index=False)
    death_demo = state[state['Category'] != 'Test Completed'].rename(columns=({'Value2' : date}))
    death_demo_file = pd.read_csv('data/death_demo.csv')
    if date in death_demo_file.columns:
        death_demo_file = death_demo_file.drop(columns = date)
    death_demo_file.merge(death_demo[['Category', date]], on='Category', how='outer').to_csv('data/death_demo.csv', index=False)
    capacity = pd.DataFrame(esri_cleaner(capacity_url))
    capacity = capacity.rename(columns = {'HospVent' : 'Ventilators',
                                          'Bed' : 'Hospital Beds'})
    capacity_file = pd.read_csv('data/capacity.csv')
    capacity_export = capacity_table(capacity, capacity_file, date)
    capacity_export.to_csv('data/capacity.csv', index=False)
    


update_date = '{d.month}/{d.day}/{d.year}'.format(d=datetime.now())

la_state_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/arcgis/rest/services/State_Level_Information_2/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=pjson'
la_county_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Cases_by_Parish/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=pjson'
region_capacity_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Louisiana_Vent_and_Bed_Report/FeatureServer/0/query?where=1%3D1&outFields=*&f=pjson'


la_covid(la_county_url,la_state_url, region_capacity_url, update_date)