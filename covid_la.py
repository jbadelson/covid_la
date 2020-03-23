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

def la_covid(parish_url, state_url, date):
    cases = pd.DataFrame(esri_cleaner(parish_url))
    cases = cases.rename(columns = {'PFIPS' : 'FIPS'})
    cases.loc[cases['FIPS'] == '0', 'FIPS'] = '22000'
    deaths = cases.copy()
    cases = cases.rename(columns = {'Cases' : date})
    print('Case fields:')
    for c in cases.columns:
        print(c)
    case_file = pd.read_csv('data/cases.csv', dtype = {'FIPS' : object})
    if date in case_file.columns:
        case_file = case_file.drop(columns = date)
    case_file.merge(cases[['FIPS', date]], 
                    on='FIPS', 
                    how='outer').to_csv('data/cases.csv', index=False)
    deaths = deaths.rename(columns = {'Deaths' : date, 'PFIPS' : 'FIPS'})
    print('Death fields:')
    for d in deaths.columns:
        print(d)
    death_file = pd.read_csv('data/deaths.csv', dtype = {'FIPS' : object})
    if date in death_file.columns:
        death_file = death_file.drop(columns = date)
    death_file.merge(deaths[['FIPS', date]],
                      on='FIPS',
                      how='outer').to_csv('data/deaths.csv', index=False)
    state = pd.DataFrame(esri_cleaner(la_state_url))
    tests = state[state['Category'] == 'Test Completed'].rename(columns = ({'Value' : 'Public', 'Value2' : 'Private'}))
    tests['date'] = update_date
    tests = tests[['date', 'Public', 'Private']].set_index('date').transpose().reset_index().rename(columns={'index' : 'Category'})
    print('Test fields:')
    for t in tests.columns:
        print(t)
    test_file = pd.read_csv('data/tests.csv')
    if date in test_file.columns:
        test_file = test_file.drop(columns = date)
    test_file.merge(tests[['Category', date]], 
                    on='Category', 
                    how='outer').to_csv('data/tests.csv', index=False)
    case_demo = state[state['Category'] != 'Test Completed'].rename(columns=({'Value' : date}))
    print('Case Demo fields:')
    for cd in case_demo.columns:
        print(cd)
    case_demo_file = pd.read_csv('data/case_demo.csv')
    if date in case_demo_file.columns:
        case_demo_file = case_demo_file.drop(columns = date)
    case_demo_file.merge(case_demo[['Category', date]], on='Category', how='outer').to_csv('data/case_demo.csv', index=False)
    death_demo = state[state['Category'] != 'Test Completed'].rename(columns=({'Value2' : date}))
    print('Death Demo Fields:')
    for dd in death_demo:
        print(dd)
    death_demo_file = pd.read_csv('data/death_demo.csv')
    if date in death_demo_file.columns:
        death_demo_file = death_demo_file.drop(columns = date)
    death_demo_file.merge(death_demo[['Category', date]], on='Category', how='outer').to_csv('data/death_demo.csv', index=False)


update_date = '{d.month}/{d.day}/{d.year}'.format(d=datetime.now())

la_state_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/arcgis/rest/services/State_Level_Information_2/FeatureServer/0/query?where=1%3D1&outFields=*&f=pjson'
la_county_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/arcgis/rest/services/Cases_by_Parish_1/FeatureServer/0/query?where=1%3D1&outFields=*&f=pjson'


la_covid(la_county_url,la_state_url, update_date)