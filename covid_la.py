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
    deaths = cases.copy()
    cases = cases.rename(columns = {'Cases' : date, 'PFIPS' : 'FIPS'})
    cases.loc[cases['PARISH'] == 'Parish Under Investigation', 'FIPS'] = '22999'
    case_file = pd.read_csv('data/cases.csv', dtype = {'FIPS' : object})
    if date in case_file.columns:
        case_file = case_file.drop(columns = date)
    print(case_file)
    print(date)
    case_file.merge(cases[['FIPS', date]], 
                    on='FIPS', 
                    how='outer').fillna(0).to_csv('data/cases.csv', index=False)
    deaths = deaths.rename(columns = {'Deaths' : date, 'PFIPS' : 'FIPS'})
    death_file = pd.read_csv('data/deaths.csv', dtype = {'FIPS' : object})
    if date in death_file.columns:
        death_file = death_file.drop(columns = date)
    death_file.merge(deaths[['FIPS', date]],
                      on='FIPS',
                      how='outer').fillna(0).to_csv('data/deaths.csv', index=False)
    state = pd.DataFrame(esri_cleaner(la_state_url))
    tests = state[state['Category'] == 'Test Completed'].rename(columns = ({'Value' : date}))
    tests['FIPS'] = 22
    test_file = pd.read_csv('data/tests.csv')
#    print(test_file)
#    print(tests)
    if date in test_file.columns:
        test_file = test_file.drop(columns = date)
    test_file.merge(tests[['FIPS', date]], 
                    on='FIPS', 
                    how='outer').fillna(0).to_csv('data/tests.csv', index=False)
    ages = state[state['Category'] != 'Test Completed'].rename(columns=({'Value' : date}))
    age_file = pd.read_csv('data/ages.csv')
    if date in age_file.columns:
        age_file = age_file.drop(columns = date)
    age_file.merge(ages[['Category', date]], on='Category', how='outer').to_csv('data/ages.csv', index=False)

update_date = datetime.now().strftime('%m/%d/%Y')

la_state_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/arcgis/rest/services/State_Level_Information_1/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token='
la_county_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/arcgis/rest/services/Cases_by_Parish_1/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token='
la_covid(la_county_url, la_state_url, update_date)
