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

LDH is currently updating their dashboard once a day at noon. 
This script should be run after that update to capture the tallies for each 
day. If it is run multiple  times per day, it will overwrite any previous data 
for the day with the updated data.

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


def la_covid(parish_url, state_url, capacity_url, la_tract_url, parish_deaths_url, region_deaths_url, date):
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
    tracts = pd.DataFrame(esri_cleaner(la_tract_url))
    tracts = tracts.rename(columns = {'TractID_1' : 'FIPS', 'CaseCount' : date})
    tracts_file = pd.read_csv('data/tracts.csv', dtype = {'FIPS' : object})
    if date in tracts_file.columns:
        tracts_file = tracts_file.drop(columns = date)
    tracts_file.merge(tracts[['FIPS', date]],
                      on='FIPS',
                      how='outer').to_csv('data/tracts.csv', index=False)
    deaths_parish = pd.DataFrame(esri_cleaner(parish_deaths_url))
    for c in deaths_parish.iloc[:, 6:13].columns:
        deaths_parish[c] = pd.to_numeric(deaths_parish[c], errors='coerce')
    deaths_parish = (pd.melt(deaths_parish, 
                             id_vars=['PFIPS', 
                                      'Parish', 
                                      'LDHH'], 
                             value_vars=['American_Indian_Alaskan_Native', 
                                         'Asian', 
                                         'Black', 
                                         'Native_Hawaiian_Other_Pacific_Islander', 
                                         'Other', 
                                         'Unknown', 
                                         'White'])
                     .sort_values(by='PFIPS'))
    deaths_parish = deaths_parish.rename(columns = {'variable' : 'Race',
                                                    'PFIPS' : 'FIPS',
                                                    'value' : date})
    deaths_parish_file = pd.read_csv('data/deaths_by_race_parish.csv', dtype = {'FIPS' : object})
    if date in deaths_parish_file.columns:
        deaths_parish_file = deaths_parish_file.drop(columns = date)
    deaths_parish_file.merge(deaths_parish[['FIPS', 'Race', date]], 
                             on=['FIPS', 'Race'], 
                             how='outer').to_csv('data/deaths_by_race_parish.csv', index=False)
    deaths_region = pd.DataFrame(esri_cleaner(region_deaths_url))
    deaths_region = deaths_region.rename(columns = {'Deaths' : date})
    deaths_region_file = pd.read_csv('data/deaths_by_race_region.csv')
    if date in deaths_region_file.columns:
        deaths_region_file = deaths_region_file.drop(columns = date)
    deaths_region_file.merge(deaths_region[['LDH_Region', 'Race', date]],
                             on=['LDH_Region', 'Race'],
                             how='outer').to_csv('data/deaths_by_race_region.csv', index=False)
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
    capacity_file = pd.read_csv('data/capacity.csv', dtype=object)
    capacity_export = capacity_table(capacity, capacity_file, date)
    capacity_export.to_csv('data/capacity.csv', index=False)
    
    


update_date = '{d.month}/{d.day}/{d.year}'.format(d=datetime.now())

la_state_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/arcgis/rest/services/State_Level_Information_2/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=pjson'
la_county_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Cases_by_Parish/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=pjson'
region_capacity_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Louisiana_Vent_and_Bed_Report/FeatureServer/0/query?where=1%3D1&outFields=*&f=pjson'
la_tract_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/LA_2010_Tracts_04262020_2/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token='
la_deaths_parish_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Deaths_by_Race_by_Parish/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=pjson'
la_deaths_region_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Deaths_by_Race_by_Region/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=pjson'

la_covid(la_county_url,la_state_url, region_capacity_url, la_tract_url, la_deaths_parish_url, la_deaths_region_url, update_date)