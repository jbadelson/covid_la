# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 19:30:07 2020

@author: jeff adelson

This script will access the ArcGIS Rest endpoints of the dashboard used by
the Louisiana Department of Health to track the coronavirus/COVID-19 pandemic
and add the latest data to csv files to preserve this data for time-series
analysis.

LDH is currently updating their dashboard once a day at noon.
This script should be run after that update to capture the tallies for each
day. If it is run multiple  times per day, it will overwrite any previous data
for the day with the updated data.

"""

import pandas as pd
import json
from urllib.request import urlopen
from datetime import datetime
import numpy as np
pd.options.display.max_rows = 500
pd.options.display.max_columns = 500

def esri_cleaner(url):
    data = urlopen(url).read()
    raw_json = json.loads(data)
    formatted_json = [feature['attributes'] for feature in raw_json['features']]
    return formatted_json

def csv_loader(file, date):
    df = pd.read_csv('data/'+file, dtype={'FIPS' : object})
    if date in df.columns:
        df = df.drop(columns = date)
    return df

def la_covid(combined_url, deaths_parish_race_url, deaths_region_race_url, cases_parish_race_url, cases_region_race_url, date):
    if datetime.today().weekday() == 2:
        print("Today is Wednesday. Please enter new values.")
        probable = int(input("Probable deaths: "))
        recoveries = int(input("Recovered: "))
        tract = str(input("URL for tract data: "))
        static_data = {"probable" : probable, "recovered" : recoveries, "tract" : tract}
        with open("static_data.json", "w") as outfile:
            json.dump(static_data, outfile)
    else:
        with open("static_data.json") as infile:
            static_data = json.load(infile)
            probable = static_data["probable"]
            recoveries = static_data["recovered"]
            tract = static_data["tract"]


    la_tract_prefix = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/'
    la_tract_suffix = '/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson'
    la_tract_url = la_tract_prefix+tract+la_tract_suffix
    data = pd.DataFrame(esri_cleaner(combined_url))
    cases = data[data['Measure'] == 'Case Count'].copy()
    cases['Value'] = cases['Value'].fillna(0).astype(int)
    cases = cases[['Group', 'Value']].rename(columns = {'Group' : 'County', 'Value' : date})
    cases_file = csv_loader('cases.csv', date)
    cases_file.merge(cases,
                     on='County',
                     how='outer').to_csv('data/cases.csv', index=False)
    print('Cases exported.')

    deaths = data[data['Measure'] == 'Deaths'].copy()
    deaths.loc[:, 'Value'] = deaths['Value'].fillna(0).apply(np.int64)
    deaths = deaths[['Group', 'Value']].rename(columns = {'Group' : 'County', 'Value' : date})
    probable = pd.DataFrame(data = {'County' : ['Probable (Statewide)'], date : probable})
    deaths = deaths.append(probable)
    deaths_file = csv_loader('deaths.csv', date)
    deaths_file.merge(deaths,
                      on='County',
                      how='outer').to_csv('data/deaths.csv', index=False)
    print('Deaths exported.')

    tracts = pd.DataFrame(esri_cleaner(la_tract_url))
    tracts = tracts.rename(columns = {'TractID' : 'FIPS', 'CaseCount' : date})
    tracts_file = csv_loader('tracts.csv', date)
    tracts_file.merge(tracts[['FIPS', date]],
                      on='FIPS',
                      how='outer').to_csv('data/tracts.csv', index=False)
    print('Tracts exported.')

    deaths_parish = pd.DataFrame(esri_cleaner(deaths_parish_race_url))
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
    deaths_parish_file = csv_loader('deaths_by_race_parish.csv', date)
    deaths_parish_file.merge(deaths_parish[['FIPS', 'Race', date]],
                             on=['FIPS', 'Race'],
                             how='outer').to_csv('data/deaths_by_race_parish.csv', index=False)
    print('Deaths by race and parish exported.')

    deaths_region = pd.DataFrame(esri_cleaner(deaths_region_race_url))
    deaths_region = deaths_region.rename(columns = {'Deaths' : date})
    deaths_region_file = csv_loader('deaths_by_race_region.csv', date)
    deaths_region_file.merge(deaths_region[['LDH_Region', 'Race', date]],
                             on=['LDH_Region', 'Race'],
                             how='outer').to_csv('data/deaths_by_race_region.csv', index=False)
    print('Deaths by race and region exported.')
# Start cases by race

    cases_race_parish = pd.DataFrame(esri_cleaner(cases_parish_race_url))
    for c in cases_race_parish.iloc[:, 6:16].columns:
        cases_race_parish[c] = pd.to_numeric(cases_race_parish[c], errors='coerce')
    cases_race_parish = (pd.melt(cases_race_parish,
                             id_vars=['PFIPS',
                                      'Parish',
                                      'LDHH'],
                             value_vars=['Deaths_Black',
                                         'Deaths_White',
                                         'Deaths_Other',
                                         'Deaths_Unknown',
                                         'Cases_Black',
                                         'Cases_White',
                                         'Cases_Other',
                                         'Cases_Unknown'])
                     .sort_values(by='PFIPS'))
    cases_race_parish['PFIPS'] = cases_race_parish.astype(str)
    cases_race_parish = cases_race_parish.rename(columns = {'variable' : 'Race',
                                                    'PFIPS' : 'FIPS',
                                                    'value' : date})
    cases_race_parish_file = csv_loader('cases_deaths_by_race_parish.csv', date)
    cases_race_parish_file.merge(cases_race_parish[['FIPS', 'Race', date]],
                             on=['FIPS', 'Race'],
                             how='outer').to_csv('data/cases_deaths_by_race_parish.csv', index=False)
    print('Cases and Deaths by race and parish exported.')

    cases_deaths_race_region = pd.DataFrame(esri_cleaner(cases_region_race_url))
    cases_deaths_race_region = (pd.melt(cases_deaths_race_region, id_vars=['LDH_Region', 'Race'], value_vars=['Deaths', 'Cases'])).sort_values(by='LDH_Region')
    cases_deaths_race_region = cases_deaths_race_region.rename(columns = {'value' : date})
    # cases_deaths_region = cases_deaths_region.rename(columns = {'Deaths' : date})
    cases_deaths_race_region_file = csv_loader('cases_deaths_by_race_region.csv', date)
    cases_deaths_race_region_file.merge(cases_deaths_race_region[['LDH_Region', 'Race', 'variable', date]],
                             on=['LDH_Region', 'Race', 'variable'],
                             how='outer').to_csv('data/cases_deaths_by_race_region.csv', index=False)
    print('Deaths by race and region exported.')

# end cases by race



    tests_detail_file = csv_loader('tests.csv', date)
    tests_public = data[data['Measure'] == 'State Tests'].copy()
    tests_public = tests_public.rename(columns = {'Value' : date, 'Group' : 'County'})
    tests_public['Category'] = 'Public'
    tests_private = data[data['Measure'] == 'Commercial Tests'].copy()
    tests_private = tests_private.rename(columns = {'Value' : date, 'Group' : 'County'})
    tests_private['Category'] = 'Private'
    tests = tests_public[['County', 'Category', date]].append(tests_private[['County', 'Category', date]])
    tests_detail_file.merge(tests,
                            left_on=['County', 'Category'],
                            right_on=['County', 'Category'],
                            how='outer').to_csv('data/tests.csv', index=False)
    print('Tests exported.')

    case_demo = data[(data['Measure'].isin(['Age', 'Gender'])) & (data['ValueType'] == 'case')].copy()
    case_demo = case_demo.rename(columns = {'Value' : date, 'Group' : 'Category'})
    case_demo_file = csv_loader('case_demo.csv', date)
    case_demo_file.merge(case_demo[['Category', date]], on='Category', how='outer').to_csv('data/case_demo.csv', index=False)
    print('Case demographics exported.')

    death_demo = data[(data['Measure'].isin(['Age', 'Gender'])) & (data['ValueType'] == 'death')].copy()
    death_demo = death_demo.rename(columns = {'Value' : date, 'Group' : 'Category'})
    death_demo_file = csv_loader('death_demo.csv', date)
    death_demo_file.merge(death_demo[['Category', date]], on='Category', how='outer').to_csv('data/death_demo.csv', index=False)
    print('Death demographics exported.')

    capacity = data[data['Measure'].isin(['Hospital Vents', 'Beds', 'ICU Beds'])].copy()
    capacity['LDH Region'] = capacity['Geography'].str[4:]
    capacity = capacity.replace({'Measure' :
                                 {'Hospital Vents' : 'Ventilators',
                                  'Beds' : 'Hospital Beds',
                                  'ICU Beds' : 'ICU'}})
    capacity_total = capacity.groupby(['LDH Region', 'Measure']).agg({'Value' : 'sum'}).reset_index()
    capacity_total['Group'] = 'Total'
    capacity = capacity.append(capacity_total, sort=True).rename(columns = {'Value' : date})
    capacity['Category'] = capacity['Measure']+' '+capacity['Group']
    capacity_file = csv_loader('capacity.csv', date)
    capacity_file.merge(capacity[['LDH Region', 'Category', date]], on=['Category', 'LDH Region'], how='outer').to_csv('data/capacity.csv', index=False)
    print('Capacity exported.')

    hosp = data[data['Measure'] == 'COVID-positive'].copy()
    hosp['Date'] = pd.to_datetime(hosp['Timeframe'])
    hosp = hosp.pivot(index='ValueType', columns='Date', values='Value')
    hosp.columns = hosp.columns.strftime('%m/%d/%Y')
    hosp = hosp.reset_index().rename(columns={'ValueType' : 'Category'})
    hosp = hosp.replace({'ValueType' : {'hospitalized' : 'Hospitalized', 'on vent' : 'Ventilators'}})
    hosp.to_csv('data/hospitalizations.csv', index=False)
    print('Hospitalizations exported.')

    onset = data[data['Measure'].isin(['Onset Date', 'Date of Death'])].copy()
    onset['Date'] = pd.to_datetime(onset['Timeframe'])
    onset = onset.pivot(index='Measure', columns='Date', values='Value')
    onset.columns = onset.columns.strftime('%m/%d/%Y')
    onset = onset.reset_index().rename(columns={'Measure' : 'Category'})
    onset = onset.replace({'Category' : {'Date of Death' : 'Deaths', 'Onset Date' : 'Cases'}})
    onset.to_csv('data/symptoms_date_of_death.csv', index=False)
    print('Onset/Date of Death exported.')
    recovered = pd.read_csv('data/recovered.csv')
    recovered[date] = recoveries
    recovered.to_csv('data/recovered.csv', index = False)

combined_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/arcgis/rest/services/Combined_COVID_Reporting/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=json'
la_deaths_parish_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Deaths_by_Race_by_Parish/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=pjson'
la_deaths_region_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Deaths_by_Race_by_Region/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=pjson'
la_cases_parish_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Cases_and_Deaths_by_Race_by_Parish/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token='
la_cases_region_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Cases_and_Deaths_by_Race_by_Region/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token='
update_date = '{d.month}/{d.day}/{d.year}'.format(d=datetime.now())

la_covid(combined_url, la_deaths_parish_url, la_deaths_region_url, la_cases_parish_url, la_cases_region_url, update_date)
