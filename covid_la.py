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

def la_covid(combined_url, deaths_parish_race_url, deaths_region_race_url, cases_parish_race_url, cases_region_race_url, cases_tests_dot_url, date):
    with open("static_data.json") as infile:
        static_data = json.load(infile)

    if datetime.today().weekday() == 2:
        print("Today is Wednesday. Please enter new location of tract data.")
        tract = str(input("URL for tract data (get URL at https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services): "))
        static_data["tract"] = tract
        with open("static_data.json", "w") as outfile:
            json.dump(static_data, outfile)
    elif datetime.today().weekday() == 1 or datetime.today().weekday() == 3:
        print("Please enter new vaccine information.")
        vacInitiated = int(input("Vaccine series initiated: ") or static_data["vacInitiated"])
        vacCompleted = int(input("Vaccine series completed: ") or static_data["vacCompleted"])
        vacAdministered = int(input("Vaccine doses administered: ") or static_data["vacAdministered"])
        newVacAdministered = int(input("New vaccine doses administered since last update: ") or static_data["newVacAdministered"])
        vacProviders = int(input("Providers enrolled: ") or static_data["vacProviders"])
        vacDistPhase = str(input("Current distribution phase: ") or static_data["vacDistPhase"])
        static_data["vacInitiated"] = vacInitiated
        static_data["vacCompleted"] = vacCompleted
        static_data["vacAdministered"] = vacAdministered
        static_data["newVacAdministered"] = newVacAdministered
        static_data["vacProviders"] = vacProviders
        static_data["vacDistPhase"] = vacDistPhase
        with open("static_data.json", "w") as outfile:
            json.dump(static_data, outfile)

    la_tract_prefix = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/'
    la_tract_suffix = '/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson'
    la_tract_url = la_tract_prefix+static_data["tract"]+la_tract_suffix

    data = pd.DataFrame(esri_cleaner(combined_url))

    dot = pd.read_excel('http://ldh.la.gov/assets/oph/Coronavirus/data/LA_COVID_TESTBYDAY_PARISH_PUBLICUSE.xlsx')
    dot['Lab Collection Date'] = dot['Lab Collection Date'].apply(lambda x: x.strftime('%m/%d/%Y'))

    dot_tests = pd.pivot(dot,
                   index='Parish',
                   columns='Lab Collection Date',
                   values='Daily Test Count')
    dot_tests.insert(0,'Category', '')
    dot_tests['Category'] = 'Tests'
    dot_cases = pd.pivot(dot,
                        index='Parish',
                        columns='Lab Collection Date',
                        values='Daily Case Count')
    dot_cases['Category'] = 'Cases'
    dot_neg_tests = pd.pivot(dot,
                        index='Parish',
                        columns='Lab Collection Date',
                        values='Daily Negative Test Count')
    dot_neg_tests['Category'] = 'Negative Tests'
    dot_pos_tests = pd.pivot(dot,
                            index='Parish',
                            columns='Lab Collection Date',
                            values='Daily Positive Test Count')
    dot_pos_tests['Category'] = 'Positive Tests'
    dot_tests.append(dot_cases).append(dot_neg_tests).append(dot_pos_tests).sort_values(by=['Parish', 'Category']).to_csv('data/cases_tests_dot.csv')
    print('Cases and tests by parish and date of test exported.')

    tract_week = pd.read_excel('https://ldh.la.gov/assets/oph/Coronavirus/data/LA_COVID_TESTBYWEEK_TRACT_PUBLICUSE.xlsx')
    tract_cases = pd.pivot(tract_week,
                            index='Tract',
                            columns='Date for end of week',
                            values='Weekly Case Count')
    tract_cases['Category'] = 'Cases'
    tract_neg_tests = pd.pivot(tract_week,
                                index='Tract',
                                columns='Date for end of week',
                                values='Weekly Negative Test Count')
    tract_neg_tests['Category'] = 'Negative Tests'
    tract_pos_tests = pd.pivot(tract_week,
                            index='Tract',
                            columns='Date for end of week',
                            values='Weekly Positive Test Count')
    tract_pos_tests['Category'] = 'Positive Tests'
    tract_tests = pd.pivot(tract_week,
                            index='Tract',
                            columns='Date for end of week',
                            values='Weekly Test Count')
    tract_tests['Category'] = 'Tests'
    tract_week.append(tract_cases).append(tract_neg_tests).append(tract_pos_tests).sort_values(by=['Tract', 'Category']).to_csv('data/cases_tests_tracts.csv')

    case_demo = data[(data['Measure'].isin(['Age', 'Gender'])) & (data['ValueType'] == 'case')].copy()
    case_demo = case_demo.rename(columns = {'Value' : date, 'Group_' : 'Category'})
    case_demo_file = csv_loader('case_demo.csv', date)
    case_demo_file.merge(case_demo[['Category', date]], on='Category', how='outer').to_csv('data/case_demo.csv', index=False)
    print('Case demographics exported.')

    death_demo = data[(data['Measure'].isin(['Age', 'Gender'])) & (data['ValueType'] == 'death')].copy()
    death_demo = death_demo.rename(columns = {'Value' : date, 'Group_' : 'Category'})
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
    capacity_total['Group_'] = 'Total'
    capacity = capacity.append(capacity_total, sort=True).rename(columns = {'Value' : date})
    capacity['Category'] = capacity['Measure']+' '+capacity['Group_']
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
    cases_deaths_race_region_file = csv_loader('cases_deaths_by_race_region.csv', date)
    cases_deaths_race_region_file.merge(cases_deaths_race_region[['LDH_Region', 'Race', 'variable', date]],
                             on=['LDH_Region', 'Race', 'variable'],
                             how='outer').to_csv('data/cases_deaths_by_race_region.csv', index=False)
    print('Deaths by race and region exported.')

    tests_molecular_file = csv_loader('tests_molecular.csv', date)
    tests_molecular = data[data['Measure'] == 'Molecular Tests'].copy()
    tests_molecular = tests_molecular.rename(columns = {'Value' : date, 'Group_' : 'County'})
    tests_molecular['Category'] = 'Molecular Tests'
    tests_molecular = tests_molecular[['County', 'Category', date]]
    tests_molecular_file.merge(tests_molecular,
                            left_on=['County', 'Category'],
                            right_on=['County', 'Category'],
                            how='outer').to_csv('data/tests_molecular.csv', index=False)
    print('Molecular Tests exported.')

    tests_antigen_file = csv_loader('tests_antigen.csv', date)
    tests_antigen = data[data['Measure'] == 'Antigen Tests'].copy()
    tests_antigen = tests_antigen.rename(columns = {'Value' : date, 'Group_' : 'County'})
    tests_antigen['Category'] = 'Antigen Tests'
    tests_antigen = tests_antigen[['County', 'Category', date]]
    tests_antigen_file.merge(tests_antigen,
                            left_on=['County', 'Category'],
                            right_on=['County', 'Category'],
                            how='outer').to_csv('data/tests_antigen.csv', index=False)

    tests_file = csv_loader('tests.csv', date)
    tests = tests_antigen[['County', 'Category', date]].append(tests_molecular[['County', 'Category', date]])
    tests_file.merge(tests,
                     left_on=['County', 'Category'],
                     right_on=['County', 'Category'],
                     how='outer').to_csv('data/tests.csv', index=False)

    cases = data[data['Measure'] == 'Confirmed Cases'].copy()
    cases['Value'] = cases['Value'].fillna(0).astype(int)
    cases = cases[['Group_', 'Value']].rename(columns = {'Group_' : 'County', 'Value' : date})
    cases_file = csv_loader('cases.csv', date)
    cases_file.merge(cases,
                     on='County',
                     how='outer').to_csv('data/cases.csv', index=False)
    print('Cases exported.')

    deaths = data[data['Measure'] == 'Confirmed Deaths'].copy()
    deaths.loc[:, 'Value'] = deaths['Value'].fillna(0).apply(np.int64)
    deaths = deaths[['Group_', 'Value']].rename(columns = {'Group_' : 'County', 'Value' : date})

    deaths_file = csv_loader('deaths.csv', date)
    deaths_file.merge(deaths,
                      on='County',
                      how='outer').to_csv('data/deaths.csv', index=False)
    print('Deaths exported.')

    probable_cases = data[data['Measure'] == 'Probable Cases'].copy()
    probable_cases['Value'] = probable_cases['Value'].fillna(0).astype(int)
    probable_cases = probable_cases[['Group_', 'Value']].rename(columns = {'Group_' : 'County', 'Value' : date})
    probable_cases_file = csv_loader('cases_probable.csv', date)
    probable_cases_file.merge(probable_cases,
                     on='County',
                     how='outer').to_csv('data/cases_probable.csv', index=False)
    print('Probable Cases exported.')

    probable_deaths = data[data['Measure'] == 'Probable Deaths'].copy()
    probable_deaths.loc[:, 'Value'] = probable_deaths['Value'].fillna(0).apply(np.int64)
    probable_deaths = probable_deaths[['Group_', 'Value']].rename(columns = {'Group_' : 'County', 'Value' : date})

    probable_deaths_file = csv_loader('deaths_probable.csv', date)
    probable_deaths_file.merge(probable_deaths,
                      on='County',
                      how='outer').to_csv('data/deaths_probable.csv', index=False)
    print('Probable Deaths exported.')

    total_cases = data[data['Measure'] == 'Total Cases'].copy()
    total_cases['Value'] = total_cases['Value'].fillna(0).astype(int)
    total_cases = total_cases[['Group_', 'Value']].rename(columns = {'Group_' : 'County', 'Value' : date})
    total_cases_file = csv_loader('cases_total.csv', date)
    total_cases_file.merge(total_cases,
                     on='County',
                     how='outer').to_csv('data/cases_total.csv', index=False)
    print('Total Cases exported.')

    total_deaths = data[data['Measure'] == 'Total Deaths'].copy()
    total_deaths.loc[:, 'Value'] = total_deaths['Value'].fillna(0).apply(np.int64)
    total_deaths = total_deaths[['Group_', 'Value']].rename(columns = {'Group_' : 'County', 'Value' : date})

    total_deaths_file = csv_loader('deaths_total.csv', date)
    total_deaths_file.merge(total_deaths,
                      on='County',
                      how='outer').to_csv('data/deaths_total.csv', index=False)
    print('Total Deaths exported.')

    recovered_file = pd.read_csv('data/recovered.csv')
    recovered_file[date] = data[data['Measure'] == 'Presumed Recovered']['Value'].values[0]
    recovered_file.to_csv('data/recovered.csv', index=False)

    tracts = pd.DataFrame(esri_cleaner(la_tract_url))
    tracts = tracts.rename(columns = {'TractID' : 'FIPS', 'CaseCount' : date})
    tracts_file = csv_loader('tracts.csv', date)
    tracts_file.merge(tracts[['FIPS', date]],
                      on='FIPS',
                      how='outer').to_csv('data/tracts.csv', index=False)
    print('Tracts exported.')

    vaccines = pd.DataFrame({
                            "Category" :[
                                            "Vaccines Initiated",
                                            "Vaccines Completed",
                                            "Vaccines Administered",
                                            "New Vaccines Administered",
                                            "Vaccine Providers",
                                            "Vaccine Distribution Phase"
                                        ],
                            date :      [
                                            static_data["vacInitiated"],
                                            static_data["vacCompleted"],
                                            static_data["vacAdministered"],
                                            static_data["newVacAdministered"],
                                            static_data["vacProviders"],
                                            static_data["vacDistPhase"]
                                        ]

                            })
    vaccines_file = csv_loader('vaccines.csv', date)
    vaccines_file.merge(vaccines,
                        on='Category',
                        how='outer').to_csv('data/vaccines.csv', index=False)

combined_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/test_this_sheet/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token='
la_deaths_parish_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Deaths_by_Race_by_Parish/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=pjson'
la_deaths_region_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Deaths_by_Race_by_Region/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=pjson'
la_cases_parish_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Cases_and_Deaths_by_Race_by_Parish_and_Region/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token='
la_cases_region_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Case_Deaths_Race_Region_new/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token='
la_cases_tests_dot_url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Parish_Case_and_Test_Counts_by_Collect_Date/FeatureServer/0/query?where=1%3D1&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson&token='
update_date = '{d.month}/{d.day}/{d.year}'.format(d=datetime.now())
la_covid(combined_url, la_deaths_parish_url, la_deaths_region_url, la_cases_parish_url, la_cases_region_url, la_cases_tests_dot_url, update_date)
