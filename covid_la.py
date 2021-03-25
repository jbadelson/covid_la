# TODO: Add long format?

import os
import sys

module_path = os.path.abspath(os.path.join('code'))
if module_path not in sys.path:
    sys.path.append(module_path)
from urllib.request import urlopen
import json
from datetime import datetime, timedelta
import pandas as pd

import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_date_fmt = "%Y-%m-%d %H:%M:%S"

file_handler = logging.FileHandler(filename='covid_la.log', mode='w')
stdout_handler = logging.StreamHandler(sys.stdout)

file_handler.setLevel(logging.DEBUG)
stdout_handler.setLevel(logging.DEBUG)

file_log_format = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s', log_date_fmt)
stdout_log_format = logging.Formatter('%(message)s')

file_handler.setFormatter(file_log_format)
stdout_handler.setFormatter(stdout_log_format)

logger.addHandler(file_handler)
logger.addHandler(stdout_handler)

with open('static_data.json') as f:
    static_data = json.load(f)

update_date = datetime.now()
if os.name == 'nt':
    update_date_string = update_date.strftime('%#m/%#d/%#Y')
else:
    update_date_string = update_date.strftime('%-m/%-d/%Y')
file_date = f'{update_date.year}{update_date.month}{update_date.day}'
url_prefix = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/'
url_suffix = '/FeatureServer/0/query?where=1%3D1&outFields=*&f=pjson&token='


def tract_date():
    if datetime.today().weekday() >= 2:
        return update_date - timedelta(days=update_date.weekday())
    else:
        return update_date - timedelta(days=update_date.weekday(), weeks=1)


needed_datasets = {'cases_deaths_primary': 'test_this_sheet',  # Main LDH cases, deaths and test data
                   'cases_deaths_parish': 'Cases_and_Deaths_by_Race_by_Parish_and_Region',
                   'cases_deaths_region': 'Case_Deaths_Race_Region_new',
                   'vaccine_primary': 'Louisiana_COVID_Vaccination_Information',
                   'vaccine_parish': 'Vaccinations_by_Race_by_Parish',
                   'tracts': 'LA_2018_Tracts_' + tract_date().strftime('%m%d%Y')}


def get_datasets():
    try:
        url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services?f=pjson'
        data = urlopen(url).read()
        current_ldh_datasets = []
        for j in json.loads(data)['services']:
            current_ldh_datasets.append(j['name'])
        logger.info("Retrieved listing of current LDH datasets.")
    except Exception as e:
        logger.error('Failed to get LDH datasets')
        logger.exception('Function get_datasets failed with exception')
        logger.error(str(e))
        sys.exit(1)
    return current_ldh_datasets


def compare_datasets(current_ldh_datasets):
    try:
        added = set(current_ldh_datasets) - set(static_data["prior_datasets"])
        deleted = set(static_data["prior_datasets"]) - set(current_ldh_datasets)
        logger.info("Comparing datasets list against known LDH datasets.")
        if len(added) > 0:
            logger.warning(f'Found {len(added)} new datasets:')
            for a in added:
                logger.info(f'    {a}')
        else:
            logger.info("There are no new datasets.")
        if len(deleted) > 1:
            logger.warning(f"Found {len(deleted)} deleted datasets.")
            for d in deleted:
                logger.info(f"    {d}")
        else:
            logger.info("There are no deleted datasets.")
    except Exception as e:
        logger.error('Failed to compare datasets')
        logger.exception('Function compare_datasets failed with exception')
        logger.error(str(e))
        sys.exit(1)


def check_datasets(current_ldh_datasets):
    try:
        logger.info("Checking to ensure all needed datasets are available.")
        missing = []
        for d in needed_datasets:
            if needed_datasets[d] in current_ldh_datasets:
                logger.info(f'FOUND: {needed_datasets[d]}')
            else:
                missing.append(needed_datasets[d])
                logger.info(f"MISSING: {needed_datasets[d]}")

        if len(missing) > 0:
            logger.info('Datasets missing:')
            for m in missing:
                logger.info(f"    {m}")
            logger.error('Missing needed datasets')
            sys.exit(1)
    except Exception as e:
        logger.error('Failed to check datasets')
        logger.exception('Function check_datasets failed with exception')
        logger.error(str(e))
        sys.exit(1)

def esri_cleaner(url):
    data = urlopen(url).read()
    raw_json = json.loads(data)
    formatted_json = [feature['attributes'] for feature in raw_json['features']]
    return formatted_json

def csv_loader(file, date):
    df = pd.read_csv(file, dtype={'FIPS': object})
    if date in df.columns:
        df = df.drop(columns=date)
    return df

def cases_deaths(cases_deaths_primary):
    try:
        categories = {'Confirmed Cases': 'cases',
                      'Confirmed Deaths': 'deaths',
                      'Probable Cases': 'cases_probable',
                      'Probable Deaths': 'deaths_probable',
                      'Total Cases': 'cases_total',
                      'Total Deaths': 'deaths_total'}
        for c in categories:
            cdf = cases_deaths_primary[cases_deaths_primary['Measure'] == c].copy()
            cdf['Value'] = cdf['Value'].fillna(0).astype(int)
            cdf = cdf[['Group_', 'Value']].rename(columns={'Group_': 'County', 'Value': update_date_string})
            cfile = csv_loader(f'data/{categories[c]}.csv', update_date_string)
            cfile.merge(cdf,
                        on='County',
                        how='outer').to_csv(f'data/{categories[c]}.csv', index=False)
            logger.info(f"COMPLETE: {c}")
    except Exception as e:
        logger.error('Failed to download cases and death data')
        logger.exception('Function cases_deaths failed with exception')
        logger.error(str(e))
        sys.exit(1)

def tests(cases_deaths_primary):
    try:
        categories = {'Molecular Tests': 'tests_molecular',
                      'Antigen Tests': 'tests_antigen'}
        tdf = csv_loader('data/tests.csv', update_date_string)
        df = pd.DataFrame()
        for c in categories:
            cdf = cases_deaths_primary[cases_deaths_primary['Measure'] == c].copy()
            cdf = cdf.rename(columns={'Value': update_date_string, 'Group_': 'County'})
            cdf['Category'] = c
            cdf = cdf[['County', 'Category', update_date_string]]
            cfile = csv_loader(f"data/{categories[c]}.csv", update_date_string)
            df = df.append(cdf)
            (cfile
             .merge(cdf,
                    left_on=['County', 'Category'],
                    right_on=['County', 'Category'],
                    how='outer')
             .to_csv(f"data/{categories[c]}.csv", index=False))
            logger.info(f"COMPLETE: {c}")
        tdf = (tdf
               .merge(df,
                      left_on=['County', 'Category'],
                      right_on=['County', 'Category'],
                      how='outer'))
        tdf.to_csv('data/tests.csv', index=False)
        logger.info(f"COMPLETE: Total tests")
    except Exception as e:
        logger.error('Failed to download test data')
        logger.exception('Function tests failed with exception')
        logger.error(str(e))
        sys.exit(1)

def demos(cases_deaths_primary):
    try:
        categories = {'case': 'case_demo',
                      'death': 'death_demo'}
        for c in categories:
            cdf = cases_deaths_primary[
                (cases_deaths_primary['Measure'].isin(['Age', 'Gender'])) & (cases_deaths_primary['ValueType'] == c)].copy()
            cdf = cdf.rename(columns={'Value': update_date_string, 'Group_': 'Category'})
            cfile = csv_loader(f"data/{categories[c]}.csv", update_date_string)
            (cfile.merge(cdf[['Category', update_date_string]],
                         on='Category',
                         how='outer').to_csv(f'data/{categories[c]}.csv', index=False))
            logger.info(f"COMPLETE: {c} demographics")
    except Exception as e:
        logger.error('Failed to download demographic data')
        logger.exception('Function demos failed with exception')
        logger.error(str(e))
        sys.exit(1)

def timelines(cases_deaths_primary):
    try:
        categories = {'COVID-positive': 'hospitalizations',
                      'Date of Death': 'symptoms_date_of_death'}
        for c in categories:
            cdf = cases_deaths_primary[cases_deaths_primary['Measure'] == c].copy()
            cdf['Date'] = pd.to_datetime(cdf['Timeframe'])
            if c == 'COVID-positive':
                cdf = cdf.pivot(index='ValueType', columns='Date', values='Value')
            else:
                cdf = cdf.pivot(index='Measure', columns='Date', values='Value')
            cdf.columns = cdf.columns.strftime('%m/%d/%Y')
            cdf = cdf.reset_index().rename(columns={'ValueType': 'Category', 'Measure': 'Category'})
            cdf.to_csv(f'data/{categories[c]}.csv', index=False)
        logger.info(f"COMPLETE: Hospitalizations, ventilators and Date of Death")
    except Exception as e:
        logger.error('Failed to download hospitalizations, ventilators or date of death data')
        logger.exception('Function timelines failed with exception')
        logger.error(str(e))
        sys.exit(1)

def capacity(cases_deaths_primary):
    try:
        capacity = cases_deaths_primary[cases_deaths_primary['Measure'].isin(['Hospital Vents', 'Beds', 'ICU Beds'])].copy()
        capacity['LDH Region'] = capacity['Geography'].str[4:]
        capacity = capacity.replace({'Measure':
                                         {'Hospital Vents': 'Ventilators',
                                          'Beds': 'Hospital Beds',
                                          'ICU Beds': 'ICU'}})
        capacity_total = capacity.groupby(['LDH Region', 'Measure']).agg({'Value': 'sum'}).reset_index()
        capacity_total['Group_'] = 'Total'
        capacity = capacity.append(capacity_total, sort=True).rename(columns={'Value': update_date_string})
        capacity['Category'] = capacity['Measure'] + ' ' + capacity['Group_']
        capacity_file = csv_loader('data/capacity.csv', update_date_string)
        capacity_file.merge(capacity[['LDH Region', 'Category', update_date_string]], on=['Category', 'LDH Region'],
                            how='outer').to_csv('data/capacity.csv', index=False)
        logger.info('COMPLETE: Capacity')
    except Exception as e:
        logger.error('Failed to download capacity data')
        logger.exception('Function capacity failed with exception')
        logger.error(str(e))
        sys.exit(1)

def recovered(cases_deaths_primary):
    try:
        recovered_file = pd.read_csv('data/recovered.csv')
        recovered_file[update_date_string] = \
        cases_deaths_primary[cases_deaths_primary['Measure'] == 'Presumed Recovered']['Value'].values[0]
        recovered_file.to_csv('data/recovered.csv', index=False)
        logger.info("COMPLETE: Recoveries")
    except Exception as e:
        logger.error('Failed to download recovery data')
        logger.exception('Function recovered failed with exception')
        logger.error(str(e))
        sys.exit(1)

def date_of_test():
    try:
        dot = pd.read_excel('http://ldh.la.gov/assets/oph/Coronavirus/data/LA_COVID_TESTBYDAY_PARISH_PUBLICUSE.xlsx')
        dot['Lab Collection Date'] = dot['Lab Collection Date'].apply(lambda x: x.strftime('%m/%d/%Y'))

        categories = ['Daily Test Count',
                      'Daily Case Count',
                      'Daily Negative Test Count',
                      'Daily Positive Test Count', ]

        df = pd.DataFrame()

        for c in categories:
            cdf = pd.pivot(dot,
                           index='Parish',
                           columns='Lab Collection Date',
                           values=c)
            cdf.insert(0, 'Category', '')
            cdf['Category'] = c
            df = df.append(cdf)
        df.sort_values(by=['Parish', 'Category']).to_csv('data/cases_tests_dot.csv')
        logger.info(f'COMPLETE: Date of Test')
    except Exception as e:
        logger.error('Failed to date of test data')
        logger.exception('Function date_of_test failed with exception')
        logger.error(str(e))
        sys.exit(1)

def tracts():
    try:
        tract_week = pd.read_excel(
            'https://ldh.la.gov/assets/oph/Coronavirus/data/LA_COVID_TESTBYWEEK_TRACT_PUBLICUSE.xlsx')
        categories = ['Weekly Case Count', 'Weekly Negative Test Count', 'Weekly Positive Test Count', 'Weekly Test Count']
        df = pd.DataFrame()
        for c in categories:
            cdf = pd.pivot(tract_week,
                           index='Tract',
                           columns='Date for end of week',
                           values=c)
            cdf['Category'] = c
            df = df.append(cdf)
        df.sort_values(by=['Tract', 'Category']).to_csv('data/cases_tests_tracts.csv')

        tracts = pd.DataFrame(esri_cleaner(url_prefix + needed_datasets['tracts'] + url_suffix))
        tracts = tracts.rename(columns={'TractID': 'FIPS', 'CaseCount': update_date_string})
        tracts_file = csv_loader('data/tracts.csv', update_date_string)
        tracts_file.merge(tracts[['FIPS', update_date_string]],
                          on='FIPS',
                          how='outer').to_csv('data/tracts.csv', index=False)
        logger.info('COMPLETE: Tracts')
    except Exception as e:
        logger.error('Failed to download tract data')
        logger.exception('Function tracts failed with exception')
        logger.error(str(e))
        sys.exit(1)

def vaccinations():
    try:
        vaccines = pd.DataFrame(esri_cleaner(url_prefix + needed_datasets['vaccine_primary'] + url_suffix))
        vaccines['Category'] = vaccines['Measure']
        vaccines_primary = vaccines[vaccines['ValueType'] == 'count'].copy()
        vaccines_primary['Group_'] = vaccines_primary['Group_'].replace('N/A', 'State')
        vaccines_primary = vaccines_primary[['Group_', 'Category', 'Value']].rename(
            columns={'Value': update_date_string, 'Group_': 'Geography'})
        vaccines_parish = pd.DataFrame(esri_cleaner(url_prefix + needed_datasets['vaccine_parish'] + url_suffix))
        vaccines_parish['Geography'] = vaccines_parish['Parish']
        vaccines_parish_init = vaccines_parish[['Geography', 'SeriesInt']].copy()
        vaccines_parish_init['Category'] = 'Parish - Series Initiated'
        vaccines_parish_init = vaccines_parish_init.rename(columns={'SeriesInt': update_date_string})
        vaccines_parish_comp = vaccines_parish[['Geography', 'SeriesComp']].copy()
        vaccines_parish_comp['Category'] = 'Parish - Series Completed'
        vaccines_parish_comp = vaccines_parish_comp.rename(columns={'SeriesComp': update_date_string})
        vaccines_file = csv_loader('data/vaccines.csv', update_date_string)
        (vaccines_file
         .merge(
            vaccines_primary
                .append(vaccines_parish_init)
                .append(vaccines_parish_comp),
            on=['Geography', 'Category'],
            how='outer').to_csv('data/vaccines.csv', index=False))
        vaccines_state_demo = vaccines[vaccines['ValueType'] == 'percentage'].copy()
        vaccines_state_demo['Group_'] = vaccines_state_demo['Group_'].replace(static_data['age_replace'])
        vaccines_state_demo['Category'] = vaccines_state_demo['Measure'] + ' : ' + vaccines_state_demo['Group_']
        vaccines_state_demo['Value'] = vaccines_state_demo['Value'] / 100
        vaccines_state_demo = vaccines_state_demo[['Geography', 'Category', 'Value']].rename(
            columns={'Value': update_date_string})
        vaccines_parish_demo = pd.melt(vaccines_parish, id_vars='Parish', value_vars=static_data['parish_demos'])
        vaccines_parish_demo = vaccines_parish_demo.rename(columns={'variable': 'Category', 'value': update_date_string, 'Parish': 'Geography'})
        vaccines_parish_demo['Category'] = vaccines_parish_demo['Category'].replace(static_data['parish_replace'])
        vaccines_parish_demo[update_date_string] = vaccines_parish_demo[update_date_string] / 100
        vaccines_demo = vaccines_state_demo.append(vaccines_parish_demo)
        vaccines_demo_file = csv_loader('data/vaccines_demo.csv', update_date_string)
        vaccines_demo_file.merge(vaccines_demo, on=['Geography', 'Category'], how='outer').to_csv('data/vaccines_demo.csv', index=False)
        logger.info('COMPLETE: Vaccinations')
    except Exception as e:
        logger.error('FAILED: Vaccinations')
        logger.exception('Vaccinations failed with exception')
        logger.error(str(e))
        sys.exit(1)

def case_death_race():
    try:
        cases_race_parish = pd.DataFrame(esri_cleaner(url_prefix + needed_datasets['cases_deaths_parish'] + url_suffix))
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
        cases_race_parish = cases_race_parish.rename(columns={'variable': 'Race',
                                                              'PFIPS': 'FIPS',
                                                              'value': update_date_string})
        cases_race_parish_file = csv_loader('data/cases_deaths_by_race_parish.csv', update_date_string)
        cases_race_parish_file.merge(cases_race_parish[['FIPS', 'Race', update_date_string]],
                                     on=['FIPS', 'Race'],
                                     how='outer').to_csv('data/cases_deaths_by_race_parish.csv', index=False)
        logger.info('COMPLETED: Cases and deaths by race and parish')

        cases_deaths_race_region = pd.DataFrame(
            esri_cleaner(url_prefix + needed_datasets['cases_deaths_region'] + url_suffix))
        cases_deaths_race_region = (
            pd.melt(cases_deaths_race_region, id_vars=['LDH_Region', 'Race'], value_vars=['Deaths', 'Cases'])).sort_values(
            by='LDH_Region')
        cases_deaths_race_region = cases_deaths_race_region.rename(columns={'value': update_date_string})
        cases_deaths_race_region_file = csv_loader('data/cases_deaths_by_race_region.csv', update_date_string)
        cases_deaths_race_region_file.merge(
            cases_deaths_race_region[['LDH_Region', 'Race', 'variable', update_date_string]],
            on=['LDH_Region', 'Race', 'variable'],
            how='outer').to_csv('data/cases_deaths_by_race_region.csv', index=False)
        logger.info('COMPLETED: Cases and deaths by race and region')
    except Exception as e:
        logger.error('Failed to case and death by parish and region data')
        logger.exception('Function case_death_race failed with exception')
        logger.error(str(e))
        sys.exit(1)

def data_download(update_date):
    try:
        vaccinations()
        cases_deaths_primary = pd.DataFrame(esri_cleaner(url_prefix + needed_datasets['cases_deaths_primary'] + url_suffix))
        cases_deaths(cases_deaths_primary)
        tests(cases_deaths_primary)
        demos(cases_deaths_primary)
        timelines(cases_deaths_primary)
        capacity(cases_deaths_primary)
        recovered(cases_deaths_primary)
        date_of_test()
        tracts()
        case_death_race()
    except Exception as e:
        logger.exception('Function data_download failed with exception')
        logger.error(str(e))
        sys.exit(1)


def main():
    try:
        current_ldh_datasets = get_datasets()
        compare_datasets(current_ldh_datasets)
        check_datasets(current_ldh_datasets)
        data_download(update_date_string)
    except Exception as e:
        logger.exception('Function main failed with exception')
        logger.error(str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()