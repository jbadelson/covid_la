#!env/bin/python
import os
import sys

module_path = os.path.abspath(os.path.dirname(__file__))
if module_path not in sys.path:
    sys.path.append(module_path)
from urllib.request import urlopen
import json
from datetime import datetime, timedelta
import pandas as pd
from tableauscraper import TableauScraper as TS

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_date_fmt = "%Y-%m-%d %H:%M:%S"

file_handler = logging.FileHandler(filename='covid_la.log', mode='w')
stdout_handler = logging.StreamHandler(sys.stdout)

file_handler.setLevel(logging.DEBUG)
stdout_handler.setLevel(logging.DEBUG)

file_log_format = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s', log_date_fmt)
stdout_log_format = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s', log_date_fmt)

file_handler.setFormatter(file_log_format)
stdout_handler.setFormatter(stdout_log_format)

logger.addHandler(file_handler)
logger.addHandler(stdout_handler)

with open(f'{module_path}/static_data.json') as f:
    static_data = json.load(f)

#update_date = pd.to_datetime('2022-03-01')
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


needed_datasets = {'cases_deaths_primary' : 'Louisiana_COVID_Reporting',  # Main LDH cases, deaths and test data
                   'cases_deaths_parish' : 'Cases_and_Deaths_by_Race_by_Parish',
                   'cases_deaths_region' : 'Cases_and_Deaths_by_Region_by_Race',
                   'vaccine_primary' : 'Louisiana_COVID_Vaccination_Info',
                   'vaccine_parish' : 'Louisiana_COVID_Vaccination_by_Parish',
                   'vaccine_tract': 'Louisiana_COVID_Vaccination_by_Tract',
                   'vaccine_full_demo' : 'Louisiana_COVID_Vaccination_Demographics',
                   'tracts': 'Louisiana_COVID_Cases_by_Tract'}


def get_datasets():
    try:
        url = 'https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services?f=pjson'
        data = urlopen(url).read()
        current_ldh_datasets = []
        for j in json.loads(data)['services']:
            if j['name'] == 'Cases and Deaths by Race by Parish and Region _ updated':
                current_ldh_datasets.append('Cases%20and%20Deaths%20by%20Race%20by%20Parish%20and%20Region%20_%20updated')
            else:
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
                logger.warning(f'    {a}')
        else:
            logger.info("There are no new datasets.")
        if len(deleted) > 1:
            logger.warning(f"Found {len(deleted)} deleted datasets.")
            for d in deleted:
                logger.warning(f"    {d}")
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
                logger.error(f"MISSING: {needed_datasets[d]}")

        if len(missing) > 0:
            logger.error('Datasets missing:')
            for m in missing:
                logger.error(f"    {m}")
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

def download(dataset):
    offset=0
    record_count = 2000

    combined = pd.DataFrame()
    while record_count == 2000:
        batch_records = pd.DataFrame(esri_cleaner(url_prefix + dataset + url_suffix + f'&resultOffset={offset}'))
        combined = combined.append(batch_records)
        offset = len(combined)
        record_count = len(batch_records)
    return combined

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
            cfile = csv_loader(f'{module_path}/data/{categories[c]}.csv', update_date_string)
            cfile.merge(cdf,
                        on='County',
                        how='outer').to_csv(f'{module_path}/data/{categories[c]}.csv', index=False)
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
        tdf = csv_loader(f'{module_path}/data/tests.csv', update_date_string)
        df = pd.DataFrame()
        for c in categories:
            cdf = cases_deaths_primary[cases_deaths_primary['Measure'] == c].copy()
            cdf = cdf.rename(columns={'Value': update_date_string, 'Group_': 'County'})
            cdf['Category'] = c
            cdf = cdf[['County', 'Category', update_date_string]]
            cfile = csv_loader(f"{module_path}/data/{categories[c]}.csv", update_date_string)
            df = df.append(cdf)
            (cfile
             .merge(cdf,
                    left_on=['County', 'Category'],
                    right_on=['County', 'Category'],
                    how='outer')
             .to_csv(f"{module_path}/data/{categories[c]}.csv", index=False))
            logger.info(f"COMPLETE: {c}")
        tdf = (tdf
               .merge(df,
                      left_on=['County', 'Category'],
                      right_on=['County', 'Category'],
                      how='outer'))
        tdf.to_csv(f'{module_path}/data/tests.csv', index=False)
        logger.info("COMPLETE: Total tests")
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
            cfile = csv_loader(f"{module_path}/data/{categories[c]}.csv", update_date_string)
            (cfile.merge(cdf[['Category', update_date_string]],
                         on='Category',
                         how='outer').to_csv(f'{module_path}/data/{categories[c]}.csv', index=False))
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
            cdf = cdf[cdf['Timeframe'] != 'current']
            cdf.loc[cdf["Timeframe"] != 'current', 'Date'] = cdf['Timeframe']
            cdf.loc[cdf["Timeframe"] == 'current', 'Date'] = datetime.now()
            cdf['Date'] = pd.to_datetime(cdf['Date'])
#            cdf['Date'] = pd.to_datetime(cdf['Timeframe'])
            if c == 'COVID-positive':
                cdf = cdf.pivot(index='ValueType', columns='Date', values='Value')
            else:
                cdf = cdf.pivot(index='Measure', columns='Date', values='Value')
            cdf.columns = cdf.columns.strftime('%m/%d/%Y')
            cdf = cdf.reset_index().rename(columns={'ValueType': 'Category', 'Measure': 'Category'})
            cdf.to_csv(f'{module_path}/data/{categories[c]}.csv', index=False)
        logger.info("COMPLETE: Hospitalizations, ventilators and Date of Death")
    except Exception as e:
        logger.error('Failed to download hospitalizations, ventilators or date of death data')
        logger.exception('Function timelines failed with exception')
        logger.error(str(e))
        sys.exit(1)

def tableau_hosp():
    try:
        url = 'https://analytics.la.gov/t/LDH/views/covid19_hosp_vent_reg/Hosp_vent_c'
        ts = TS(logLevel='ERROR')
        ts.loads(url)
        workbook = ts.getWorkbook()
        sheets = workbook.getSheets()
        ws = ts.getWorksheet('Hospitalization and Ventilator Usage')
        filters = ws.getFilters()
        hosp = pd.DataFrame()
        for t in filters[0]['values']:
            wb = ws.setFilter('Region', t, dashboardFilter=True)
            regionWs = wb.getWorksheet('Hospitalization and Ventilator Usage')
            df = pd.DataFrame(regionWs.data)
            df = df.rename(columns = {'SUM(laggedCOVID Positive inHosp)-alias' : 'hospitalized - '+t, 'SUM(laggedCOVID Positive onVent)-alias' : 'on_vent - '+t, 'DAY(DateTime)-value' : 'date'})
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%m/%d/%Y')
            df = df.set_index('date')
            hosp = pd.concat([hosp, df[['hospitalized - '+t, 'on_vent - '+t]]], axis = 1)
        hosp = hosp.transpose().reset_index()
        h = hosp['index'].str.split(' - ', expand=True).rename(columns={0 : 'Category', 1 : 'Geography'})
        hosp = pd.concat([h[['Geography', 'Category']], hosp], axis=1)
        hosp['Geography'] = 'Region '+hosp['Geography']
        hosp = hosp.drop('index', axis=1)
        hosp.to_csv(f'{module_path}/data/region_hosp.csv', index=False)
        pd.concat([hosp[hosp['Category']=='hospitalized'].iloc[:,2:].sum().rename('hospitalized'),hosp[hosp['Category']=='on_vent'].iloc[:,2:].sum().rename('on_vent')], axis=1).transpose().reset_index().rename(columns={'index' : 'Category'}).to_csv(f'{module_path}/data/hospitalizations.csv', index=False)
        logger.info('COMPLETE: Regional hospitalization data from Tableau')
    except Exception as e:
        logger.error('Failed to download regional hospitalization data')
        logger.exception('Function tableau_hosp failed with exception')
        logger.error(str(e))
        sys.exit(1)
    try:
        url = 'https://analytics.la.gov/t/LDH/views/COVID19_deathsxtime/OverTime'
        ts = TS(logLevel='ERROR')
        ts.loads(url)
        workbook = ts.getWorkbook()
        sheets = workbook.getSheets()
        ws = ts.getWorksheet('Deaths by date of death')
        ws.data[['DAY(Timeframe)-value','SUM(Deaths)-value']].dtypes
        deaths_dot = pd.DataFrame(ws.data)
        deaths_dot = deaths_dot.rename(columns = {'DAY(Timeframe)-value' : 'date', 'SUM(Deaths)-value' : 'Date of Death'})
        deaths_dot['date'] = pd.to_datetime(deaths_dot['date']).dt.strftime('%m/%d/%Y')
        deaths_dot = deaths_dot.set_index('date')
        deaths_dot[['Date of Death']].transpose().reset_index().rename(columns={'index' : 'Category'}).to_csv(f'{module_path}/data/symptoms_date_of_death.csv', index=False)
    except Exception as e:
        logger.error('Failed to download date of death data')
        logger.exception('Function tableau_hosp failed with exception')
        logger.error(str(e))
        sys.exit(1)
    try:
        url = 'https://analytics.la.gov/t/LDH/views/extracovidinfo/Dashboard1'
        ts = TS()
        ts.loads(url)
        ws = ts.getWorksheet('New Reinfections')
        new_reinfections = ws.data['AGG(SUM(INT([Value])))-alias'][0]
        ws = ts.getWorksheet('Total Reinfections')
        total_reinfections = ws.data['AGG(SUM(INT([Value])))-alias'][0]
        reinfect = pd.DataFrame({'Category' : ['New Reinfections', 'Total Reinfections'], update_date_string : [new_reinfections, total_reinfections]})
        reinfect_file = csv_loader(f'{module_path}/data/reinfect.csv', update_date_string)
        reinfect_file = reinfect_file.merge(reinfect, on='Category', how='outer')
        reinfect_file.to_csv(f'{module_path}/data/reinfect.csv', index=False)
    except Exception as e:
        logger.error('Failed to download reinfection info')
        logger.exception('Function tableau_hosp failed with exception')
        logger.error(str(e))
        #sys.exit(1)
    try:
        variants_file = pd.read_csv(f'{module_path}/data/variants.csv')
        url = 'https://analytics.la.gov/t/LDH/views/extracovidinfo/Dashboard1'
        ts = TS()
        ts.loads(url)
        workbook = ts.getWorkbook()
        sheets = workbook.getSheets()
        ws = ts.getWorksheet('Variants')
        variants = ws.data
        variants_dict = {}
        variants_date = variants['Timeframe-alias'][0]
        for i,r in variants.iterrows():
            variants_dict[r['Group-alias']] = r['AGG(SUM(FLOAT([Value])))-alias']/100
        for k in variants_dict:
            variants_file.loc[variants_date, k] = variants_dict[k]
        variants_file.to_csv(f'{module_path}/data/variants.csv')
    except Exception as e:
        logger.error('Failed to download variant info')
        logger.exception('Function tableau_hosp failed with exception')
        logger.error(str(e))
        #sys.exit(1)
    try:
        url = 'https://analytics.la.gov/t/LDH/views/casesxcollection_reinf/FirstandReinfections?%3Aembed=y&%3AisGuestRedirectFromVizportal=y'
        ts = TS()
        ts.loads(url)
        sheets = workbook.getSheets()
        ws = ts.getWorksheet('First and Reinfections by Collection Date')
        filters = ws.getFilters()
        # wb = ws.setFilter('region', '2 - Baton Rouge')
        # regionWs = wb.getWorksheet('First and Reinfections by Collection Date')
        reinfections = pd.DataFrame()
        for t in filters[0]['values']:
            wb = ws.setFilter('region', t)
            regionWs = wb.getWorksheet('First and Reinfections by Collection Date')
            df = pd.DataFrame(regionWs.data)
            df = df.rename(columns={'SUM(Number of Rows (Aggregated))-value' : 'infections', 'collectdate-value' : 'date', 'casetype (group)-alias' : 'type'})
            # df['date'] = pd.to_datetime(df['date']).dt.strftime('%m/%d/%Y')
            df = pd.pivot(df, index='date', columns='type', values = 'infections').rename(columns={'First Infections' : 'First Infections - '+t, 'Reinfections' : 'Reinfections - '+t})
            reinfections = pd.concat([reinfections, df[['First Infections - '+t, 'Reinfections - '+t]]], axis=1)
        reinfections = reinfections.transpose().reset_index()
        r = reinfections['type'].str.split(' - ',expand=True).rename(columns={0:'Category', 1:'Geography'})
        reinfections = pd.concat([r[['Geography', 'Category']], reinfections], axis=1)
        reinfections['Geography'] = 'Region '+reinfections['Geography']
        reinfections = reinfections.drop('type',axis=1).fillna(0)
        reinfections.to_csv(f'{module_path}/data/reinfections.csv')
    except Exception as e:
        logger.error('Failed to download variant info')
        logger.exception('Function tableau_hosp failed with exception')
        logger.error(str(e))

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
        capacity_file = csv_loader(f'{module_path}/data/capacity.csv', update_date_string)
        capacity_file.merge(capacity[['LDH Region', 'Category', update_date_string]], on=['Category', 'LDH Region'],
                            how='outer').to_csv(f'{module_path}/data/capacity.csv', index=False)
        logger.info('COMPLETE: Capacity')
    except Exception as e:
        logger.error('Failed to download capacity data')
        logger.exception('Function capacity failed with exception')
        logger.error(str(e))
        sys.exit(1)

def recovered(cases_deaths_primary):
    try:
        recovered_file = pd.read_csv(f'{module_path}/data/recovered.csv')
        recovered_file[update_date_string] = \
        cases_deaths_primary[cases_deaths_primary['Measure'] == 'Presumed Recovered']['Value'].values[0]
        recovered_file.to_csv(f'{module_path}/data/recovered.csv', index=False)
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
        df.sort_values(by=['Parish', 'Category']).to_csv(f'{module_path}/data/cases_tests_dot.csv')
        logger.info('COMPLETE: Date of Test')
    except Exception as e:
        logger.error('Failed to download date of test data')
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
        df.sort_values(by=['Tract', 'Category']).to_csv(f'{module_path}/data/cases_tests_tracts.csv')

        tracts = download(needed_datasets['tracts'])
        tracts = tracts.rename(columns={'GEOID': 'FIPS', 'CaseCount': update_date_string})
        tracts_file = csv_loader(f'{module_path}/data/tracts.csv', update_date_string)
        tracts_file.merge(tracts[['FIPS', update_date_string]],
                          on='FIPS',
                          how='outer').to_csv(f'{module_path}/data/tracts.csv', index=False)
        logger.info('COMPLETE: Tracts')
    except Exception as e:
        logger.error('Failed to download tract data')
        logger.exception('Function tracts failed with exception')
        logger.error(str(e))
        sys.exit(1)

def vaccine_tracts():
    try:
        vaccine_tracts = download(needed_datasets['vaccine_tract'])
        vaccine_tracts['TractID'] = vaccine_tracts['TractID'].astype(str)
        vaccine_tracts = pd.melt(vaccine_tracts, id_vars=['TractID'], value_vars=['SeriesInt', 'SeriesComp'])
        vaccine_tracts = vaccine_tracts.rename(columns = {'variable' : 'Category', 'value' : update_date_string})
        vaccine_tracts_file = csv_loader(f'{module_path}/data/vaccine_tracts.csv', update_date_string)
        vaccine_tracts_file['TractID'] = vaccine_tracts_file['TractID'].astype(str)
        (vaccine_tracts_file
         .merge(
             vaccine_tracts,
                 on=['TractID', 'Category'],
                 how='outer').to_csv(f'{module_path}/data/vaccine_tracts.csv', index=False))
        logger.info('COMPLETE: Vaccine Tracts')
    except Exception as e:
        logger.error('FAILED: Vaccine Tracts')
        logger.exception('Vaccine Tracts failed with exception')
        logger.error(str(e))
        sys.exit(1)

def vaccinations():
    try:
        vaccines = download(needed_datasets['vaccine_primary'])
        vaccines['Category'] = vaccines['Measure']
        vaccines_primary = vaccines[vaccines['ValueType'] == 'count'].copy()
        vaccines_primary['Group_'] = vaccines_primary['Group_'].replace('N/A', 'State')
        vaccines_primary = vaccines_primary[['Group_', 'Category', 'Value']].rename(
            columns={'Value': update_date_string, 'Group_': 'Geography'})
        vaccines_parish = download(needed_datasets['vaccine_parish'])
        vaccines_parish['Geography'] = vaccines_parish['Parish']
        vaccines_parish_init = vaccines_parish[['Geography', 'SeriesInt']].copy()
        vaccines_parish_init['Category'] = 'Parish - Series Initiated'
        vaccines_parish_init = vaccines_parish_init.rename(columns={'SeriesInt': update_date_string})
        vaccines_parish_comp = vaccines_parish[['Geography', 'SeriesComp']].copy()
        vaccines_parish_comp['Category'] = 'Parish - Series Completed'
        vaccines_parish_comp = vaccines_parish_comp.rename(columns={'SeriesComp': update_date_string})
        vaccines_file = csv_loader(f'{module_path}/data/vaccines.csv', update_date_string)
        (vaccines_file
         .merge(
            vaccines_primary
                .append(vaccines_parish_init)
                .append(vaccines_parish_comp),
            on=['Geography', 'Category'],
            how='outer').to_csv(f'{module_path}/data/vaccines.csv', index=False))
    except Exception as e:
        logger.error('FAILED: Vaccinations')
        logger.exception('Vaccinations failed with exception')
        logger.error(str(e))
        sys.exit(1)
    try:
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
        demo_replace = vaccines_demo[~vaccines_demo.Category.str.contains("Pct")]['Category'].to_list()
        demo_replace_dict = {}
        for d in demo_replace:
                demo_replace_dict[d] = d.split(':')[0]+" (Pct) : "+d.split(':')[1]
        vaccines_demo['Category'] = vaccines_demo['Category'].replace(demo_replace_dict)

        offset=0
        record_count = 2000

        combined = pd.DataFrame()
        while record_count == 2000:
            batch_records = pd.DataFrame(esri_cleaner(url_prefix + 'Louisiana_COVID_Vaccination_Demographics' + url_suffix + f'&resultOffset={offset}'))
            combined = combined.append(batch_records)
            offset = len(batch_records)
            record_count = len(batch_records)
        combined['area'] = combined['area'].replace({"_Region 4" : "LDH Region 4",
                                                     "_Region 5" : "LDH Region 5",
                                                     "_Region 6" : "LDH Region 6",
                                                     "_Region 7" : "LDH Region 7",
                                                     "_Region 8" : "LDH Region 8",
                                                     "_Region 9" : "LDH Region 9",
                                                     "_Louisiana" : "Louisiana",
                                                     "_Region 1" : "LDH Region 1",
                                                     "_Region 2" : "LDH Region 2",
                                                     "_Region 3" : "LDH Region 3"})
        combined_pivot = pd.pivot(combined, index='area', columns=['value_type', 'measure'], values='value')
        combined_pivot['Incomplete'] = combined_pivot['Complete']+combined_pivot['Incomplete']
        combined_pivot['Unvaccinated'] = combined_pivot['Incomplete']+combined_pivot['Unvaccinated']
        combined_pivot= combined_pivot.reset_index()
        combined_melt = pd.melt(combined_pivot, id_vars='area')

        # Clean up this mess
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Aged 0-4'), 'Category'] = 'Age - Series Complete : 0 to 4 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Aged 5-17'), 'Category'] = 'Age - Series Complete : 5 to 17 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Aged 18-29'), 'Category'] = 'Age - Series Complete : 18 to 29 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Aged 30-39'), 'Category'] = 'Age - Series Complete : 30 to 39 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Aged 40-49'), 'Category'] = 'Age - Series Complete : 40 to 49 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Aged 50-59'), 'Category'] = 'Age - Series Complete : 50 to 59 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Aged 60-69'), 'Category'] = 'Age - Series Complete : 60 to 69 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Aged 70 plus'), 'Category'] = 'Age - Series Complete : 70+ Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Age Unknown'), 'Category'] = 'Age - Series Complete : Unknown'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'White'), 'Category'] = 'Race - Series Complete : White'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Black'), 'Category'] = 'Race - Series Complete : Black'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Other Race'), 'Category'] = 'Race - Series Complete : Other'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Unknown Race'), 'Category'] = 'Race - Series Complete : Unknown Race'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Female'), 'Category'] = 'Sex - Series Complete : Female'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Male'), 'Category'] = 'Sex - Series Complete : Male'
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Gender Unknown'), 'Category'] = 'Sex - Series Complete : Gender Unknown'
        # LDH has a typo spelling Gender Unknown as Gender Unkown. Leave both versions in until they fix it.
        combined_melt.loc[(combined_melt['value_type'] == 'Complete') & (combined_melt['measure'] == 'Gender Unkown'), 'Category'] = 'Sex - Series Complete : Gender Unknown'

        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Aged 0-4'), 'Category'] = 'Age - Series Initiated : 0 to 4 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Aged 5-17'), 'Category'] = 'Age - Series Initiated : 5 to 17 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Aged 18-29'), 'Category'] = 'Age - Series Initiated : 18 to 29 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Aged 30-39'), 'Category'] = 'Age - Series Initiated : 30 to 39 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Aged 40-49'), 'Category'] = 'Age - Series Initiated : 40 to 49 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Aged 50-59'), 'Category'] = 'Age - Series Initiated : 50 to 59 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Aged 60-69'), 'Category'] = 'Age - Series Initiated : 60 to 69 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Aged 70 plus'), 'Category'] = 'Age - Series Initiated : 70+ Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Age Unknown'), 'Category'] = 'Age - Series Initiated : Unknown'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'White'), 'Category'] = 'Race - Series Initiated : White'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Black'), 'Category'] = 'Race - Series Initiated : Black'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Other Race'), 'Category'] = 'Race - Series Initiated : Other'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Unknown Race'), 'Category'] = 'Race - Series Initiated : Unknown Race'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Female'), 'Category'] = 'Sex - Series Initiated : Female'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Male'), 'Category'] = 'Sex - Series Initiated : Male'
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Gender Unknown'), 'Category'] = 'Sex - Series Initiated : Gender Unknown'
        # LDH has a typo spelling Gender Unknown as Gender Unkown. Leave both versions in until they fix it.
        combined_melt.loc[(combined_melt['value_type'] == 'Incomplete') & (combined_melt['measure'] == 'Gender Unkown'), 'Category'] = 'Race - Series Complete : Gender Unknown'

        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Aged 0-4'), 'Category'] = 'Age - Total Population : 0 to 4 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Aged 5-17'), 'Category'] = 'Age - Total Population : 5 to 17 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Aged 18-29'), 'Category'] = 'Age - Total Population : 18 to 29 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Aged 30-39'), 'Category'] = 'Age - Total Population : 30 to 39 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Aged 40-49'), 'Category'] = 'Age - Total Population : 40 to 49 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Aged 50-59'), 'Category'] = 'Age - Total Population : 50 to 59 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Aged 60-69'), 'Category'] = 'Age - Total Population : 60 to 69 Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Aged 70 plus'), 'Category'] = 'Age - Total Population : 70+ Years'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'White'), 'Category'] = 'Race - Total Population : White'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Black'), 'Category'] = 'Race - Total Population : Black'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Other Race'), 'Category'] = 'Race - Total Population : Other'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Female'), 'Category'] = 'Sex - Total Population : Female'
        combined_melt.loc[(combined_melt['value_type'] == 'Unvaccinated') & (combined_melt['measure'] == 'Male'), 'Category'] = 'Sex - Total Population : Male'

        combined_melt = combined_melt[combined_melt['Category'].notnull()][['area', 'Category', 'value']]
        combined_melt = combined_melt.rename(columns = {'area' : 'Geography', 'value' : update_date_string})

        vaccines_demo = vaccines_demo.append(combined_melt)
        vaccines_demo_file = csv_loader(f'{module_path}/data/vaccines_demo.csv', update_date_string)
        vaccines_demo_file.merge(vaccines_demo, on=['Geography', 'Category'], how='outer').to_csv(f'{module_path}/data/vaccines_demo.csv', float_format='%.10f', index=False)
        if len(vaccines_demo_file.merge(vaccines_demo, on=['Geography', 'Category'], how='outer')) > 6560:
            logger.error('Vaccines Demo File Has Too Many Records')
        logger.info('COMPLETE: Vaccinations')
    except Exception as e:
        logger.error('FAILED: Vaccination demographics')
        logger.exception('Vaccination demographics failed with exception')
        logger.error(str(e))
        sys.exit(1)

def case_death_race():
    try:
        cases_race_parish = download(needed_datasets['cases_deaths_parish'])
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
        cases_race_parish_file = csv_loader(f'{module_path}/data/cases_deaths_by_race_parish.csv', update_date_string)
        cases_race_parish_file.merge(cases_race_parish[['FIPS', 'Race', update_date_string]],
                                     on=['FIPS', 'Race'],
                                     how='outer').to_csv(f'{module_path}/data/cases_deaths_by_race_parish.csv', index=False)
        logger.info('COMPLETE: Cases and deaths by race and parish')

        cases_deaths_race_region = download(needed_datasets['cases_deaths_region'])
        cases_deaths_race_region = (
            pd.melt(cases_deaths_race_region, id_vars=['LDH_Region', 'Race'], value_vars=['Deaths', 'Cases'])).sort_values(
            by='LDH_Region')
        cases_deaths_race_region = cases_deaths_race_region.rename(columns={'value': update_date_string})
        cases_deaths_race_region_file = csv_loader(f'{module_path}/data/cases_deaths_by_race_region.csv', update_date_string)
        cases_deaths_race_region_file.merge(
            cases_deaths_race_region[['LDH_Region', 'Race', 'variable', update_date_string]],
            on=['LDH_Region', 'Race', 'variable'],
            how='outer').to_csv(f'{module_path}/data/cases_deaths_by_race_region.csv', index=False)
        logger.info('COMPLETE: Cases and deaths by race and region')
    except Exception as e:
        logger.error('Failed to case and death by parish and region data')
        logger.exception('Function case_death_race failed with exception')
        logger.error(str(e))
        sys.exit(1)

def data_download(update_date):
    try:
        vaccinations()
        vaccine_tracts()
        cases_deaths_primary = download(needed_datasets['cases_deaths_primary'])
        cases_deaths(cases_deaths_primary)
        tests(cases_deaths_primary)
        demos(cases_deaths_primary)
#        timelines(cases_deaths_primary)
        capacity(cases_deaths_primary)
        tableau_hosp()
#        recovered(cases_deaths_primary)
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
        static_data["prior_datasets"] = current_ldh_datasets
        with open(f"{module_path}/static_data.json", "w") as outfile:
            json.dump(static_data, outfile)
    except Exception as e:
        logger.exception('Function main failed with exception')
        logger.error(str(e))

        sys.exit(1)

if __name__ == "__main__":
    main()
