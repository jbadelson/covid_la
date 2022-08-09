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
import time

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_date_fmt = "%Y-%m-%d %H:%M:%S"

# stdout_handler = logging.StreamHandler(sys.stdout)
# stdout_handler.setLevel(logging.DEBUG)
# stdout_log_format = logging.Formatter('[%(asctime)s] {%(filename)s} %(levelname)s - %(message)s', log_date_fmt)
# stdout_handler.setFormatter(stdout_log_format)
# logger.addHandler(stdout_handler)

#with open(f'{module_path}/static_data.json') as f:
#    static_data = json.load(f)

update_date = datetime.now()
#update_date = pd.to_datetime('2022-07-27')

if os.name == 'nt':
    update_date_string = update_date.strftime('%#m/%#d/%#Y')
else:
    update_date_string = update_date.strftime('%-m/%-d/%Y')

def retry(times):
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :type times: Int
    """
    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except:
                    logger.error(
                        'Exception thrown when attempting to run %s, attempt '
                        '%d of %d' % (func, attempt, times)
                    )
                    attempt += 1
                    time.sleep(5)
            return func(*args, **kwargs)
        return newfn
    return decorator

def csv_loader(file, date):
    df = pd.read_csv(file, dtype={'FIPS': object})
    if date in df.columns:
        df = df.drop(columns=date)
    return df

@retry(times=5)
def cases():
    logger.info('STARTING: Parish case data by type.')
    # load data from tableau workbook
    url='https://analytics.la.gov/t/LDH/views/CasesChartsforDashboard/NewandPreviousCasesbyDate'
    ts = TS()
    ts.loads(url)
    workbook = ts.getWorkbook()
    # tabular data on total cases is in the Table: New and Previous Cases chart 
    # in the Cases by Test Date tab of Covid-19 Cases by Test Collection Date
    workbook = workbook.setParameter("New and Previous Cases Chart Selection", "Table: Cases by Type by Parish")
    cases = pd.DataFrame(workbook.getWorksheet('Parish Cases List (2)').data)
    cases = cases.rename(columns={'Parish-value' : 'County', 'SUM(Cases)-alias' : update_date_string})
    case_types = {'%all%' : 'cases_total', 
                    'Confirmed' : 'cases', 
                    'Probable' : 'cases_probable', 
                    'Reinfections' : 'cases_reinfections'}

    for category in case_types.keys():
        df = cases[(cases['casetype-value'] == category) & (cases['County']!='%all%')][['County', update_date_string]]
        # Summing all parish data because on launch West Feliciana had multiple rows
        df = df.groupby('County').agg({update_date_string : 'sum'}).reset_index()
        df_file = csv_loader(f"{module_path}/data/{case_types[category]}.csv", update_date_string)
        df_file.merge(df, on='County', how='outer').to_csv(f"{module_path}/data/{case_types[category]}.csv", index=False)
    logger.info('COMPLETE: Parish case data by type downloaded and stored.')

@retry(times=5)
def deaths():
    logger.info('STARTING: Parish death data by type.')
    # load data from tableau workbook
    url='https://analytics.la.gov/t/LDH/views/URLDashboardDeaths/DeathsbyParishList'
    ts = TS()
    ts.loads(url)
    workbook = ts.getWorkbook()
    deaths = pd.DataFrame(workbook.getWorksheet('Deaths by Parish and Region').data)
    deaths = deaths.rename(columns = {'Parish-value' : 'County', 'SUM(Value)-alias' : update_date_string})

    death_types = {'Total Deaths' : 'deaths_total',
                    'Probable Deaths' : 'deaths_probable',
                    'Confirmed Deaths' : 'deaths'}
    for category in death_types.keys():
        df = deaths[(deaths['Measure-value'] == category) & (deaths['County']!='%all%')][['County', update_date_string]]
        df_file = csv_loader(f"{module_path}/data/{death_types[category]}.csv", update_date_string)
        df_file.merge(df, on='County', how='outer').to_csv(f"{module_path}/data/{death_types[category]}.csv", index=False)
    logger.info('COMPLETE: Parish death data by type downloaded and stored.')

def case_demos():
    logger.info('STARTING: Case demographics')
    ts = TS()
    url = 'https://analytics.la.gov/t/LDH/views/CasesChartsforDashboard/CasesbyAge'
    ts.loads(url)
    workbook = ts.getWorkbook()
    workbook = workbook.setParameter("Select Age Range View", "Cumulative Cases Bar Chart")
    ages = pd.DataFrame(workbook.getWorksheet('Cases by Age Cumulative').data)
    age_converter = {
        '0-4'   : '0 to 4',
        '5-17'  : '5 to 17',
        '18-29' : '18 to 29',
        '30-39' : '30 to 39',
        '40-49' : '40 to 49',
        '50-59' : '50 to 59',
        '60-69' : '60 to 69',
        '+70'   : '70+'             
                    }
    ages['Date'] = update_date_string
    ages = ages.rename(columns = {
        'Age Range-value' : 'Category',
        'Region-alias' : 'Geography',
        'SUM(Cases)-value' : 'Cases'
    })
    ages['Category'] = ages['Category'].replace(age_converter)
    age_export = pd.pivot(ages, index=['Category', 'Geography'], columns='Date', values='Cases').reset_index()
    age_export['Geography'] = age_export['Geography'].apply(lambda x: f"Region {x.split(' - ')[0]}")
    age_export_la = age_export.groupby('Category').sum().reset_index()
    age_export_la['Geography'] = 'Louisiana'
    age_export_la = age_export_la.rename(columns={'Date' : update_date_string})
    logger.info('COMPLETE: Case demographics downloaded and saved.')
    df_file = csv_loader(f"{module_path}/data/case_demo.csv", update_date_string)
    df_file.merge(age_export_la, on=['Geography', 'Category'], how='outer').to_csv(f"{module_path}/data/case_demo.csv", index=False)


@retry(times=5)
def hospitalizations():
    logger.info('STARTING: State hospitalization and ventilator data.')
    url = 'https://analytics.la.gov/t/LDH/views/URLDashboardHospitalizations/HospitalCharts'
    ts = TS()
    ts.loads(url)
    workbook = ts.getWorkbook()
    hosp_worksheet = workbook.getWorksheet('Hospital and Vent Usage')
    hosp = hosp_worksheet.data
    hosp['DateTime-value'] = pd.to_datetime(hosp['DateTime-value']).dt.strftime('%m/%d/%Y')
    hosp = hosp.rename(columns={'DateTime-value' : 'Category', 'SUM(Covid Positive in Hospital)-value' : 'hospitalized', 'SUM(Covid Positive on Vent)-alias' : 'on_vent'})
    hosp = hosp[['Category', 'hospitalized', 'on_vent']].set_index('Category').transpose().reset_index().rename(columns={'index' : 'Category'})
    hosp.to_csv(f'{module_path}/data/hospitalizations.csv', index=False)
    logger.info('COMPLETE: State hospitalization data downloaded and stored.')

@retry(times=5)
def hosp_region():
    logger.info('STARTING: Regional hospitalization and ventilator data.')
    url = 'https://analytics.la.gov/t/LDH/views/URLDashboardHospitalizations/HospitalCharts'
    ts = TS(logLevel='ERROR')
    ts.loads(url)
    workbook = ts.getWorkbook()
    sheets = workbook.getSheets()
    ws = ts.getWorksheet('Hospital and Vent Usage')
    filters = ws.getFilters()
    hosp = pd.DataFrame()
    for t in filters[0]['values']:
        if t != 'Under Investigation':
            logger.info(f'    DOWNLOADED: {t} hospitalization and ventilator data')
            wb = ws.setFilter('Region', t, dashboardFilter=True)
            regionWs = wb.getWorksheet('Hospital and Vent Usage')
            df = pd.DataFrame(regionWs.data)
            df = df.rename(columns = {'SUM(Covid Positive in Hospital)-value' : 'hospitalized - '+t, 'SUM(Covid Positive on Vent)-alias' : 'on_vent - '+t, 'DateTime-value' : 'date'})
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%m/%d/%Y')
            df = df.set_index('date')
            hosp = pd.concat([hosp, df[['hospitalized - '+t, 'on_vent - '+t]]], axis = 1)
    hosp = hosp.transpose().reset_index()
    h = hosp['index'].str.split(' - ', expand=True).rename(columns={0 : 'Category', 1 : 'Geography'})
    hosp = pd.concat([h[['Geography', 'Category']], hosp], axis=1)
    hosp['Geography'] = 'Region '+hosp['Geography']
    hosp = hosp.drop('index', axis=1)
    hosp.to_csv(f'{module_path}/data/region_hosp.csv', index=False)
    logger.info('COMPLETE: Regional hospitalization and ventilator data downloaded and stored.')

@retry(times=5)
def capacity():
    logger.info('STARTING: Hospital capacity data.')
    ts = TS()
    url = 'https://analytics.la.gov/t/LDH/views/URLDashboardHospitalizations/RegBedAvailability'
    ts.loads(url)
    workbook = ts.getWorkbook()
    worksheet = workbook.getWorksheet('Hospital Reg Bed Availability')
    beds = pd.DataFrame(worksheet.data)
    beds_tot_avail = (
        beds[beds['Bed Status-alias'] == 'Available']
        .rename(
            columns={
                'Region-alias' : 'LDH Region', 
                'SUM(Abs Diverging)-alias' : 'Hospital Beds Still Available', 
                'SUM(Bed Count)-alias' : 'Hospital Beds Total'
                }
            )
    )
    beds_in_use = (
        beds[beds['Bed Status-alias'] == 'In Use']
        .rename(
            columns = {
                'Region-alias' : 'LDH Region', 
                'SUM(Abs Diverging)-alias' : 'Hospital Beds In Use'
                }
            )
    )
    beds_all = (
        beds_tot_avail[
            [
                'LDH Region', 
                'Hospital Beds Still Available', 
                'Hospital Beds Total'
                ]
            ]
            .merge(
                beds_in_use[
                    [
                        'LDH Region', 
                        'Hospital Beds In Use'
                        ]
                    ], 
                    on="LDH Region"
                )
    )
    beds_all['LDH Region'] = beds_all['LDH Region'].apply(lambda x: f"Region {x.split(' - ')[0]}")
    beds_all = pd.melt(beds_all, id_vars = 'LDH Region', value_vars=['Hospital Beds Still Available', 'Hospital Beds In Use', 'Hospital Beds Total'])

    ts = TS()
    url = 'https://analytics.la.gov/t/LDH/views/URLDashboardHospitalizations/ICUBedAvailability'
    ts.loads(url)
    workbook = ts.getWorkbook()
    worksheet = workbook.getWorksheet('Hospital ICU Bed Availability')
    icu = pd.DataFrame(worksheet.data)
    icu_avail = (
        icu[icu['Bed Status-alias'] == 'Available']
        .rename(
            columns={
                'Region-alias' : 'LDH Region', 
                'SUM(Abs Diverging)-alias' : 'ICU Still Available', 
                'SUM(Bed Count)-alias' : 'ICU Total'
                }
            )
    )
    icu_tot_in_use = (
        icu[icu['Bed Status-alias'] == 'In Use']
        .rename(
            columns = {
                'Region-alias' : 'LDH Region', 
                'SUM(Abs Diverging)-alias' : 'ICU In Use', 
                'SUM(Bed Count)-alias' : 'ICU Total'
                }
            )
    )
    icu_all = (
        icu_avail[
            [
                'LDH Region', 
                'ICU Still Available'
                ]
            ]
            .merge(
                icu_tot_in_use[
                    [
                        'LDH Region', 
                        'ICU In Use', 
                        'ICU Total'
                        ]
                    ], 
                    on='LDH Region'
                )
    )
    icu_all['LDH Region'] = icu_all['LDH Region'].apply(lambda x: f"Region {x.split(' - ')[0]}")
    icu_all = pd.melt(icu_all, id_vars = 'LDH Region', value_vars=['ICU Still Available', 'ICU In Use', 'ICU Total'])

    cap = pd.concat([beds_all, icu_all], axis=0)
    cap = cap.rename(columns = {'variable' : 'Category', 'value' : update_date_string})
    cap_file = csv_loader(f"{module_path}/data/capacity.csv", update_date_string)
    cap_file.merge(cap, on=['LDH Region', 'Category'], how='outer').to_csv(f"{module_path}/data/capacity.csv", index=False)

    logger.info('COMPLETE: Hospital capacity data downloaded and stored.')

@retry(times=5)
def vaccines():
    logger.info('STARTING: Parish vaccine data.')
    ts = TS()
    url = 'https://analytics.la.gov/t/LDH/views/VaccinationDashboard2/VaccinationStatusbyAgeRaceGender2'
    ts.loads(url)
    workbook = ts.getWorkbook()
    worksheet = workbook.getWorksheet('Cumulative % Totals Demo Tables').data
    parishes = worksheet.groupby('Parish-value').agg({'ATTR(Completed)-alias' : 'max', 'ATTR(Initiated)-alias' : 'max'})
    parishes = parishes.reset_index()
    parishes = parishes.rename(columns = {'ATTR(Completed)-alias' : 'Series Completed', 'ATTR(Initiated)-alias' : 'Series Initiated', 'Parish-value' : 'Geography'})
    parishes = pd.melt(parishes, id_vars=['Geography'], value_vars=['Series Completed', 'Series Initiated']).rename(columns = {'variable' : 'Category', 'value' : update_date_string})
    state = parishes.groupby('Category').sum().reset_index()
    state['Geography'] = 'State'
    parishes['Category'] = parishes.apply(lambda x: f"Parish - {x['Category']}", axis=1)
    state['Category'] = state.apply(lambda x: f"Total {x['Category']}", axis=1)
    vaccines_export = pd.concat([state, parishes], axis=0)
    vaccines_file = csv_loader(f"{module_path}/data/vaccines.csv", update_date_string)
    vaccines_file.merge(vaccines_export, on=['Geography', 'Category'], how='outer').to_csv(f"{module_path}/data/vaccines.csv", index=False)
    logger.info('COMPLETE: Parish vaccine data downloaded and stored.')


@retry(times=5)
def vaccine_demos():
    logger.info('STARTING: Vaccine demographic data.')
    age_converter = {
        '0 - 4'   : '0 to 4 Years',
        '18 - 29' : '18 to 29 Years',
        '30 - 39' : '30 to 39 Years',
        '40 - 49' : '40 to 49 Years',
        '5 - 17'  : '5 to 17 Years',
        '50 - 59' : '50 to 59 Years',
        '60 - 69' : '60 to 69 Years',
        '70+'     : '70+ Years'
    }
    ts = TS(logLevel='ERROR')
    url = 'https://analytics.la.gov/t/LDH/views/VaccinationDashboard2/VaccinationStatusbyAgeRaceGender'
    df_export = pd.DataFrame()
    ts.loads(url)
    workbook = ts.getWorkbook()
    worksheet = ts.getWorksheet('Cumulative Totals by Demographics')
    print(worksheet.getFilters())
    for geography in worksheet.getFilters()[2]['values']:
        logger.info(f"    DOWNLOADING: {geography} vaccine demographics")
        area = worksheet.setFilter('area', geography)
        area = area.getWorksheet('Cumulative Totals by Demographics')
        df_temp = pd.DataFrame()
        for measure in ['Race', 'Gender', 'Age']:
            logger.info(f"        DOWNLOADING: {geography} {measure} data")
            category = area.setFilter('Measure Group', measure)
            category = category.getWorksheet('Cumulative Totals by Demographics')
            df_temp = pd.concat([df_temp, category.data], axis=0)
        df_temp['Vaccination Status-value'] = df_temp['Vaccination Status-value'].replace({'Complete' : 'Series Complete'})
        df_temp['Vaccination Status-value'] = df_temp['Vaccination Status-value'].replace({'Incomplete' : 'Series Initiated'})
        df_temp['Measure Group-alias'] = df_temp['Measure Group-alias'].str.replace('Gender', 'Sex')
        df_temp['Measure-value'] = df_temp['Measure-value'].replace(age_converter)
        df_temp['Measure-value'] = df_temp['Measure-value'].replace({'Unknown Gender' : 'Gender Unknown'})
        df_temp['SUM(Value)-alias'] = pd.to_numeric(df_temp['SUM(Value)-alias'], errors='coerce')
        df_total = df_temp.groupby(['Measure Group-alias', 'Measure-value']).agg({'SUM(Value)-alias' : 'sum'}).reset_index()
        df_total['Category'] = df_total.apply(lambda x: f"{x['Measure Group-alias']} - Total Population : {x['Measure-value']}", axis=1)
        df_temp['Category'] = df_temp.apply(lambda x: f"{x['Measure Group-alias']} - {x['Vaccination Status-value']} : {x['Measure-value']}", axis=1)
        df_temp = pd.concat([df_temp, df_total], axis=0)
        df_temp['Geography'] = geography.replace('_','')
        df_temp = df_temp.rename(columns={'SUM(Value)-alias' : update_date_string})
        df_export = pd.concat([df_export, df_temp[['Geography', 'Category', update_date_string]]])
    df_export = df_export[['Geography', 'Category', update_date_string]]
    vaccine_demo_file = csv_loader(f'{module_path}/data/vaccines_demo.csv', update_date_string)
    vaccine_demo_file.merge(df_export, on=['Geography', 'Category'], how='outer').to_csv(f"{module_path}/data/vaccines_demo.csv", index=False)
    logger.info("COMPLETE: Vaccine demographic data downloaded and stored")
    
def main():
    cases()
    case_demos()
    deaths()
    hospitalizations()
    hosp_region()
    capacity()
    vaccines()
    vaccine_demos()

if __name__ == "__main__":
    main()