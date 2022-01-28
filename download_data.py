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


needed_datasets = {'cases_deaths_primary' : 'test_this_sheet',  # Main LDH cases, deaths and test data
                   'cases_deaths_parish' : 'Cases_and_Deaths_by_Race_by_Parish',
                   'cases_deaths_region' : 'Cases_and_Deaths_by_Region_by_Race',
                   'vaccine_primary' : 'Louisiana_COVID_Vaccination_Information___for_checking',
                   'vaccine_parish' : 'Vaccinations_by_Race_by_Parish',
                   'vaccine_tract': 'Louisiana_Vaccinations_by_Tract',
                   'vaccine_full_demo' : 'Louisiana_Vaccination_Full_Demographics',
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
        offset = len(batch_records)
        record_count = len(batch_records)
    return combined

def download_all():
    datasets = get_datasets()
    for d in datasets:
        try:
            if d != 'Blocks_2010_Pop_Only_Comprehensive_Coastline' and d != 'Census_Block_Groups_2010_Comprehensive_Coastline' and d != 'Census_Tracts_2010_Comprehensive_Coastline':
                print(d)
                df = pd.DataFrame(download(d))
                df.to_csv(f"{module_path}/data/full_datasets/{d}{datetime.now().strftime('%Y-%m-%d')}.csv")
        except:
            pass

def main():
    try:
        download_all()
    except Exception as e:
        logger.exception('Downloading all datasests failed.')
        logger.error(str(e))

        sys.exit(1)

if __name__ == "__main__":
    main()

