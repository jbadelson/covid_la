# Tracking COVID-19 in Louisiana

## Update (March 14, 2021):
We're a year into the pandemic and it's time for a bit of an update to this repository. Keeping up with LDH's data and various other work has meant I haven't ever gotten around to fully documenting this code and the script itself is full of kludges that I've been meaning to fix for some time.

At the moment, I'm doing a full rewrite of this script and considering various approaches for storing the data in the repository for ease of use. 

I don't know if anyone is still using the data contained in this repository. If you are, please get in touch with me at jadelson@theadvocate.com to ensure that I do not make any changes that will cause problems for your application.

At this point I intend to update the scripts and data on Sunday, March 21, 2021. 

## Old description

This script accesses the ArcGIS Rest endpoints of the dashboard used by the Louisiana Department of Health to track the coronavirus/COVID-19 pandemic and add the latest data to csv files to preserve this data for time-series analysis.

There are two important notes about the current data:

On May 22, LDH switched to a new schema for its data that required a rewrite of the script used to fetch the data. I am still double-checking this data to ensure it was downloaded correctly and attached to the correct files.

It is also important to note that the update includes data on Onset of Symptoms and Date of Death that is more recent than the information publicly displayed on the LDH dashboard. Data for either of those categories should be considered incomplete until at least 8 days have passed.

Given the amount of data LDH is producing and the variations between the different datasets, I intend to create data dictionaries for each of the files contained in this repository. Until then, here's a brief description of each file: <br>
* Cases (Parish-level) in cases.csv
* Deaths (Parish-level) reported to the state in deaths.csv
* Tests conducted by public and private labs (Statewide) in tests.csv (Note: testing information prior to 3/9/2020 is based on public statements from state officials.)
* Public and private tests by parish in test_details.csv (Parish-level)
* Age groups, median age, age range and sex of those who tested positve (Statewide) in case_demo.csv
* Age groups of those who died (Statewide) in death_demo.csv
* Number of hospital beds, ICUs and ventilators available, in use and total in each LDH Region (http://ldh.la.gov/index.cfm/page/2) in capacity.csv. (Note: Prior to LDH adding these statistics to their dashboard on 4/2/2020, capacity figures were reported somewhat sporatically. Data for previous days has been reconstructed as well as possible but may be missing data or include data that is not consistent with how LDH is currently counting resources.)
* Number of patients confirmed positive for COVID-19 hospitalized and on ventilators (Statewide) in hospitalizations.csv. (Note: This file will also include PUI - Patient Under Investigation - information on days it is made available).
* Data on the date patients who tested positive reported their first symptoms started and the date deaths actually occurred, as opposed to the date they were reported to the state (Statewide) in symptoms_date_of_death.csv. The state does not track the actual date a patient reports onset of symptoms. Instead, symtoms are considered to have first appeared four days before a patient tests positive. Therefore, the cases data in this file can be shifted forward four days to determine the date of the test. (Note: the state adds an additional days' worth of data each day but also may adjust previous days' data.).
* Number of cases by Census tract in tracts.csv. (Note: The first day of data was provided as raw counts but LDH has since switched to providing ranges. This data is expected to be updated weekly).
* Number of deaths by race and parish for parishes with more than 25 deaths in deaths_by_race_parish.csv (Note: Each parish/race combination is a single row. This data is expected to be updated weekly).
* Number of deaths by race and parish by LDH Region in deaths_by_race_region.csv (Note: Each region/race combination is a single row. This data is expected to be updated weekly).

Null fields in any file represent days in which that information was not made available.

LDH is currently updating their dashboard once a day at noon. This script should be run after the update to capture the data for each day. If it is run multiple times per day, it will overwrite any previous data for the day with the updated data.

My goal is to update the CSV files in this repository every day, as soon after the noon update as possible. This will often be within minutes but may take longer if LDH updates their schema, any problems are detected in the data or other work takes priority.

If you have questions about this data or find it useful, please email Jeff Adelson at jadelson@theadvocate.com.

Data can be attributed to: Louisiana Department of Health data compiled by Jeff Adelson, The Times-Picayune | The New Orleans Advocate or simply to Louisiana Department of Health data compiled by The Times-Picayune | The New Orleans Advocate.
