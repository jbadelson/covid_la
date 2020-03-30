# Tracking COVID-19 in Louisiana

This script accesses the ArcGIS Rest endpoints of the dashboard used by the Louisiana Department of Health to track the coronavirus/COVID-19 pandemic and add the latest data to csv files to preserve this data for time-series analysis.

As of this update, LDH provides the following data:<br>
* Cases (Parish-level) in cases.csv
* Deaths (Parish-level) in deaths.csv
* Tests conducted by public and private labs (Statewide) in tests.csv (Note: testing information prior to 3/9/2020 is based on public statements from state officials)
* Age groups, median age, age range and sex of those who tested positve (Statewide) in case_demo.csv
* Age groups of those who died (Statewide) in death_demo.csv

Null fields in any file represent days in which that information was not made available.

LDH is currently updating their dashboard once a day at noon. This script should be run after the update to capture the data for each day. If it is run multiple times per day, it will overwrite any previous data for the day with the updated data.

My goal is to update the CSV files in this repository will be every day.
