# Tracking COVID-19 in Louisiana

# Note: Louisiana now provides parish-level data on public and private tests. This information will be stored in test_details.csv until April 2, 2020, at which point all data will be saved to tests.csv. In order to distinguish between public and private tests, each parish will have two rows for each day indicating the total number of private tests and total number of public tests completed at that point. Be aware that this change may break applications that depend on the tests.csv file. 

This script accesses the ArcGIS Rest endpoints of the dashboard used by the Louisiana Department of Health to track the coronavirus/COVID-19 pandemic and add the latest data to csv files to preserve this data for time-series analysis.

As of this update, LDH provides the following data:<br>
* Cases (Parish-level) in cases.csv
* Deaths (Parish-level) in deaths.csv
* Tests conducted by public and private labs (Statewide) in tests.csv (Note: testing information prior to 3/9/2020 is based on public statements from state officials. This file will be merged with test_details.csv on April 2, 2020. See note at top.)
* Public and private tests by parish in test_details.csv (Parish-level)
* Age groups, median age, age range and sex of those who tested positve (Statewide) in case_demo.csv
* Age groups of those who died (Statewide) in death_demo.csv

Null fields in any file represent days in which that information was not made available.

LDH is currently updating their dashboard once a day at noon. This script should be run after the update to capture the data for each day. If it is run multiple times per day, it will overwrite any previous data for the day with the updated data.

My goal is to update the CSV files in this repository every day, as soon after the noon update as possible. This will often be within minutes but may take longer if LDH updates their schema, any problems are detected in the data or other work takes priority.

If you have questions about this data or find it useful, please email Jeff Adelson at jadelson@theadvocate.com.

Data can be attributed to Jeff Adelson, The Times-Picayune | The New Orleans Advocate or simply to The Times-Picayune | The New Orleans Advocate.
