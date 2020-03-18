# Tracking COVID-19 in Louisiana

This script accesses the ArcGIS Rest endpoints of the dashboard used by the Louisiana Department of Health to track the coronavirus/COVID-19 pandemic
and add the latest data to csv files to preserve this data for time-series analysis.

As of this update, LDH provides the following data:
Cases (Parish-level)
Deaths (Parish-level)
Tests (Statewide)
Age groups of those who tested positve (Statewide)

LDH is currently updating their dashboard twice a day, at 9:30 a.m. and 5:30 p.m. This script should be run after the last update of the day to capture the final tallies for each day. If it is run multiple 
times per day, it will overwrite any previous data for the day with the updated data.
