# Urban Analytics — Educational Accessibility, Spatial Inequality, and Social Mobility
This repository is a collection of documents for the Group Project for the CASA module on Urban Data Visualisation. 

## Theme: Urban Analytics 
This project applies network-based accessibility modelling to examine how access to secondary schools, particularly high-performing schools varies across neighbourhoods in London.
It integrates transport network modelling with school performance data and socioeconomic indicators to explore whether spatial accessibility reinforces educational inequality.

## Core Questions

Main RQ: To what extent does the geographic distribution of high-performing secondary schools in London reflect and reinforce patterns of socio-economic inequality across neighbourhoods?

RQ 1 — Spatial Concentration:
Are high-performing secondary schools (by Ofsted rating, Attainment 8 and Progress 8) disproportionately concentrated in particular boroughs or sub-regions of London, and does this pattern correlate with neighbourhood deprivation (IMD)?

RQ 2 — Intake vs. Neighbourhood:
Do high-performing secondary schools serve socio-economically different populations from the neighbourhoods in which they are located, as measured by FSM rates, % EAL and IMD decile

RQ 3 — Accessibility (Equity):
How does travel time to the nearest high-performing secondary school vary across London's sub-regions, and is longer travel time systematically associated with higher neighbourhood deprivation?

## Data Parsing 
- Step 1: Download edubasealldata20260307.csv into large folder from https://get-information-schools.service.gov.uk/Downloads (Click all establishment fields 
- Step 2: Run compress.py to convert the csv to parquet in the data folder. 
- Step 3: Run build_schools_database.py to pull and compile the files together 
