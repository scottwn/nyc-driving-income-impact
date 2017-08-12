# nyc-driving-income-impact

This was a Big Data project to find links between income and people who walk or bike to work and are victims of traffic crashes in New York City. See `paper.pdf` in this repo for a detailed description and results.

We used three public data sources:
https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95
https://nycopendata.socrata.com/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9
https://www2.census.gov/programs-surveys/acs/data/pums/2015/5-Year/csv_pny.zip

Java code for cleaning this data using MapReduce is found in `src/mapreduce/`

Other code in src is Python code for cleaning data or Hive scripts for doing analytics.

`ST_Geometry_UDF.q` `puma_to_hive.py` and `split_latlong.q`: This was an attempt to use a [UDF provided by ESRI](https://github.com/Esri/spatial-framework-for-hadoop) to put lat/long keys in the crash and complaint data inside the [PUMA shapes](https://www.census.gov/geo/reference/puma.html). We weren't able to do this successfully, contributions would be helpful! Our workaround was instead using a [zip code to PUMA crosswalk](https://www.baruch.cuny.edu/confluence/display/geoportal/NYC+Geographies).

`analytics.py`: This file uses PySpark to perform statistical analysis and facilitate exploration of the data. This script first reads a hive table and then performs transformations on it to calculate some statistics of the data. Along with displaying it in a format so that it makes analysis easier.

`analytics.q`: This Hive script does some simple analysis on the analytic table like sorting, finding the mean of the three data, and finding the community district where has high commuters/crashes/complaints.

`create_data_tables.q`: This file has a set of queries which create the database and tables for analytics.
