# Cleaning Supplementary Data

This directory contains the code needed to clean supplementary data regularly used in analysis of EPC and MCS data.

The **three** key datasets identified as supplementary include:

1. **Geographic data:** UK postcodes to latitude and longitudes. This is helpful for geocoding addresses without calling any APIs. Data comes from [FreeMapTools](https://www.freemaptools.com/download-uk-postcode-lat-lng.htm).
2. **Population data:** Total populations per nuts2 region in Europe. This is helpful for normalising results by population. Data comes from [Eurostat](https://appsso.eurostat.ec.europa.eu/nui/show.do?dataset=demo_r_d2jan&lang=en).
3. **Income data:** Index of Multiple Deprivation data. This is helpful if it is also important to consider income as a feature. Data comes from [Consumer Data Research Center](https://data.cdrc.ac.uk/dataset/index-multiple-deprivation-imd).

### Running supplementary data preprocessing scripts

Currently, the only dataset that requires some preprocessing is the **population data**. The data is preprocessed by:

- Clean text;
- Determine data types;
- Identify UK NUTS2 regions;
- Merge with NUTS2 data;
- Choose correct West Midlands population;
- Save cleaned data to s3.

To clean the raw data, run `python process_supplementary_data.py` in your activated conda enviornment.

### To Dos

1. Add IMD preprocessing when new data is released
2. Determined the cadence for when to update these sources
