# Cleaning MCS Data

## Installation data

The code for cleaning MCS installation data is found in **process_mcs_installations.py**. The function **get_processed_mcs_data** does the following:

- Fetches raw MCS installation data
- Removes old records (i.e. groups records by address and installation date, then only keeps the record with maximum **version_number**)
- Adds useful columns: splits **products** into **product_id**, **product_name**, **manufacturer**, **flow_temp** and **scop**, and adds **rhi** and **year** columns
- Replaces any clearly incorrect values of cost, flow temperature or SCOP with NA
- Flags installations that occurred as part of a "cluster" of installations in the same postcode within a certain time interval
- Cleans installer names
- Returns the cleaned installation data

## Installer data

...

## Joining MCS and EPC data

**mcs_epc_joining.py** contains code for joining MCS installations to domestic EPC data. The function **join_mcs_epc_data** takes MCS installation data, attempts to match the address with an address in the domestic EPC register, and if successful adds relevant fields from the EPC to the installation records (if **all_records** is set to True then a row will be added for each matching EPC).

The matching is performed by initially looking for an exact match between postcodes and "numeric tokens" (which are defined to be any character string containing numbers, such as "42" or "3a") in the MCS and EPC addresses. The full addresses of records with a match are then compared using the Jaro-Winkler method, which measures the similarity between two strings, prioritising characters near the start of the string. (This method was chosen because key information such as the house name is likely to be near the start of the address string, and as to not excessively punish the inclusion of extra information in one address string that does not appear in another such as town or county.) Records that exceed a certain similarity threshold are considered a match and the data is joined.

The function **select_most_relevant_epc** filters a "fully merged" MCS-EPC dataset (i.e. one in which each MCS record is linked to multiple EPC records) to the EPC that is assumed to best reflect the status of the property at the time of the heat pump installation. For each recorded installation, if an EPC record predating the heat pump installation exists then the latest such record is chosen; otherwise, the earliest EPC record is chosen.
