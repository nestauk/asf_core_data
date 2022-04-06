# MCS: within dataset and between dataset testing

Welcome to the testing pipeline of MCS installations and installers data :nerd_face:!

#### Set Up

To be able to pull data from google drive and upload to s3 automatically, you need to download the `client_secrets.json` from `s3://asf-core-data/inputs/MCS/latest_raw_data/` to this folder.

There are permissions to [Julia Suter](mailto:julia.suter@nesta.org.uk), [Chris Williamson](mailto:chris.williamson@nesta.org.uk) and [India Kerle](india.kerle@nesta.org.uk) for the Google `hp data dumps` Application. No one else will be able to approve the use of downloading the data via the API.

#### Steps

Once you have downloaded `client_secrets.json`, you will be able to run the bash script to:

1. Pull the latest installers and installations data from google drive (latest is based on createdDate), save data locally then upload to S3. the data name is important: installations data **must** have the phrase **installations** in the title while installer data **must** have the phrase **installer** in the title.
2. Run data comparisons between the newest installations data and an older installations dataset.
3. Run within dataset testing of the latest installer data.
4. Output testing information to a timestamped `.txt` file in folder.

To run bash script in activated conda environment, `bash mcs_test.sh`.

#### To Dos

1. Within testing of latest installations data;
2. Between dataset testing of latest installer data and previous installer data.
