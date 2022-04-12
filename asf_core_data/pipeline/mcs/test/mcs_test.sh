#!/bin/bash

python gdrive_to_s3.py
latest_outputs=$(aws s3api list-objects --bucket asf-core-data --prefix inputs/MCS/latest_raw_data/ --output text --query "Contents[].{Key: Key}")
latest_outputs_array=($latest_outputs)
new_installations="${latest_outputs_array[1]}"
new_installers="${latest_outputs_array[2]}"
python compare_mcs_installations.py --new_installations_df "${new_installations}" --new_installers_df "${new_installers}"
