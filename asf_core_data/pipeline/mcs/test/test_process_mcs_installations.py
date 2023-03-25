"""
Script to test the data processing pipeline for MCS installations data.
"""

import pytest
import re
import pandas as pd
from asf_core_data.pipeline.mcs.process.process_mcs_installations import (
    get_processed_installations_data,
)
from asf_core_data.getters.mcs_getters.get_mcs_installations import (
    get_most_recent_raw_historical_installations_data,
)
from asf_core_data.pipeline.mcs.process.process_mcs_utils import (
    drop_instances_test_accounts,
)

from asf_core_data.config import base_config

processed_installations = get_processed_installations_data()
raw_installations = get_most_recent_raw_historical_installations_data()


def test_raw_columns_exist():
    """
    Checks if raw columns are as expected.
    """

    expected_cols = base_config.historical_installations_rename_cols_dict.keys()

    existing_cols = raw_installations.columns

    for col in expected_cols:
        if col in ["Installation Type", " Installation Type"]:
            assert ("Installation Type" in existing_cols) or (
                " Installation Type" in existing_cols
            )
        else:
            assert col in existing_cols

    for col in existing_cols:
        assert col in expected_cols


def test_company_unique_id_is_added():
    """
    Test whether the company unique ID is successfully added to the installations table
    and that there are no missing values.
    """
    assert "company_unique_id" in processed_installations.columns

    assert (
        len(
            processed_installations[
                pd.isnull(processed_installations["company_unique_id"])
            ]
        )
        == 0
    )


def test_regex_extraction_works():
    """
    Check if regex in add_hp_features() is working.
    """

    product_regex_dict = {
        "product_id": "MCS Product Number: ([^|]+)",
        "product_name": "Product Name: ([^|]+)",
        "manufacturer": "License Holder: ([^|]+)",
        "flow_temp": "Flow Temp: ([^|]+)",
        "scop": "SCOP: ([^)]+)",
    }
    product = "(ID: 1234 | MCS Product Number: abc 1-7 xx | Product Name: + 1-7 xx | License Holder: ABC XYZ, S.L. | Flow Temp: 50 | SCOP: 3.91)"
    results_dict = dict()
    for product_feat, regex in product_regex_dict.items():
        results_dict[product_feat] = re.search(regex, product).group(1).strip()

    assert results_dict == {
        "product_id": "abc 1-7 xx",
        "product_name": "+ 1-7 xx",
        "manufacturer": "ABC XYZ, S.L.",
        "flow_temp": "50",
        "scop": "3.91",
    }


def test_same_lines_before_after():
    """
    Checks if raw and processed datasets have the same number of lines
    (discounting the lines corresponding to test accounts and duplicate lines).
    """

    assert len(
        drop_instances_test_accounts(
            raw_installations, "Installation Company Name"
        ).drop_duplicates()
    ) == len(processed_installations)


def test_no_duplicate_lines():
    """
    Checks that there are no duplicate lines in the processed dataset.
    """
    assert len(processed_installations.drop_duplicates()) == len(
        processed_installations
    )
