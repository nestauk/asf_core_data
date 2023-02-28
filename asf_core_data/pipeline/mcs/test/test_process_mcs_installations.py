"""
Script to test the data processing pipeline for MCS installations data.
"""

import pytest
import pandas as pd
from asf_core_data.pipeline.mcs.process.process_mcs_installations import (
    get_processed_installations_data,
)


def test_company_unique_id_is_added():
    """
    Test whether the company unique ID is successfully added to the installations table
    and that there are no missing values.
    """
    data = get_processed_installations_data(refresh=True)

    assert "company_unique_id" in data.columns

    assert len(data[pd.isnull(data["company_unique_id"])]) == 0
