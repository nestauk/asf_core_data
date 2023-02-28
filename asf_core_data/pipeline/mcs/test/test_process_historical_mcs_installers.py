"""
Script to test the data processing pipeline for raw historical MCS installers data.
"""

import pytest
import pandas as pd
from datetime import datetime
import os
from asf_core_data.pipeline.mcs.process.process_historical_mcs_installers import (
    rename_columns,
    deal_with_versions_of_trading_as,
    match_companies_house,
    clean_company_name,
    from_list_to_dictionary,
    dictionary_mapping_trading_as_company_names,
    map_position_of_subset_items,
    position_to_value,
    basic_preprocessing_of_installations,
)
from asf_core_data.getters.mcs_getters.get_mcs_installers import (
    get_processed_historical_installers_data,
    get_raw_historical_mcs_installers,
)
from asf_core_data.getters.mcs_getters.get_mcs_installations import (
    get_raw_historical_installations_data,
)


raw_installers_data = get_raw_historical_mcs_installers(
    "raw_historical_mcs_installers_20230207.xlsx"
)
processed_installers_data = get_processed_historical_installers_data("20230207")


def test_rename_columns():
    """
    Test if the rename_columns() function works as expected by checking:
    - it successfully transforms a set of predefined columns;
    - if a few expressions can still be found in the column names.
    """
    cols = ["My Column", "ADDDRESS 3", "G/WS heat pump"]
    cols = rename_columns(cols)

    assert cols == ["my_column", "address_3", "g_ws_hp"]

    processed_dataset_cols = [col for col in processed_installers_data.columns]
    for col in processed_dataset_cols:
        assert "heat pump" not in col
        assert " " not in col
        assert "/" not in col
        assert "adddress" not in col
        assert col == col.lower()


def test_join_installation_and_design_vars():
    """
    Test if the join_installation_and_design_vars() function works as expected by checking if
    the percentage of missing values is lower for the new columns than for the original ones
    since the new variables are computed from the "installation" and "design" ones.
    """

    # only focusing on original records
    processed_data = processed_installers_data[
        processed_installers_data["original_record"]
    ]

    # raw columns that contain the "installation" keywords e.g. air_source_hp_installation_start_date
    installation_cols = [
        col for col in raw_installers_data.columns if "installation" in col
    ]

    for col in installation_cols:
        processed_col = col.replace("installation_", "")
        assert len(raw_installers_data[pd.isnull(raw_installers_data[col])]) >= len(
            processed_data[pd.isnull(processed_data[processed_col])]
        )
        # e.g. air source_hp_design_start_date
        design_var = col.replace("installation", "design")
        assert len(
            raw_installers_data[pd.isnull(raw_installers_data[design_var])]
        ) >= len(processed_data[pd.isnull(processed_data[processed_col])])


def test_deal_with_versions_of_trading_as():
    """
    Test if the deal_with_versions_of_trading_as() function works as expected.
    """

    names = [
        "A t/a B",
        "C ta D",
        "Eta F",
        "G a trading name of H",
        "I the trading name of J",
        "A ta B t/a C the trading name of D the trading name of E",
    ]

    new_names = []
    for n in names:
        new_names.append(deal_with_versions_of_trading_as(n))

    assert new_names == [
        "A trading as B",
        "C trading as D",
        "Eta F",
        "G trading as H",
        "I trading as J",
        "A trading as B trading as C trading as D trading as E",
    ]


def test_match_companies_house():
    """
    Get result for the data we know and check accuracy.
    """

    data = processed_installers_data[processed_installers_data["original_record"]]
    data = data[data["effective_to"] >= datetime.today()]
    # data = data[data["effective_from"]>=datetime(2021,1,1)]
    data = data[["company_name", "postcode"]]
    # data = data.sample(n=50)

    data[["full_address", "date_of_creation"]] = data.apply(
        lambda x: match_companies_house(
            x["company_name"], os.environ.get("COMPANIES_HOUSE_API_KEY")
        ),
        axis=1,
    )

    data["postcode_companies_house"] = (
        data["full_address"]
        .apply(lambda x: x.split(",")[-1].strip() if x is not None else None)
        .str.upper()
        .str.replace(" ", "")
    )

    # check if disagreement only happens in less than 10% of the instances
    assert (
        len(data[data["postcode"] != data["postcode_companies_house"]]) / len(data)
        < 0.1
    )


def test_recompute_full_address():
    """
    Test if the recompute_full_address() function works as expected by checking
    if there's at least one instance (in non-original records) where full address
    is not None.
    """

    data = processed_installers_data[~processed_installers_data["original_record"]]

    fraction = len(data[~pd.isnull(data["full_address"])]) / len(data)

    assert fraction > 0


def test_get_missing_installers_info():
    """
    Test if the test_get_missing_installers_info() function works as expected by
    checking if we have at least one non-original record (since we know they exist).
    """

    data = processed_installers_data[~processed_installers_data["original_record"]]

    assert len(data) > 0


def test_update_full_address():
    """
    Test if the update_full_address() function works as expected by
    checking if we are not concatenating nans to the full address string.
    """

    data = processed_installers_data[
        ~pd.isnull(processed_installers_data["full_address"])
    ]

    assert len(data[data["full_address"].str.contains(",nan")]) == 0


def test_create_certified_flags():
    """
    Test if the create_certified_flags() function works as expected by
    checking:
    - if there's a start date then the certified flag is True
    - that these flags only take two values, True and False
    """

    data = processed_installers_data.copy()

    certified_flags = [col for col in data.columns if "certified" in col]

    for col in certified_flags:
        start_date_col = col.replace("certified", "start_date")
        assert len(data[data[col]]) == len(data[~pd.isnull(data[start_date_col])])
        assert len(set(data[col].unique()).intersection([True, False])) == 2


def test_clean_company_name():
    """
    Test if the clean_company_name() function works as expected.
    """

    company_name = "COMPANY & A (old/ account) ltd ltd. limited limited."
    assert clean_company_name(company_name) == "company a"

    company_name = "\t A trading as    B "
    assert clean_company_name(company_name) == "a trading as b"


def test_from_list_to_dictionary():
    """
    Test if the from_list_to_dictionary() function works as expected.
    """

    l = ["Company A", "Company B", "Company A trading as Company B"]

    assert from_list_to_dictionary(l) == {
        "Company B": "Company A",
        "Company A trading as Company B": "Company A",
    }


def test_dictionary_mapping_trading_as_company_names():
    """
    Test if the dictionary_mapping_trading_as_company_names() function works as expected.
    """

    test_df = pd.DataFrame(columns=["processed_company_name"])
    test_df["processed_company_name"] = ["a trading as b", "c trading as d"]

    assert dictionary_mapping_trading_as_company_names(test_df) == {
        "b": "a",
        "a trading as b": "a",
        "d": "c",
        "c trading as d": "c",
    }


def test_map_position_of_subset_items():
    """
    Test if the map_position_of_subset_items() function works as expected.
    """

    l = ["company a ltd", "a ltd", "a"]

    assert map_position_of_subset_items(l) == {0: 2, 1: 2}


def test_position_to_value():
    """
    Test if the position_to_value() function works as expected.
    """

    l = ["a", "b", "c"]
    d = {0: 2, 1: 2}

    assert position_to_value(l, d) == {"a": "c", "b": "c"}


def test_map_installers_same_location_different_id():
    """
    Test if the map_installers_same_location_different_id() function works as expected
    by checking if installers with the same postcode and first line of address also
    have the same company unique ID.
    """

    assert list(
        processed_installers_data.groupby(["postcode", "address_1"])
        .nunique()["company_unique_id"]
        .unique()
    ) == [1]


def test_create_installer_unique_id():
    """
    Test if the create_installer_unique_id() function works as expected
    by checking that the company unique ID is never missing.
    """

    data = get_processed_historical_installers_data("20230207")

    assert len(data[pd.isnull(data["company_unique_id"])]) == 0


def test_geocode_postcode():
    """
    Test if the geocode_postcode() function works as expected by checking
    that only a small percentage of postcodes are missing.
    """

    processed_data = processed_installers_data[
        ~processed_installers_data["original_record"]
    ]

    fraction_missing = len(processed_data[pd.isnull(processed_data["postcode"])]) / len(
        processed_data
    )

    assert fraction_missing < 0.3


def test_add_certification_body_info():
    """
    Test if the add_certification_body_info() function works as expected
    by checking that the certification body values in installations
    match the values in installers.
    """

    raw_installations = get_raw_historical_installations_data(
        "raw_historical_mcs_installations_20230119.xlsx"
    )
    basic_preprocessing_of_installations(raw_installations)

    cb_installations = set(raw_installations["certification_body"].unique())
    cb_installers = set(
        processed_installers_data[
            ~pd.isnull(processed_installers_data["certification_body"])
        ]["certification_body"].unique()
    )

    assert cb_installers.issubset(cb_installations)


def test_preprocess_historical_installers():
    """
    Test if the preprocess_historical_installers() function works as expected
    by checking that no instances have been lost.
    """
    processed_data = processed_installers_data[
        processed_installers_data["original_record"]
    ]

    assert len(raw_installers_data.drop_duplicates()) == len(processed_data)
