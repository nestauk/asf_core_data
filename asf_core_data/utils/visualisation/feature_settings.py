from asyncio import constants


country_order = ["England", "Wales", "Scotland"]

rating_order = ["A", "B", "C", "D", "E", "F", "G", "unknown"]

efficiency_order = ["unknown", "Very Poor", "Poor", "Average", "Good", "Very Good"]

tenure_order = ["owner-occupied", "rental (social)", "rental (private)", "unknown"]

prop_type_order = ["House", "Bungalow", "Park home", "Maisonette", "Flat"]


year_order = [
    2008,
    2009,
    2010,
    2011,
    2012,
    2013,
    2014,
    2015,
    2016,
    2017,
    2018,
    2019,
    2020,
    2021,
]

built_form_order = [
    "End-Terrace",
    "Enclosed End-Terrace",
    "Mid-Terrace",
    "Enclosed Mid-Terrace",
    "Detached",
    "Semi-Detached",
    "unknown",
]

heating_source_order = ["oil", "LPG", "gas", "electric", "unknown"]

constr_year_order = [
    "England and Wales: before 1900",
    "England and Wales: 1900-1929",
    "England and Wales: 1930-1949",
    "England and Wales: 1950-1966",
    "England and Wales: 1967-1975",
    "England and Wales: 1976-1982",
    "England and Wales: 1983-1990",
    "England and Wales: 1991-1995",
    "England and Wales: 1996-2002",
    "England and Wales: 2003-2006",
    "England and Wales: 2007 onwards",
    "Scotland: before 1919",
    "Scotland: 1919-1929",
    "Scotland: 1930-1949",
    "Scotland: 1950-1964",
    "Scotland: 1965-1975",
    "Scotland: 1976-1983",
    "Scotland: 1984-1991",
    "Scotland: 1992-1998",
    "Scotland: 1999-2002",
    "Scotland: 2003-2007",
    "Scotland: 2008 onwards",
    "unknown",
]

const_year_order_merged = [
    "England and Wales: before 1900",
    "Scotland: before 1919",
    "1900-1929",
    "1930-1949",
    "1950-1966",
    "1965-1975",
    "1976-1983",
    "1983-1991",
    "1991-1998",
    "1996-2002",
    "2003-2007",
    "2007 onwards",
    "unknown",
]

const_year_order_merged_no_scotland = [
    "England and Wales: before 1900",
    "1900-1929",
    "1930-1949",
    "1950-1966",
    "1965-1975",
    "1976-1983",
    "1983-1991",
    "1991-1998",
    "1996-2002",
    "2003-2007",
    "2007 onwards",
    "unknown",
]

const_year_order_merged_no_england_wales = [
    "Scotland: before 1919",
    "1900-1929",
    "1930-1949",
    "1950-1966",
    "1965-1975",
    "1976-1983",
    "1983-1991",
    "1991-1998",
    "1996-2002",
    "2003-2007",
    "2007 onwards",
    "unknown",
]


map_dict = {
    "CONSTRUCTION_AGE_BAND": const_year_order_merged,
    "CONSTRUCTION_AGE_BAND_no_scotland": const_year_order_merged_no_scotland,
    "CONSTRUCTION_AGE_BAND_no_england_wales": const_year_order_merged_no_england_wales,
    "PROPERTY_TYPE": prop_type_order,
    "COUNTRY": country_order,
    "INSPECTION_DATE": year_order,
    "CURRENT_ENERGY_RATING": rating_order,
    "POTENTIAL_ENERGY_RATING": rating_order,
    "MAINHEAT_ENERGY_EFF": efficiency_order,
    "SHEATING_ENERGY_EFF": efficiency_order,
    "HOT_WATER_ENERGY_EFF": efficiency_order,
    "FLOOR_ENERGY_EFF": efficiency_order,
    "WINDOWS_ENERGY_EFF": efficiency_order,
    "WALLS_ENERGY_EFF": efficiency_order,
    "ROOF_ENERGY_EFF": efficiency_order,
    "MAINHEATC_ENERGY_EFF": efficiency_order,
    "LIGHTING_ENERGY_EFF": efficiency_order,
    "TENURE": tenure_order,
    "BUILT_FORM": built_form_order,
    "HEATING_SOURCE": heating_source_order,
}
