import sqlite3
import argparse
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import pandas as pd
import numpy as np
from file_paths import (
    statewide_geocoding,
    non_geopy_addresses,
    database_path,
    raw_data,
    processed_data,
)


def load_clean_addresses():
    """
    Cleans/Processes dataset
    
    Loads dataset from raw_data file
    Splits out any units from the address column
        and merges them with the address_2 column
    Zip code is cleaned to not include numbers after a -
        and any not leading with a 0 have one added.

    Returns:
        DataFrame: cleaned dataframe

    """
    addresses = pd.read_csv(f"{raw_data}\\addresses.csv")

    addresses = addresses[addresses.member_id != 1003].copy()
    addresses["address"] = addresses["address"].str.title()
    addresses[["address", "unit"]] = addresses["address"].str.split(
        "(?=Apt |Flr |Fl |Box |Bldg |Unit |#)(.*$)", expand=True
    )[[0, 1]]
    addresses["unit"] = np.where(
        addresses["unit"].isnull(), addresses["address_2"], addresses["unit"]
    )
    addresses["address"] = addresses.address.str.replace(".", "")
    addresses["address"] = addresses.address.str.strip()

    addresses["unit"] = addresses.unit.str.replace(".", "")
    addresses["unit"] = addresses.unit.str.strip()

    addresses["zip"] = addresses["zip"].str.replace(" ", "-")
    addresses["zip"] = addresses["zip"].str.split("-", expand=True)[0]
    addresses["zip"] = addresses["zip"].apply(
        lambda x: ("0" + str(x))[:5] if len(str(x)) < 5 else str(x)[:5]
    )

    addresses["full_address"] = (
        addresses["address"].str.title().str.rstrip()
        + ", "
        + addresses["city"].str.title()
        + " "
        + addresses["state"]
    )
    return addresses


def check_for_new(addresses):
    """
    Checks to for addresses in the dataframe that have a primary key
    that is not in the existing database

    Args:
        addresses(df): dataframe of addresses

    Returns:
        DataFrame: cleaned dataframe

    """
    addresses["pk"] = addresses["member_id"].astype(str) + addresses["address"]

    conn = sqlite3.connect(database_path)
    address_db = pd.read_sql("SELECT member_id, address from addresses", conn)
    conn.close()

    address_db["pk"] = address_db["member_id"].astype(str) + address_db["address"]

    address_new = addresses[-addresses.pk.isin(address_db.pk)].copy()

    try:
        current_tough_addresses = pd.read_csv(non_geopy_addresses)
        current_tough_addresses["pk"] = (
            current_tough_addresses["member_id"].astype(str)
            + current_tough_addresses["address"]
        )
        address_new = address_new[
            -address_new.pk.isin(current_tough_addresses.pk)
        ].copy()
    except NameError:
        pass

    return address_new


def load_open_map_dataset():
    """
    Loads the OpenMaps dataset and creates a merged column
    of the number, street, and city.

    Returns:
        DataFrame: dataframe

    """
    state_addresses = pd.read_csv(statewide_geocoding)

    state_addresses["full_address"] = (
        state_addresses["NUMBER"].fillna("")
        + " "
        + state_addresses["STREET"].str.title()
        + ", "
        + state_addresses["CITY"].str.title()
        + " "
        + "RI"
    )

    return state_addresses


def geocode_via_open_map(address_new, state_addresses, address_cols):
    """
    Looks for address in the address_new dataframe that are in the
    state_addresses file and copies the lat/lon values

    Args:
        address_new(DataFrame): pandas dataframe of new addresses
        state_addresses(DataFrame): pandas dataframe of OpenMaps addresses
        address_cols(list): list of columns to keep

    Returns:
        geocoded(DataFrame): dataframe with lat/lon parsed
        geocode_needed(DataFrame): dataframe that did not have lat/lon parsed

    """
    address_new = address_new.merge(state_addresses, on="full_address", how="left")

    address_new.rename(columns={"LON": "lon", "LAT": "lat"}, inplace=True)

    address_drops = [col for col in address_new.columns if col not in address_cols]

    address_new.drop(address_drops, axis=1, inplace=True)

    address_new.reset_index(drop=True, inplace=True)

    geocoded = address_new[address_new.lat.notnull()].copy()
    geocode_needed = address_new[address_new.lat.isnull()].copy()

    geocode_needed["geocode_address"] = (
        geocode_needed["address"].str.title().str.rstrip()
        + ", "
        + geocode_needed["city"].str.title()
        + " "
        + geocode_needed["state"]
        + " "
        + geocode_needed["zip"].astype(str)
    )

    return geocoded, geocode_needed


def geocode_via_geopy(geocode_needed, address_cols):
    """
    Uses geopy and Nominatim to geocode the addresses not in the OpenMaps dataset.

    Args:
        geocode_needed(DataFrame): pandas dataframe of addresses that could
            not be geocoded using the OpenMaps dataset
        address_cols(list): list of columns to keep

    Returns:
        geopy_geocoded(DataFrame): dataframe with lat/lon parsed

    Output:
        csv:file of tough addresses that could not be parsed
    """
    geolocator = Nominatim(user_agent="specify_your_app_name_here")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=5)

    geocode_needed["local"] = geocode_needed["geocode_address"].apply(geocode)

    geocode_needed["coordinates"] = geocode_needed["local"].apply(
        lambda loc: tuple(loc.point) if loc else None
    )

    geocode_needed["lat"] = geocode_needed["coordinates"].apply(
        lambda loc: loc[0] if loc else None
    )

    geocode_needed["lon"] = geocode_needed["coordinates"].apply(
        lambda loc: loc[1] if loc else None
    )

    tough_addresses = geocode_needed[geocode_needed.lon.isnull()].copy()
    geopy_geocoded = geocode_needed[geocode_needed.lon.notnull()].copy()

    print(f"{tough_addresses.shape[0]} addresses were not parsed see tough_adds file.")

    try:
        current_tough_addresses = pd.read_csv(non_geopy_addresses)
        tough_addresses = current_tough_addresses.append(tough_addresses)
        tough_addresses.drop_duplicates(subset=["address"], inplace=True)
    except FileNotFoundError:
        pass

    tough_addresses.to_csv(non_geopy_addresses, index=False)

    geopy_geocoded.drop(
        [col for col in geopy_geocoded.columns if col not in address_cols],
        axis=1,
        inplace=True,
    )

    return geopy_geocoded


def append_and_save(geocoded, geopy_geocoded):
    """
    Merges geocoded dataframes and adds an as of date column
    and a column indicating these are the new active addresses
    for each ppt

    Args:
        geocoded(DataFrame): pandas dataframe of addresses that were
            geocoded using openmaps
        geopy_geocoded(DataFrame): pandas dataframe of addresses that were
            geocoded using geopy

    Returns:
        addresses_to_add(DataFrame): dataframe with lat/lon parsed

    Output:
        csv: processed data file
    """
    addresses_to_add = geocoded.append(geopy_geocoded)
    addresses_to_add.drop_duplicates(subset=["member_id", "address"], inplace=True)
    addresses_to_add["as_of"] = pd.to_datetime("today").date()
    addresses_to_add["active"] = 1
    addresses_to_add = addresses_to_add[addresses_to_add.member_id != 1003]
    addresses_to_add.to_csv(f"{processed_data}\\addresses.csv", index=False)
    return addresses_to_add


def process_addresses(update=True):
    """
    Cleans/Processes dataset
    
    Drops non-indicated columns
    Geocodes addresses

    Returns:
        DataFrame: cleaned dataframe

    Output:
        csv: processed data file
        csv:file of tough addresses that could not be parsed

    """
    address_cols = [
        "lat",
        "lon",
        "active",
        "address",
        "as_of",
        "city",
        "member_id",
        "state",
        "zip",
    ]
    address_new = load_clean_addresses()

    state_addresses = load_open_map_dataset()

    if update is True:
        address_new = check_for_new(address_new)

    geocoded, geocode_needed = geocode_via_open_map(
        address_new, state_addresses, address_cols
    )
    geopy_geocoded = geocode_via_geopy(geocode_needed, address_cols)

    return append_and_save(geocoded, geopy_geocoded)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update",
        default=True,
        help="Are we updating the database or creating it? True for update",
    )

    arguments = parser.parse_args()

    process_addresses(**vars(arguments))

