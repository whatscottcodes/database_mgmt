from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import pandas as pd
import numpy as np
import sqlite3
from db_rename_cols import address_cols, address_drop_cols
import os
import argparse
from db_rename_cols import (
    statewide_geocoding,
    non_geopy_addresses,
    database_path,
    ehr_file_location,
)

try:
    current_tough_addresses = pd.read_csv(non_geopy_addresses)
except FileNotFoundError:
    pass


def load_clean_addresses():
    addresses = pd.read_excel(
        f"{ehr_file_location}addresses.xls", "Sheet1", index_col=None
    )

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

    addresses["full_address"] = (
        addresses["address"].str.title().str.rstrip()
        + ", "
        + addresses["city"].str.title()
        + " "
        + addresses["state"]
    )
    addresses.to_csv("updated_addresses.csv")
    return addresses


def check_for_new(addresses):

    addresses["pk"] = addresses["member_id"].astype(str) + addresses["address"]

    conn = sqlite3.connect(database_path)
    address_db = pd.read_sql("SELECT member_id, address from addresses", conn)
    conn.close()

    address_db["pk"] = address_db["member_id"].astype(str) + address_db["address"]

    address_new = addresses[-addresses.pk.isin(address_db.pk)].copy()

    try:
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


def geocode_via_open_map(address_new, state_addresses, address_drops, address_cols):
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
        tough_addresses = current_tough_addresses.append(tough_addresses)
    except NameError:
        pass
    tough_addresses.to_csv(non_geopy_addresses, index=False)

    geopy_geocoded.drop(
        [col for col in geopy_geocoded.columns if col not in address_cols],
        axis=1,
        inplace=True,
    )

    return geopy_geocoded


def append_and_save(geocoded, geopy_geocoded):
    addresses_to_add = geocoded.append(geopy_geocoded)
    addresses_to_add.drop_duplicates(subset=["member_id", "address"], inplace=True)
    addresses_to_add.to_csv(
        "C:\\Users\\snelson\\repos\\db_mgmt\\data_raw\\addresses.csv", index=False
    )

    os.remove(f"{ehr_file_location}addresses.xls")
    print("Addresses Complete!")
    return addresses_to_add


def create_addresses_to_add(update=True):
    try:
        address_new = load_clean_addresses()
    except FileNotFoundError:
        print("Missing addressess")
        return None
    state_addresses = load_open_map_dataset()
    if update:
        address_new = check_for_new(address_new)

    geocoded, geocode_needed = geocode_via_open_map(
        address_new, state_addresses, address_drop_cols, address_cols
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

    create_addresses_to_add(**vars(arguments))

