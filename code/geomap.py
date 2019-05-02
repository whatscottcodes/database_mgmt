import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from datetime import date
import numpy as np

today = str(date.today())


def geolocate_addresses(ppts_addresses, state_addresses):
    ppts_addresses["address"] = ppts_addresses["address"].str.title()
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        ".", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Apt [a-zA-Z]+[0-9]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Apt [0-9]+[a-zA-Z]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Apt [0-9]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Bldg [a-zA-Z]+[0-9]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Bldg [a-zA-Z]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Bldg [0-9]+ [a-zA-Z]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Bldg [0-9]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Fl [a-zA-Z]+[0-9]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Fl [0-9]+ [a-zA-Z]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Fl [0-9]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"# 2R", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"# 8F", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"# [0-9]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"# [0-9]+[a-zA-Z]+", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Lowr Level", "", regex=True
    )
    ppts_addresses["address"] = ppts_addresses["address"].str.replace(
        r"Unit [0-9]+", "", regex=True
    )

    ppts_addresses["full_address"] = (
        ppts_addresses.address.str.rstrip() + ", " + ppts_addresses.city
    )

    state_addresses["full_address"] = (
        state_addresses.NUMBER
        + " "
        + state_addresses.STREET.str.title()
        + ", "
        + state_addresses.CITY.str.title()
    )

    ppts_addresses = ppts_addresses.merge(
        state_addresses, on="full_address", how="left"
    )

    ppts_addresses.drop_duplicates(subset=["member_id"], inplace=True)

    drop_cols = [
        "NUMBER",
        "STREET",
        "UNIT",
        "DISTRICT",
        "CITY",
        "REGION",
        "HASH",
        "POSTCODE",
        "ID",
    ]
    ppts_addresses.drop(drop_cols, axis=1, inplace=True)

    geocode_df = ppts_addresses[ppts_addresses.LON.isnull()].copy()

    geocode_df["geocode_address"] = (
        geocode_df["full_address"] + " " + geocode_df["state"] + " " + geocode_df["zip"]
    )

    geolocator = Nominatim(user_agent="specify_your_app_name_here")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=5)

    geocode_df["local"] = geocode_df["geocode_address"].apply(geocode)

    geocode_df["coordinates"] = geocode_df["local"].apply(
        lambda loc: tuple(loc.point) if loc else None
    )

    geocode_df["LAT"] = geocode_df["coordinates"].apply(
        lambda loc: loc[0] if loc else None
    )
    geocode_df["LON"] = geocode_df["coordinates"].apply(
        lambda loc: loc[1] if loc else None
    )

    not_null_coords = ppts_addresses[-ppts_addresses.LON.isnull()].copy()

    final_addresses = not_null_coords.append(geocode_df, sort=True)

    final_addresses.to_csv("current_ppts_with_coors.csv", index=False)

    return final_addresses
