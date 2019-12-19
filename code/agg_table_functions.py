import argparse
from paceutils import (
    Enrollment,
    Demographics,
    Incidents,
    Utilization,
    Team,
    Helpers,
    CenterEnrollment,
    Quality,
)
import pandas as pd
import sqlite3
from data_to_sql import sql_table_utils as stu
from file_paths import (
    processed_data,
    agg_db_path,
    daily_census_data,
    update_logs_folder,
)

end_date = pd.to_datetime("today").strftime("%Y-%m-%d")


def create_enrollment_agg_table(
    params=("2005-12-01", end_date), db_path=agg_db_path, freq="MS", update=True
):
    """
    Create an aggregate table of enrollment values

    Loops through indicator/column name and matching function,
    creates a dataframe with a month column and value column
    and adds that value column to a master dataframe with a month column

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'
        db_path(str): path to the aggregate database
        freq(str): "MS" or "QS" indicates if values should be grouped monthly
            or quarterly
        update(bool): if the table being updated or created

    Returns:
        DataFrame: the created dataframe

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    e = Enrollment()

    if str(update).lower() == "true":
        if freq == "MS":
            params = (e.last_month()[0], e.month_to_date()[1])
            update = True
        else:
            params = (e.last_quarter()[0], e.month_to_date()[1])
            update = True
    else:
        update = False

    enrollment_funcs = {
        "disenrolled": e.disenrolled,
        "voluntary_disenrolled": e.voluntary_disenrolled_percent,
        "enrolled": e.enrolled,
        "deaths": e.deaths,
        "net_enrollment": e.net_enrollment,
        "avg_years_enrolled": e.avg_years_enrolled,
        "inquiries": e.inquiries,
        "avg_days_to_enrollment": e.avg_days_to_enrollment,
        "conversion_rate_180_days": e.conversion_rate_180_days,
    }

    enrollment_agg = e.loop_plot_df(e.census_on_end_date, params, freq=freq).rename(
        columns={"Month": "month", "Value": "census"}
    )

    for col_title, func in enrollment_funcs.items():
        dff = e.loop_plot_df(func, params, freq=freq).rename(
            columns={"Month": "month", "Value": col_title}
        )
        enrollment_agg = enrollment_agg.merge(dff, on="month", how="left")

    prev_months = [0]
    prev_months.extend(enrollment_agg["census"][:-1])

    growth = ((enrollment_agg["census"] - prev_months) / prev_months) * 100
    if not update:
        growth[0] = 100

    enrollment_agg["growth_rate"] = growth

    enrollment_agg["churn_rate"] = (
        enrollment_agg["disenrolled"] / enrollment_agg["census"]
    ) * 100

    enrollment_agg.to_csv(f"{processed_data}\\enrollment_agg.csv", index=False)

    if freq == "QS":
        table_name = "enrollment_q"
    else:
        table_name = "enrollment"

    conn = sqlite3.connect(db_path)

    if update:

        stu.update_sql_table(
            enrollment_agg, table_name, conn, ["month"], agg_table=True
        )

    else:
        stu.create_table(enrollment_agg, table_name, conn, ["month"], agg_table=True)

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\enrollment_agg{str(pd.to_datetime('today').date())}.txt",
        "a",
    ).close()

    return enrollment_agg


def create_demographic_agg_table(
    params=("2005-12-01", end_date), db_path=agg_db_path, freq="MS", update=True
):
    """
    Create an aggregate table of demographic values

    Loops through indicator/column name and matching function,
    creates a dataframe with a month column and value column
    and adds that value column to a master dataframe with a month column

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'
        db_path(str): path to the aggregate database
        freq(str): "MS" or "QS" indicates if values should be grouped monthly
            or quarterly
        update(bool): if the table being updated or created

    Returns:
        DataFrame: the created dataframe

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    d = Demographics()

    if str(update).lower() == "true":
        if freq == "MS":
            params = (d.last_month()[0], d.month_to_date()[1])
            update = True
        else:
            params = (d.last_quarter()[0], d.month_to_date()[1])
            update = True

    demographic_func = {
        "dual_enrolled": d.dual_count,
        "percent_dual_enrolled": d.percent_dual,
        "medicare_only": d.medicare_only_count,
        "percent_medicare_only": d.percent_medicare_only,
        "medicaid_only": d.medicaid_only_count,
        "percent_medicaid_only": d.percent_medicaid_only,
        "private_pay": d.private_pay_count,
        "percent_private_pay": d.percent_private_pay,
        "percent_primary_non_english": d.percent_primary_non_english,
        "percent_non_white": d.percent_non_white,
        "living_in_community": d.living_in_community,
        "percent_living_in_community": d.living_in_community_percent,
        "percent_below_65": d.percent_age_below_65,
        "percent_female": d.percent_female,
        "bh_dx_percent": d.behavorial_dx_percent,
        "six_chronic_conditions": d.over_six_chronic_conditions_percent,
        "percent_attending_dc": d.percent_attending_dc,
    }

    demo_agg = d.loop_plot_df(d.avg_age, params, freq=freq).rename(
        columns={"Month": "month", "Value": "avg_age"}
    )

    for col_title, func in demographic_func.items():
        dff = d.loop_plot_df(func, params, freq=freq).rename(
            columns={"Month": "month", "Value": col_title}
        )
        demo_agg = demo_agg.merge(dff, on="month", how="left")

    demo_agg.to_csv(f"{processed_data}\\demographics_agg.csv", index=False)

    if freq == "QS":
        table_name = "demographics_q"
    else:
        table_name = "demographics"

    conn = sqlite3.connect(db_path)

    if update:
        stu.update_sql_table(demo_agg, table_name, conn, ["month"], agg_table=True)

    else:
        stu.create_table(demo_agg, table_name, conn, ["month"], agg_table=True)

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\demographic_agg{str(pd.to_datetime('today').date())}.txt",
        "a",
    ).close()

    return demo_agg


def create_incidents_agg_tables(
    params=("2017-07-01", end_date),
    incident_table="falls",
    db_path=agg_db_path,
    freq="MS",
    update=True,
):
    """
    Create an aggregate table of incidents values

    Loops through indicator/column name and matching function,
    creates a dataframe with a month column and value column
    and adds that value column to a master dataframe with a month column

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'
        incident_table(str): incident to use to create aggregate table.
        db_path(str): path to the aggregate database
        freq(str): "MS" or "QS" indicates if values should be grouped monthly
            or quarterly
        update(bool): if the table being updated or created

    Returns:
        DataFrame: the created dataframe

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    i = Incidents()

    if str(update).lower() == "true":
        if freq == "MS":
            params = (i.last_month()[0], i.month_to_date()[1])
            update = True
        else:
            params = (i.last_quarter()[0], i.month_to_date()[1])
            update = True

    incidents_func = {
        "total": i.total_incidents,
        "unique_ppts": i.ppts_w_incident,
        "num_ppts_with_multiple": i.num_of_incident_repeaters,
        "num_by_ppts_with_multiple": i.incidents_by_repeaters,
        "percent_without": i.percent_without_incident_in_period,
        "percent_by_repeaters": i.percent_by_repeaters,
        "ppts_with_above_avg": i.ppts_above_avg,
        "percent_of_ppts_with_above_avg": i.percent_of_ppts_over_avg,
    }

    additonal_funcs = {
        "falls": {
            "major_harm_percent": i.major_harm_percent,
            "total_adjusted": i.adjusted_incident_count,
            "adjusted_per100MM": i.adjusted_per_100MM,
        },
        "infections": {
            "sepsis_per_100MM": i.sepsis_per_100,
            "uti_per_100MM": i.uti_per_100,
        },
        "med_errors": {
            "major_harm_percent": i.major_harm_percent,
            "high_risk": i.high_risk_med_error_count,
        },
        "wounds": {
            "avg_healing_time": i.avg_wound_healing_time,
            "percent_unstageable": i.unstageable_wound_percent,
            "pressure_ulcer_per_100": i.pressure_ulcer_per_100,
        },
        "burns": {
            "third_degree_rate": i.third_degree_burn_rate,
            "rn_assessment_percent": i.rn_assessment_following_burn_percent,
        },
    }

    all_funcs = {**incidents_func, **additonal_funcs[incident_table]}
    df = i.loop_plot_df(
        i.incident_per_100MM, params, freq=freq, additional_func_args=[incident_table]
    ).rename(columns={"Month": "month", "Value": "per_100MM"})

    for col_title, func in all_funcs.items():
        if col_title in [
            "sepsis_per_100MM",
            "uti_per_100MM",
            "high_risk",
            "avg_healing_time",
            "percent_unstageable",
            "pressure_ulcer_per_100",
            "third_degree_rate",
            "rn_assessment_percent",
        ]:
            dff = i.loop_plot_df(func, params, freq=freq).rename(
                columns={"Month": "month", "Value": col_title}
            )
        else:
            dff = i.loop_plot_df(
                func, params, freq=freq, additional_func_args=[incident_table]
            ).rename(columns={"Month": "month", "Value": col_title})

        df = df.merge(dff, on="month", how="left")

    df.to_csv(f"{processed_data}\\{incident_table}_agg.csv", index=False)

    if freq == "QS":
        table_name = f"{incident_table}_q"
    else:
        table_name = incident_table

    conn = sqlite3.connect(db_path)

    if update:
        stu.update_sql_table(df, table_name, conn, ["month"], agg_table=True)

    else:
        stu.create_table(df, table_name, conn, ["month"], agg_table=True)
    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\{incident_table}_agg{str(pd.to_datetime('today').date())}.txt",
        "a",
    ).close()

    return df


def create_utilization_table(
    params=("2017-07-01", end_date), db_path=agg_db_path, freq="MS", update=True
):
    """
    Create an aggregate table of utilization values

    Loops through indicator/column name and matching function,
    creates a dataframe with a month column and value column
    and adds that value column to a master dataframe with a month column

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'
        db_path(str): path to the aggregate database
        freq(str): "MS" or "QS" indicates if values should be grouped monthly
            or quarterly
        update(bool): if the table being updated or created

    Returns:
        DataFrame: the created dataframe

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    u = Utilization()

    if str(update).lower() == "true":
        if freq == "MS":
            params = (u.last_month()[0], u.month_to_date()[1])
            update = True
        else:
            params = (u.last_quarter()[0], u.month_to_date()[1])
            update = True

    utilization_types = ["acute", "psych", "skilled", "respite", "custodial"]

    utilization_func = {
        "_admissions": u.admissions_count,
        "_admissions_per_100MM": u.admissions_per_100MM,
        "_discharges": u.discharges_count,
        "_alos": u.alos,
        "_los_per_100MM": u.los_per_100mm,
        "_days": u.utilization_days,
        "_days_per_100MM": u.days_per_100MM,
        "_unique_admissions": u.unique_admissions_count,
        "_weekend_admissions": u.weekend_admissions_count,
        "_weekend_percent": u.weekend_admission_percent,
    }

    er_visit_func = {
        "er_visits_per_100MM": u.admissions_per_100MM,
        "er_visits": u.admissions_count,
    }

    nf_only_funcs = {
        "_per_100MM": u.ppts_in_utl_per_100MM,
        "_percent": u.ppts_in_utl_percent,
    }

    utl_agg = u.loop_plot_df(u.er_to_inp_rate, params, freq=freq).rename(
        columns={"Month": "month", "Value": "er_to_inp_rate"}
    )

    for col_title, func in utilization_func.items():
        for utilization in utilization_types:
            dff = u.loop_plot_df(
                func, params, freq=freq, additional_func_args=[utilization]
            ).rename(columns={"Month": "month", "Value": utilization + col_title})
            utl_agg = utl_agg.merge(dff, on="month", how="left")

    for utilization in ["acute", "psych", "er_only"]:
        dff = u.loop_plot_df(
            u.readmits_30day_rate, params, freq=freq, additional_func_args=[utilization]
        ).rename(
            columns={"Month": "month", "Value": utilization + "_30_day_readmit_rate"}
        )
        utl_agg = utl_agg.merge(dff, on="month", how="left")

    for col_title, func in er_visit_func.items():
        dff = u.loop_plot_df(
            func, params, freq=freq, additional_func_args=["er_only"]
        ).rename(columns={"Month": "month", "Value": col_title})
        utl_agg = utl_agg.merge(dff, on="month", how="left")

    for col_title, func in nf_only_funcs.items():
        for nf_type in ["skilled", "respite", "custodial", "alfs"]:
            dff = u.loop_plot_df(
                func, params, freq=freq, additional_func_args=[nf_type]
            ).rename(columns={"Month": "month", "Value": nf_type + col_title})
            utl_agg = utl_agg.merge(dff, on="month", how="left")

    dff = u.loop_plot_df(
        u.percent_nf_discharged_to_higher_loc, params, freq=freq
    ).rename(columns={"Month": "month", "Value": "nf_higher_loc_discharge_percent"})
    utl_agg = utl_agg.merge(dff, on="month", how="left")

    utl_agg.to_csv(f"{processed_data}\\utilization_agg.csv", index=False)

    if freq == "QS":
        table_name = "utilization_q"
    else:
        table_name = "utilization"
    conn = sqlite3.connect(db_path)

    if update:
        stu.update_sql_table(utl_agg, table_name, conn, ["month"], agg_table=True)

    else:
        stu.create_table(utl_agg, table_name, conn, ["month"], agg_table=True)
    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\utilization_agg{str(pd.to_datetime('today').date())}.txt",
        "a",
    ).close()

    return utl_agg


def create_quality_agg_table(
    params=("2005-12-01", end_date), db_path=agg_db_path, freq="MS", update=True
):
    """
    Create an aggregate table of quality values

    Loops through indicator/column name and matching function,
    creates a dataframe with a month column and value column
    and adds that value column to a master dataframe with a month column

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'
        db_path(str): path to the aggregate database
        freq(str): "MS" or "QS" indicates if values should be grouped monthly
            or quarterly
        update(bool): if the table being updated or created

    Returns:
        DataFrame: the created dataframe

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    q = Quality()

    if str(update).lower() == "true":
        if freq == "MS":
            params = (q.last_month()[0], q.month_to_date()[1])
            update = True
        else:
            params = (q.last_quarter()[0], q.month_to_date()[1])
            update = True

    quality_func = {
        "mortality_within_30_days_of_discharge": q.mortality_within_30days_of_discharge_rate,
        "percent_of_discharges_with_mortality_in_30": q.percent_of_discharges_with_mortality_in_30,
        "no_hosp_admission_since_enrollment": q.no_hosp_admission_since_enrollment,
        "no_hosp_admission_last_year": q.no_hosp_admission_last_year,
        "pneumo_rate": q.pneumo_rate,
        "influ_rate": q.influ_rate,
        "avg_days_until_nf_admission": q.avg_days_until_nf_admission,
    }

    quality_agg = q.loop_plot_df(q.mortality_rate, params, freq=freq).rename(
        columns={"Month": "month", "Value": "mortality_rate"}
    )

    for col_title, func in quality_func.items():
        dff = q.loop_plot_df(func, params, freq=freq).rename(
            columns={"Month": "month", "Value": col_title}
        )
        quality_agg = quality_agg.merge(dff, on="month", how="left")

    quality_agg.to_csv(f"{processed_data}\\quality_agg.csv", index=False)

    if freq == "QS":
        table_name = "quality_q"
    else:
        table_name = "quality"

    conn = sqlite3.connect(db_path)

    if update:
        stu.update_sql_table(quality_agg, table_name, conn, ["month"], agg_table=True)

    else:
        stu.create_table(quality_agg, table_name, conn, ["month"], agg_table=True)

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\quality_agg{str(pd.to_datetime('today').date())}.txt",
        "a",
    ).close()

    return quality_agg


def create_team_utl_agg_table(
    params=("2017-07-01", end_date), db_path=agg_db_path, freq="MS", update=True
):
    """
    Create an aggregate table of team utilization values

    Loops through indicator/column name and matching function,
    creates a dataframe with a month column and value column
    and adds that value column to a master dataframe with a month column

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'
        db_path(str): path to the aggregate database
        freq(str): "MS" or "QS" indicates if values should be grouped monthly
            or quarterly
        update(bool): if the table being updated or created

    Returns:
        DataFrame: the created dataframe

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    t = Team()

    if str(update).lower() == "true":
        if freq == "MS":
            params = (t.last_month()[0], t.month_to_date()[1])
            update = True
        else:
            params = (t.last_quarter()[0], t.month_to_date()[1])
            update = True

    utilization_types = ["acute", "psych", "skilled", "respite", "custodial"]

    utilization_need_args = {
        "_admissions": t.admissions_by_team,
        "_days": t.days_by_team,
        "_discharges": t.discharges_by_team,
        "_alos": t.alos_for_discharges_by_team,
    }

    utilization = {
        "readmits": t.readmits_by_team,
        "custodial_ppts": t.ppts_in_custodial_by_team,
        "percent_of_discharges_with_mortality_in_30": t.percent_of_discharges_with_mortality_in_30_by_team,
        "mortality_within_30_days_of_discharge": t.mortality_within_30days_of_discharge_rate_by_team,
        "no_hosp_admission_since_enrollment": t.no_hosp_admission_since_enrollment_by_team,
        "er_only_visits": t.er_only_visits_by_team,
    }

    utl_team = t.loop_plot_team_df(t.ppts_on_team, params, freq=freq)
    
    utl_team.drop(
        [col for col in utl_team.columns if col != "month"], axis=1, inplace=True
    )

    for col_title, func in utilization.items():
        dff = t.loop_plot_team_df(func, params, freq=freq, col_suffix=f"_{col_title}")
        utl_team = utl_team.merge(dff, on="month", how="left")

    for col_title, func in utilization_need_args.items():
        for utilization in utilization_types:
            dff = t.loop_plot_team_df(
                func,
                params,
                freq=freq,
                additional_func_args=[utilization],
                col_suffix=f"_{utilization}{col_title}",
            )
            utl_team = utl_team.merge(dff, on="month", how="left")


    utl_team.to_csv(f"{processed_data}\\utl_team.csv", index=False)

    if freq == "QS":
        table_name = "team_utl_q"
    else:
        table_name = "team_utl"

    conn = sqlite3.connect(db_path)

    if update:
        stu.update_sql_table(utl_team, table_name, conn, ["month"], agg_table=True)

    else:
        stu.create_table(utl_team, table_name, conn, ["month"], agg_table=True)
    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\team_utilization_agg{str(pd.to_datetime('today').date())}.txt",
        "a",
    ).close()

    return utl_team


def create_team_info_agg_table(
    params=("2017-07-01", end_date), db_path=agg_db_path, freq="MS", update=True
):
    """
    Create an aggregate table of team information related values

    Loops through indicator/column name and matching function,
    creates a dataframe with a month column and value column
    and adds that value column to a master dataframe with a month column

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'
        db_path(str): path to the aggregate database
        freq(str): "MS" or "QS" indicates if values should be grouped monthly
            or quarterly
        update(bool): if the table being updated or created

    Returns:
        DataFrame: the created dataframe

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    t = Team()

    if str(update).lower() == "true":
        if freq == "MS":
            params = (t.last_month()[0], t.month_to_date()[1])
            update = True
        else:
            params = (t.last_quarter()[0], t.month_to_date()[1])
            update = True

    team_info = {
        "avg_age": t.avg_age_by_team,
        "percent_primary_non_english": t.percent_primary_non_english_by_team,
        "avg_years_enrolled": t.avg_years_enrolled_by_team,
        "ppts": t.ppts_on_team,
        "mortality": t.mortality_by_team,
    }
    team_info_df = t.loop_plot_team_df(t.ppts_on_team, params, freq=freq)
    
    team_info_df.drop(
        [col for col in team_info_df.columns if col != "month"], axis=1, inplace=True
    )

    for col_title, func in team_info.items():
        dff = t.loop_plot_team_df(func, params, freq=freq, col_suffix=f"_{col_title}")
        team_info_df = team_info_df.merge(dff, on="month", how="left")

    team_info_df.to_csv(f"{processed_data}\\team_info_df.csv", index=False)

    if freq == "QS":
        table_name = "team_info_q"
    else:
        table_name = "team_info"

    conn = sqlite3.connect(db_path)

    if update:
        stu.update_sql_table(team_info_df, table_name, conn, ["month"], agg_table=True)

    else:
        stu.create_table(team_info_df, table_name, conn, ["month"], agg_table=True)
    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\team_info_agg{str(pd.to_datetime('today').date())}.txt",
        "a",
    ).close()

    return team_info_df


def create_team_incidents_agg_table(
    params=("2017-07-01", end_date), db_path=agg_db_path, freq="MS", update=True
):
    """
    Create an aggregate table of team incidents values

    Loops through indicator/column name and matching function,
    creates a dataframe with a month column and value column
    and adds that value column to a master dataframe with a month column

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'
        db_path(str): path to the aggregate database
        freq(str): "MS" or "QS" indicates if values should be grouped monthly
            or quarterly
        update(bool): if the table being updated or created

    Returns:
        DataFrame: the created dataframe

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    t = Team()

    if str(update).lower() == "true":
        if freq == "MS":
            params = (t.last_month()[0], t.month_to_date()[1])
            update = True
        else:
            params = (t.last_quarter()[0], t.month_to_date()[1])
            update = True

    incident_types = ["burns", "falls", "infections", "med_errors", "wounds"]

    incidents = {
        "": t.total_incidents_by_team,
        "_per_100MM": t.incidents_per_member_by_team,
        "_unique_ppts": t.ppts_w_incident_by_team,
    }

    incidents_team = t.loop_plot_team_df(t.ppts_on_team, params, freq=freq)

    incidents_team.drop(
        [col for col in incidents_team.columns if col != "month"], axis=1, inplace=True
    )

    for col_title, func in incidents.items():
        for incident in incident_types:
            dff = t.loop_plot_team_df(
                func,
                params,
                freq=freq,
                additional_func_args=[incident],
                col_suffix=f"_{incident}{col_title}",
            )
            incidents_team = incidents_team.merge(dff, on="month", how="left")

    incidents_team.to_csv(f"{processed_data}\\incidents_team.csv", index=False)

    if freq == "QS":
        table_name = "team_incidents_q"
    else:
        table_name = "team_incidents"

    conn = sqlite3.connect(db_path)
    if update:
        stu.update_sql_table(
            incidents_team, table_name, conn, ["month"], agg_table=True
        )

    else:
        stu.create_table(incidents_team, table_name, conn, ["month"], agg_table=True)
    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\team_incidents_agg{str(pd.to_datetime('today').date())}.txt",
        "a",
    ).close()

    return incidents_team


def create_dc_attnd_table(params, freq):
    """
    Reads in daily census data spreadsheets and gets total
    or average values for the given params

    params(tuple): start date and end date in format 'YYYY-MM-DD'
    freq(str): "MS" or "QS" indicates if values should be grouped monthly
        or quarterly

    Returns:
        DataFrame: dataframe with average or summed data columns for each center

    """
    pvd_dc_attnd = pd.read_excel(
        daily_census_data, sheet_name="pvd", parse_dates=["date"]
    )
    woon_dc_attnd = pd.read_excel(
        daily_census_data, sheet_name="woon", parse_dates=["date"]
    )
    wes_dc_attnd = pd.read_excel(
        daily_census_data, sheet_name="wes", parse_dates=["date"]
    )

    pvd_dc_attnd = pvd_dc_attnd[
        (pvd_dc_attnd["date"] >= params[0]) & (pvd_dc_attnd["date"] <= params[1])
    ].copy()
    woon_dc_attnd = woon_dc_attnd[
        (woon_dc_attnd["date"] >= params[0]) & (woon_dc_attnd["date"] <= params[1])
    ].copy()
    wes_dc_attnd = wes_dc_attnd[
        (wes_dc_attnd["date"] >= params[0]) & (wes_dc_attnd["date"] <= params[1])
    ].copy()

    if freq == "QS":
        month_move = 3
    else:
        month_move = 1

    pvd_dc_attnd["month"] = (
        pvd_dc_attnd["date"] - pd.offsets.MonthBegin(month_move)
    ).dt.strftime("%Y-%m-%d")
    woon_dc_attnd["month"] = (
        woon_dc_attnd["date"] - pd.offsets.MonthBegin(month_move)
    ).dt.strftime("%Y-%m-%d")
    wes_dc_attnd["month"] = (
        wes_dc_attnd["date"] - pd.offsets.MonthBegin(month_move)
    ).dt.strftime("%Y-%m-%d")

    pvd_dc_attnd.drop("date", axis=1, inplace=True)
    woon_dc_attnd.drop("date", axis=1, inplace=True)
    wes_dc_attnd.drop("date", axis=1, inplace=True)

    pvd_dc_attnd["pace_cancelation_rate"] = (
        pvd_dc_attnd["p_cancelled"] / pvd_dc_attnd["p_scheduled"]
    )
    woon_dc_attnd["pace_cancelation_rate"] = (
        woon_dc_attnd["p_cancelled"] / woon_dc_attnd["p_scheduled"]
    )
    wes_dc_attnd["pace_cancelation_rate"] = (
        wes_dc_attnd["p_cancelled"] / wes_dc_attnd["p_scheduled"]
    )

    pvd_dc_attnd.columns = [
        f"pvd_{col}" if col != "month" else col for col in pvd_dc_attnd.columns
    ]
    woon_dc_attnd.columns = [
        f"woon_{col}" if col != "month" else col for col in woon_dc_attnd.columns
    ]
    wes_dc_attnd.columns = [
        f"wes_{col}" if col != "month" else col for col in wes_dc_attnd.columns
    ]

    pvd_group = pvd_dc_attnd.groupby("month").mean().reset_index()
    woon_group = woon_dc_attnd.groupby("month").mean().reset_index()
    wes_group = wes_dc_attnd.groupby("month").mean().reset_index()

    all_centers = pvd_group.merge(woon_group, on="month", how="left").merge(
        wes_group, on="month", how="left"
    )

    return all_centers


def create_center_agg_table(
    params=("2005-12-01", end_date), db_path=agg_db_path, freq="MS", update=True
):
    """
    Create an aggregate table of center related enrollment values

    Loops through indicator/column name and matching function,
    creates a dataframe with a month column and value column
    and adds that value column to a master dataframe with a month column

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'
        db_path(str): path to the aggregate database
        freq(str): "MS" or "QS" indicates if values should be grouped monthly
            or quarterly
        update(bool): if the table being updated or created

    Returns:
        DataFrame: the created dataframe

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    ce = CenterEnrollment()

    if str(update).lower() == "true":
        if freq == "MS":
            params = (ce.last_month()[0], ce.month_to_date()[1])
            update = True
        else:
            params = (ce.last_quarter()[0], ce.month_to_date()[1])
            update = True

    enrollment_funcs = {
        "_disenrolled": ce.disenrolled,
        "_voluntary_disenrolled": ce.voluntary_disenrolled,
        "_enrolled": ce.enrolled,
        "_deaths": ce.deaths,
    }

    center_shorthand_dict = {
        "Providence": "pvd",
        "Woonsocket": "woon",
        "Westerly": "wes",
    }

    enrollment_agg = ce.loop_plot_df(
        ce.census_on_end_date, params, freq=freq, additional_func_args=["Providence"]
    ).rename(columns={"Month": "month", "Value": "pvd_census"})

    dff = ce.loop_plot_df(
        ce.census_on_end_date, params, freq=freq, additional_func_args=["Woonsocket"]
    ).rename(columns={"Month": "month", "Value": "woon_census"})

    enrollment_agg = enrollment_agg.merge(dff, on="month", how="left")

    dff = ce.loop_plot_df(
        ce.census_on_end_date, params, freq=freq, additional_func_args=["Westerly"]
    ).rename(columns={"Month": "month", "Value": "wes_census"})

    enrollment_agg = enrollment_agg.merge(dff, on="month", how="left")

    for col_title, func in enrollment_funcs.items():
        for center, center_abr in center_shorthand_dict.items():
            dff = ce.loop_plot_df(
                func, params, freq=freq, additional_func_args=[center]
            ).rename(columns={"Month": "month", "Value": center_abr + col_title})
            enrollment_agg = enrollment_agg.merge(dff, on="month", how="left")

    dc_attendance = create_dc_attnd_table(params, freq)
    enrollment_agg = enrollment_agg.merge(dc_attendance, on="month", how="left")

    if freq == "QS":
        table_name = "center_enrollment_q"
    else:
        table_name = "center_enrollment"

    conn = sqlite3.connect(db_path)

    if update:
        stu.update_sql_table(
            enrollment_agg, table_name, conn, ["month"], agg_table=True
        )

    else:
        stu.create_table(enrollment_agg, table_name, conn, ["month"], agg_table=True)

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\center_agg{str(pd.to_datetime('today').date())}.txt",
        "a",
    ).close()

    return enrollment_agg


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update",
        default=True,
        help="Are we updating the database or creating it? True for update",
    )

    arguments = parser.parse_args()

    create_enrollment_agg_table(**vars(arguments))
    create_demographic_agg_table(**vars(arguments))

    create_incidents_agg_tables(incident_table="falls", **vars(arguments))
    create_incidents_agg_tables(incident_table="infections", **vars(arguments))
    create_incidents_agg_tables(incident_table="med_errors", **vars(arguments))
    create_incidents_agg_tables(incident_table="wounds", **vars(arguments))
    create_incidents_agg_tables(incident_table="burns", **vars(arguments))

    create_utilization_table(**vars(arguments))
    create_quality_agg_table(**vars(arguments))

    create_team_utl_agg_table(**vars(arguments))
    create_team_info_agg_table(**vars(arguments))
    create_team_incidents_agg_table(**vars(arguments))
    create_center_agg_table(**vars(arguments))

    create_enrollment_agg_table(freq="QS", **vars(arguments))
    create_demographic_agg_table(freq="QS", **vars(arguments))

    create_incidents_agg_tables(freq="QS", incident_table="falls", **vars(arguments))
    create_incidents_agg_tables(
        freq="QS", incident_table="infections", **vars(arguments)
    )
    create_incidents_agg_tables(
        freq="QS", incident_table="med_errors", **vars(arguments)
    )
    create_incidents_agg_tables(freq="QS", incident_table="wounds", **vars(arguments))
    create_incidents_agg_tables(freq="QS", incident_table="burns", **vars(arguments))

    create_utilization_table(freq="QS", **vars(arguments))
    create_quality_agg_table(freq="QS", **vars(arguments))

    create_team_utl_agg_table(freq="QS", **vars(arguments))
    create_team_info_agg_table(freq="QS", **vars(arguments))
    create_team_incidents_agg_table(freq="QS", **vars(arguments))
    create_center_agg_table(freq="QS", **vars(arguments))

    print("Complete")
