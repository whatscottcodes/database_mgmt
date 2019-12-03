import argparse

from process_db_data import (
    process_addresses,
    process_admission_claims,
    process_alfs,
    process_appointments,
    process_authorizations,
    process_burns,
    process_center_days,
    process_demographics,
    process_detail_claims,
    process_dx,
    process_enrollment,
    process_er_only,
    process_falls,
    process_infections,
    process_influenza,
    process_inpatient,
    process_med_errors,
    process_medications,
    process_payments,
    process_pneumococcal,
    process_referrals,
    process_quick_list,
    process_wounds,
)

from data_to_sql import (
    addresses_to_sql,
    admission_claims_to_sql,
    alfs_to_sql,
    appts_to_sql,
    auths_to_sql,
    burns_to_sql,
    center_days_to_sql,
    claims_detail_to_sql,
    demographics_to_sql,
    dx_to_sql,
    enrollment_to_sql,
    er_only_to_sql,
    falls_to_sql,
    infections_to_sql,
    influ_to_sql,
    inpatient_to_sql,
    med_errors_to_sql,
    meds_to_sql,
    monthly_census_to_sql,
    payments_to_sql,
    pnuemo_to_sql,
    ppts_to_sql,
    referrals_to_sql,
    wounds_to_sql,
)
from get_file_functions import choose_file_to_get

### The dictionary below takes the database table name
### and maps it to the arguments for the get_file_func
### and the related process_data and to_sql functions
### that are used to update the table

table_name_to_funcs = {
    "addresses": {
        "file_type": "xls",
        "filename": ["addresses"],
        "process": [process_addresses.process_addresses],
        "to_sql": addresses_to_sql.addresses_to_sql,
    },
    "admission_claims": {
        "file_type": "csv",
        "filename": ["PCMClaimAdmissionDischarge"],
        "process": [process_admission_claims.process_admission_claims],
        "to_sql": admission_claims_to_sql.admission_claims_to_sql,
    },
    "alfs": {
        "file_type": "csv",
        "filename": ["Admission Changes"],
        "process": [process_alfs.process_alfs],
        "to_sql": alfs_to_sql.alfs_to_sql,
    },
    "appointments": {
        "file_type": "xls",
        "filename": ["appts"],
        "process": [process_appointments.process_appointments],
        "to_sql": appts_to_sql.appts_to_sql,
    },
    "authorizations": {
        "file_type": "authorizations",
        "filename": [""],
        "process": [process_authorizations.process_authorizations],
        "to_sql": auths_to_sql.auths_to_sql,
    },
    "burns": {
        "file_type": "csv",
        "filename": ["incident_Burns"],
        "process": [process_burns.process_burns],
        "to_sql": burns_to_sql.burns_to_sql,
    },
    "center_days": {
        "file_type": "csv",
        "filename": ["ParticipantCenterDays"],
        "process": [process_center_days.process_center_days],
        "to_sql": center_days_to_sql.center_days_to_sql,
    },
    "claims_detail": {
        "file_type": "claim details",
        "filename": [""],
        "process": [process_detail_claims.process_detail_claims],
        "to_sql": claims_detail_to_sql.claims_detail_to_sql,
    },
    "demographics": {
        "file_type": "xls",
        "filename": ["demographics"],
        "process": [process_demographics.process_demographics],
        "to_sql": demographics_to_sql.demographics_to_sql,
    },
    "dx": {
        "file_type": "csv",
        "filename": ["EmrDroppedHcc_nc", "EmrDroppedHcc"],
        "process": [process_dx.process_dx],
        "to_sql": dx_to_sql.dx_to_sql,
    },
    "enrollment": {
        "file_type": "csv",
        "filename": [
            "ParticipantEnrollmentDisenrollmentDetail",
            "ParticipantQuickList",
        ],
        "process": [
            process_quick_list.process_quick_list,
            process_enrollment.process_enrollment,
        ],
        "to_sql": enrollment_to_sql.enrollment_to_sql,
    },
    "er_only": {
        "file_type": "csv",
        "filename": ["ServiceUtilizationEmergency"],
        "process": [process_er_only.process_er_only],
        "to_sql": er_only_to_sql.er_only_to_sql,
    },
    "falls": {
        "file_type": "csv",
        "filename": ["incident_Falls"],
        "process": [process_falls.process_falls],
        "to_sql": falls_to_sql.falls_to_sql,
    },
    "infections": {
        "file_type": "csv",
        "filename": ["incident_Infection"],
        "process": [process_infections.process_infections],
        "to_sql": infections_to_sql.infections_to_sql,
    },
    "influ": {
        "file_type": "xls",
        "filename": ["influ", "influ_contra"],
        "process": [process_influenza.process_influenza],
        "to_sql": influ_to_sql.influ_to_sql,
    },
    "inpatient": {
        "file_type": "csv",
        "filename": ["ServiceUtilizationInpatient", "ServiceUtilizationEmergency_IP"],
        "process": [process_inpatient.process_inpatient],
        "to_sql": inpatient_to_sql.inpatient_to_sql,
    },
    "med_errors": {
        "file_type": "csv",
        "filename": ["incident_Med Errors"],
        "process": [process_med_errors.process_med_errors],
        "to_sql": med_errors_to_sql.med_errors_to_sql,
    },
    "medications": {
        "file_type": "xls",
        "filename": ["meds"],
        "process": [process_medications.process_medications],
        "to_sql": meds_to_sql.meds_to_sql,
    },
    "monthly_census": {
        "file_type": "csv",
        "filename": [
            "ParticipantEnrollmentDisenrollmentDetail",
            "ParticipantQuickList",
        ],
        "process": [
            process_quick_list.process_quick_list,
            process_enrollment.process_enrollment,
        ],
        "to_sql": monthly_census_to_sql.monthly_census_to_sql,
    },
    "payments": {
        "file_type": "csv",
        "filename": ["PCMPaymentRegister"],
        "process": [process_payments.process_payments],
        "to_sql": payments_to_sql.payments_to_sql,
    },
    "pneumo": {
        "file_type": "xls",
        "filename": ["pneumo"],
        "process": [process_pneumococcal.process_pneumococcal],
        "to_sql": pnuemo_to_sql.pnuemo_to_sql,
    },
    "ppts": {
        "file_type": "csv",
        "filename": [
            "ParticipantEnrollmentDisenrollmentDetail",
            "ParticipantQuickList",
        ],
        "process": [
            process_quick_list.process_quick_list,
            process_enrollment.process_enrollment,
        ],
        "to_sql": ppts_to_sql.ppts_to_sql,
    },
    "referrals": {
        "file_type": "csv",
        "filename": ["ReferralDetail"],
        "process": [process_referrals.process_referrals],
        "to_sql": referrals_to_sql.referrals_to_sql,
    },
    "wounds": {
        "file_type": "csv",
        "filename": ["wound_grid"],
        "process": [process_wounds.process_wounds],
        "to_sql": wounds_to_sql.wounds_to_sql,
    },
}


def update_table(table_name):
    """
    Retrieves the related files from the EHR for DB folder
    Processes the data using the table's process_data function
    Updates table using the table's to_sql function

    Args:
        table_name(str): table to be updated
    """
    for filename in table_name_to_funcs[table_name]["filename"]:
        choose_file_to_get(table_name_to_funcs[table_name]["file_type"], filename)

    for process_func in table_name_to_funcs[table_name]["process"]:
        process_func()

    table_name_to_funcs[table_name]["to_sql"](update=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--table_name", help="Name of table to update")

    arguments = parser.parse_args()

    update_table(**vars(arguments))
