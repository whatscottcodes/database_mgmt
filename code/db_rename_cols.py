db_name = "V:\\Databases\\PaceDashboard.db"
report_db = "V:\\Databases\\reporting.db"
statewide_geocoding = "C:\\Users\\snelson\\data\\statewide.csv"
non_geopy_addresses = "C:\\Users\\snelson\\data\\addresses_to_parse\\tough_adds.csv"
ehr_file_location = "C:\\Users\\snelson\\data\\ehr_for_db\\"

address_rename_dict = {
    "MemberID": "member_id",
    "Address1": "address",
    "City": "city",
    "State": "state",
    "Zip": "zip",
}

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

address_drop_cols = ["LastName", "FirstName", "Center", "Facility", "Address2", "Phone"]


# demo_cols = ['member_id', 'dob', 'ethnicity', 'race', 'language', 'gender']

# these columns need to be dropped in each incident report
common_incident_drop = ["First Name", "Last Name", "Submitted By", "Center"]
# these are the additional columns that should be dropped from each Cognify report
# note: wounds is our own grid, so we can do with it as we please
incident_drop_cols = {
    "burns": common_incident_drop + ["Control Number", "Description"],
    "falls": common_incident_drop + ["Control Number"],
    "infections": common_incident_drop,
    "med_errors": common_incident_drop + ["Control Number"],
    "wounds": ["Participant"],
}

vacc_rename_dict = {
    "Patient: Patient ID": "member_id",
    "Immunization: Date Administered": "date_administered",
}

# names of inpatient Cognify cols
inpatient_rename_dict = {
    "MemberID": "member_id",
    "AdmissionDate": "admission_date",
    "DischargeDate": "discharge_date",
    "LOSDays": "los",
    "FacilityName": "facility",
    "DischargeDisposition": "discharge_reason",
    "AdmitReason": "admit_reason",
    "AdmissionType": "admission_type",
}
# cols to drop from inpatient Cognify dataframe
inpatient_drop = [
    "textBox5",
    "Center",
    "ParticipantName",
    "PCP",
    "ICUDays",
    "Diagnosis",
    "Readmit",
    "AdmissionScheduled",
]
# names of er Cognify cols
er_rename_dict = {
    "MemberID": "member_id",
    "AdmissionDate": "admission_date",
    "Facility": "facility",
}
# cols to drop from er Cognify dataframe
er_drop_cols = ["textBox5", "textBox2", "Center", "ParticipantName", "PCP", "Diagnosis"]
# cols not needed in UR Grid
utl_drop_cols = [
    "Ppt Name",
    "Member ID",
    "Date of visit",
    "Date of Discharge",
    "# of days hospitalized",
    "Last MD visit",
    "Last RN visit",
    "Hospital",
    "CMS Preventable",
    "if yes, what was done",
    "Possible interventions that could have prevented visit",
    "Additional interventions 1",
    "Additional interventions 2",
    "Additional interventions 3",
    "Comments",
    "End of life",
]
utl_rename_dict = {
    "# of days prior to hospital visit": "days_MD",
    "# of days prior to hospital visit.1": "days_RN",
    "Visit type": "visit_type",
    "Time of visit": "time",
    "Lives": "living_situation",
    "Adm from": "admitted_from",
    "Facilty adm from": "admitting_facility",
    "Reason for visit": "reason",
    "Discharge Diagnosis": "discharge_dx",
    "Related to": "related",
    "Sent by On-call staff": "sent_by_oc",
    "PACE staff aware of S/S prior": "aware_ss",
    "PACE aware of visit prior": "aware_visit",
    "Preventable/Avoidable": "preventable",
}

enrollment_drop = ["ParticipantName", "Gender", "SSN", "DeathDate"]
enrollment_rename_dict = {
    "Center": "center",
    "MemberID": "member_id",
    "Medicare": "medicare",
    "Medicaid": "medicaid",
    "EnrollmentDate": "enrollment_date",
    "DisenrollmentDate": "disenrollment_date",
    "Other": "disenroll_reason",
}

center_days_drop = ["ParticipantName", "Center", "TimeAttending"]
center_days_rename_dict = {"MemberID": "member_id", "CenterDays": "days"}

dx_cols = ["member_id", "dx_desc", "icd10"]

dx_drop_cols = ["dx_doc"]

demographics_drop = ["first", "last"]

