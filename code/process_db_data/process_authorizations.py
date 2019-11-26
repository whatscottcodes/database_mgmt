import pandas as pd
from process_db_data.data_cleaning_utils import clean_table_columns
from file_paths import processed_data, raw_data


def process_authorizations():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Approval Expiration Dates with year 9999
        are replaced with an empty string
    Column names are cleaned
    Auth and Service codes are replaced with descriptions
    Referring provider first and last name are merged into one column
    Referred to provider first and last name are merged into one column
    Leading and trailing spaces are stripped from vendor column
    Indicated columns are dropped
    NA member values are dropped
    Training member is dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    auths = pd.read_csv(
        f"{raw_data}\\auths.csv",
        parse_dates=[
            "EnteredDate",
            "ModifiedDate",
            "ApprovalEffectiveDate",
            "ExpectedServiceDate",
        ],
    )
    drop_cols = [
        "Source",
        "EnteredBy",
        "ModifiedBy",
        "ParticipantFirstName",
        "ParticipantLastName",
        "ReferredToProviderOfService",
        "Hours",
        "Frequency",
        "LengthOfStay",
    ]

    auths.drop(drop_cols, axis=1, inplace=True)

    replace_outside_dates = {}
    for outside_date in auths["ApprovalExpirationDate"].unique():
        if "9999" in outside_date:
            replace_outside_dates[outside_date] = ""

    auths.dropna(subset=["MemberId"], inplace=True)
    auths["MemberId"] = auths["MemberId"].astype(int)

    auths["ApprovalExpirationDate"].replace(replace_outside_dates, inplace=True)
    auths["ApprovalExpirationDate"] = pd.to_datetime(auths["ApprovalExpirationDate"])

    auth_types = {
        "DM": "DME",
        "DY": "Dialysis",
        "ER": "Emergency Room",
        "HH": "Home Care",
        "HO": "Hospice",
        "IN": "Inpatient",
        "OP": "Outpatient",
        "OT": "Other",
        "PR": "Personal Emergency Response",
        "TR": "Transportation",
    }

    service_types = {
        "OR20": "Podiatry",
        "DE10": "Dental",
        "XR10": "Radiology (General)",
        "CR11": "Cardiology",
        "VS10": "Ophthalmology",
        "UR10": "Urology",
        "VS20": "Optometry",
        "OA10": "Other",
        "XR40": "Radiology Mammography",
        "GA10": "Gastroenterology",
        "GV10": "Office Visit",
        "SG10": "Surgeon",
        "NE10": "Nephrology",
        "PH80": "Hematology/Oncology",
        "OR10": "Orthopedics",
        "XR30": "Radiology - Cat Scan",
        "BH10": "Psychiatry",
        "VS50": "Vision Hardware",
        "XR50": "Radiology - MRI",
        "XR90": "Radiology - Ultrasound",
        "DM10": "DME - Purchase",
        "SK10": "Dermatology",
        "NE20": "Neurology",
        "OU20": "Radiology Tests",
        "PC20": "Primary Care",
        "DE50": "Dental Hardware",
        "BH30": "Psychology",
        "WO20": "Wounds Care",
        "EM10": "Emergency Dept Visit",
        "EM40": "Observation Management",
        "IN10": "Inpatient Hospital",
        "XR55": "Outpatient MRI/CT",
        "SG20": "Outpatient Surgery",
        "VS30": "Ophthalmology - Special Services",
        "BH11": "Inpatient Psychiatric Unit/Facility",
        "SG50": "Surgery Services",
        "PH70": "Phys. Svc. - Inpatient/Outpatient Medical Special",
        "PC10": "Phys. Svc. - Other",
        "XR60": "Radiology - Diagnostic Nuclear Medicine",
        "XR80": "Radiation Therapy",
        "BH20": "Health and Behavior Assessment",
        "IN20": "Inpatient Medical Specialist",
        "CA20": "Cancer Center",
        "PU20": "Phys. Svc. - Pulmonology",
        "LB10": "Pathology and Laboratory",
        "RD10": "Chemotherapy",
        "SP20": "In-Home Medical Supplies",
        "DM00": "Oxygen",
        "DM15": "DME - Rental",
        "SP30": "Medical Supplies General",
        "AU20": "Phys. Svc. - Audiology",
        "AU50": "Speech & Hearing Hardware",
        "DY10": "Dialysis",
        "SN10": "Phys. Svc. - SNF",
        "CR10": "Cardiovascular",
        "NU20": "Nurse Practioner",
        "HHRN": "Home Health RN Services",
        "HO10": "Hospice Services",
        "HO20": "Hospice - Hospital or LTC",
        "DM60": "Orthotic Procedures and Devices",
        "DM30": "Prosthetic Procedures and Devices",
        "AU10": "Otorhinolaryngologic Services",
        "HM40": "Housekeeping and Chore Services",
        "GY10": "Phys. Svc. - Gynecology",
        "RD20": "EKG",
        "PS10": "Purchase Service Other",
        "HM20": "In-Home - Personal Care/Home Chore",
        "HH53": "In-Home Supportive Care",
        "HM30": "Chore Services",
        "HH20": "Personal Care Assistant - PCA",
        "TR10": "Emergency Transportation",
        "HH50": "Home Health Aide - HHA",
        "AL10": "Assisted Living - Permanent Placement",
        "NH60": "Nursing Home - Skilled Nursing",
        "NH30": "Nursing Home - Permanent Placement",
        "NH50": "Nursing Home - Respite/Temporary",
        "NH20": "Nursing Home - Inpatient",
        "ME10": "Meals & Distribution",
        "HH51": "In-Home - Supportive Care",
        "TR50": "Non Emergent Transportation",
        "PC11": "Physician Fees - Other",
        "PT20": "Physical Therapy - Normal (Non-In Home)",
        "AD15": "Adult Day Center Attendance",
        "PR20": "Prescription Medications",
        "PR10": "Personal Alarm/Response System",
        "TR60": "Transportation - Other",
        "AL20": "Assisted Living - Respite/Temporary",
        "PT10": "Physical Therapy - In Home",
        "OT10": "Occupational Therap - In Home",
        "EM30": "Phys. Svc - Emergency Room",
        "ME20": "Meals - In Home",
        "TR70": "Transportation - Ambulette",
        "HH80": "Home Health Other",
        "SP40": "Medical Supplies General (House Stock)",
        "GV20": "Phys. Svc - Admin",
        "HH30": "Homemaker",
        "AN10": "Phys. Svc - Anesthesiology",
        "MS10": "Medical Services",
        "MH10": "Purchase Service MH/MR",
        "AL30": "Assisted Living - Observation",
        "ET10": "Enteral and Parenteral Therapy",
        "RBMS": "R&B Medical/Surgical",
        "PY10": "Psychotherapy",
        "EM20": "Emergency Room Out of Area",
    }

    auths.columns = clean_table_columns(auths.columns)

    auths["authorization_type"].replace(auth_types, inplace=True)
    auths["service_type"].replace(service_types, inplace=True)

    auths["referring_provider"] = (
        auths["referring_provider_first_name"].fillna("")
        + " "
        + auths["referring_provider_last_name"].fillna("")
    )

    auths["vendor"] = (
        auths["referred_to_provider_first_name"].fillna("")
        + " "
        + auths["referred_to_provider_last_name"].fillna("")
    )

    auths["vendor"] = auths["vendor"].str.strip()

    combined_cols_to_drop = [
        "referring_provider_first_name",
        "referring_provider_last_name",
        "referred_to_provider_first_name",
        "referred_to_provider_last_name",
    ]

    auths.drop(combined_cols_to_drop, axis=1, inplace=True)

    auths.dropna(subset=["member_id"], inplace=True)
    auths = auths[auths.member_id != 1003]

    auths.reset_index(drop=True, inplace=True)
    auths.to_csv(f"{processed_data}\\auths.csv", index=False)


if __name__ == "__main__":
    process_authorizations()
