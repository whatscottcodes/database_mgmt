#!/usr/bin/env python3

import luigi
import shutil
import os
import time
import pandas as pd
import glob
from file_paths import (
    ehr_file_location,
    raw_data,
    processed_data,
    database_path,
    archive_data,
    databases_folder,
    update_logs_folder,
    luigi_log
)
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
    process_grievances,
    process_incidents,
    process_infections,
    process_influenza,
    process_inpatient,
    process_med_errors,
    process_medications,
    process_payments,
    process_pneumococcal,
    process_quick_list,
    process_referrals,
    process_wounds,
)

from data_to_sql import (
    addresses_to_sql,
    admission_claims_to_sql,
    alfs_to_sql,
    appts_to_sql,
    auths_to_sql,
    burns_to_sql,
    centers_to_sql,
    center_days_to_sql,
    claims_detail_to_sql,
    demographics_to_sql,
    dx_to_sql,
    enrollment_to_sql,
    er_only_to_sql,
    falls_to_sql,
    grievances_to_sql,
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
    teams_to_sql,
    wounds_to_sql,
)

import agg_table_functions as atf

class GetCognifyFile(luigi.Task):
    cognify_filepath = luigi.Parameter(default="")
    new_filepath = luigi.Parameter(default="")

    def output(self):
        return luigi.LocalTarget(str(self.new_filepath))

    def run(self):
        shutil.copy2(str(self.cognify_filepath), str(self.new_filepath))


class GetAuthorizations(luigi.Task):
    try:
        ehr_file = [x for x in os.listdir(ehr_file_location) if "PRI_auth" in x][0]
    except IndexError:
        print("Auths file missing")
        ehr_file = ""
    cognify_filepath = luigi.Parameter(
        default=f"{ehr_file_location}\\{ehr_file}"
    )

    new_filepath = luigi.Parameter(default=f"{raw_data}\\auths.csv")

    def output(self):
        return luigi.LocalTarget(str(self.new_filepath))

    def run(self):
        shutil.copy2(str(self.cognify_filepath), str(self.new_filepath))

class GetPSFile(luigi.Task):
    ps_filepath = luigi.Parameter(default="")
    new_filepath = luigi.Parameter(default="")

    def output(self):
        return luigi.LocalTarget(str(self.new_filepath))

    def run(self):
        data_xls = pd.read_excel(self.ps_filepath, index_col=None)
        data_xls.to_csv(self.new_filepath, encoding="utf-8", index=False)

class GetClaimsDetails(luigi.Task):
    try:
        ehr_file = [x for x in os.listdir(ehr_file_location) if "ClaimDetail_PRI" in x][0]
    except IndexError:
        print("Claim file missing")
        ehr_file = ""
    ehr_file_name = luigi.Parameter(
        default=ehr_file
    )

    new_filepath = luigi.Parameter(default=f"{raw_data}\\claims_detail.csv")

    def output(self):
        return luigi.LocalTarget(str(self.new_filepath))

    def run(self):
        data_xls = pd.read_excel(
            f"{ehr_file_location}\\{self.ehr_file_name}", header=4, index_col=None
        )
        data_xls.to_csv(self.new_filepath, encoding="utf-8", index=False)


class ProcessAddresses(luigi.Task):
    new_filename = f"{processed_data}\\addresses.csv"

    def requires(self):
        return GetPSFile(ps_filepath=f"{ehr_file_location}\\addresses.xls",
        new_filepath=f"{raw_data}\\addresses.csv")

    def output(self):
        return luigi.LocalTarget(self.new_filename)

    def run(self):
        return process_addresses.process_addresses()

class ProcessAlfs(luigi.Task):
    new_filename = f"{processed_data}\\alfs.csv"

    def requires(self):
        return GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\Admission Changes.csv",
                new_filepath=f"{raw_data}\\alfs.csv",
            )

    def output(self):
        return luigi.LocalTarget(self.new_filename)

    def run(self):
        return process_alfs.process_alfs()

class ProcessAppts(luigi.Task):
    new_filename = f"{processed_data}\\appts.csv"

    def requires(self):
        return GetPSFile(ps_filepath=f"{ehr_file_location}\\appts.xls", new_filepath=f"{raw_data}\\appts.csv")

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_appointments.process_appointments()

class ProcessAuths(luigi.Task):
    new_filename = f"{processed_data}\\auths.csv"

    def requires(self):
        return GetAuthorizations()

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_authorizations.process_authorizations()

class ProcessBurns(luigi.Task):
    new_filename = f"{processed_data}\\burns.csv"

    def requires(self):
        return GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\incident_Burns.csv",
                new_filepath=f"{raw_data}\\burns.csv",
            )

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_burns.process_burns()

class ProcessCenterDays(luigi.Task):
    new_filename = f"{processed_data}\\center_days.csv"

    def requires(self):
        return GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\ParticipantCenterDays.csv",
                new_filepath=f"{raw_data}\\center_days.csv",
            )

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_center_days.process_center_days()
    
class ProcessAdmitClaims(luigi.Task):
    new_filename = f"{processed_data}\\admit_claims.csv"

    def requires(self):
        return [GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\PCMClaimAdmissionDischarge.csv",
                new_filepath=f"{raw_data}\\admit_claims.csv",
            )]

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_admission_claims.process_admission_claims()

class ProcessClaimsDetails(luigi.Task):
    new_filename = f"{processed_data}\\claims_detail.csv"

    def requires(self):
        return [GetClaimsDetails()]

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_detail_claims.process_detail_claims()

class ProcessDemographics(luigi.Task):
    new_filename = f"{processed_data}\\demographics.csv"

    def requires(self):
        return GetPSFile(ps_filepath=f"{ehr_file_location}\\demographics.xls", new_filepath=f"{raw_data}\\demographics.csv")

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_demographics.process_demographics()

class ProcessDx(luigi.Task):
    new_filename = f"{processed_data}\\dx.csv"

    def requires(self):
        return [GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\EmrDroppedHcc.csv",
                new_filepath=f"{raw_data}\\dx_current.csv",
            ), GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\EmrDroppedHcc_nc.csv",
                new_filepath=f"{raw_data}\\dx_not_current.csv",
            )]

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_dx.process_dx()

class ProcessEnrollment(luigi.Task):

    def requires(self):
        return [GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\ParticipantEnrollmentDisenrollmentDetail.csv",
                new_filepath=f"{raw_data}\\enrollment.csv",
            ),
            GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\ParticipantTransfer.csv",
                new_filepath=f"{raw_data}\\transfers.csv",
            )
        ]

    def output(self):
        return [
                luigi.LocalTarget(f"{processed_data}\\{filename}.csv")
                for filename in ["enrollment", "ppts", "centers"]
            ]
    
    def run(self):
        return process_enrollment.process_enrollment()

class ProcessEROnly(luigi.Task):

    def requires(self):
        return [ProcessEnrollment(),
        #ProcessClaims(),
        #ParseUtilizationGrid(),
        GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\ServiceUtilizationEmergency.csv",
                new_filepath=f"{raw_data}\\er_only.csv",
            )        
        ]

    def output(self):
        return [
                luigi.LocalTarget(f"{processed_data}\\{filename}.csv")
                for filename in ["er_only"]
            ]
    
    def run(self):
        return process_er_only.process_er_only(update=False)

class ProcessFalls(luigi.Task):
    new_filename = f"{processed_data}\\falls.csv"

    def requires(self):
        return GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\incident_Falls.csv",
                new_filepath=f"{raw_data}\\falls.csv",
            )

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_falls.process_falls()

class ProcessGrievances(luigi.Task):
    new_filename = f"{processed_data}\\grievances.csv"

    def requires(self):
        return GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\grievances resolved 1-31-19  and later.csv",
                new_filepath=f"{raw_data}\\grievances.csv",
            )

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_grievances.process_grievances()

class ProcessInfections(luigi.Task):
    new_filename = f"{processed_data}\\infections.csv"

    def requires(self):
        return GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\incident_Infection.csv",
                new_filepath=f"{raw_data}\\infections.csv",
            )

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_infections.process_infections()

class ProcessInflu(luigi.Task):
    new_filename = f"{processed_data}\\influ.csv"

    def requires(self):
        return [GetPSFile(ps_filepath=f"{ehr_file_location}\\influ.xls", new_filepath=f"{raw_data}\\influ.csv"),
            GetPSFile(ps_filepath=f"{ehr_file_location}\\influ_contra.xls", new_filepath=f"{raw_data}\\influ_contra.csv")]

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_influenza.process_influenza()

class ProcessInpatient(luigi.Task):

    def requires(self):
        return [ProcessEnrollment(),
            GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\ServiceUtilizationInpatient.csv",
                new_filepath=f"{raw_data}\\inpatient.csv",
            ),
            GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\ServiceUtilizationEmergency_IP.csv",
                new_filepath=f"{raw_data}\\er_adm.csv",
            )]

    def output(self):
        return luigi.LocalTarget(f"{processed_data}\\inpatient.csv")
    
    def run(self):
        return process_inpatient.process_inpatient(update=False)

class ProcessMedErrors(luigi.Task):
    new_filename = f"{processed_data}\\med_errors.csv"

    def requires(self):
        return GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\incident_Med Errors.csv",
                new_filepath=f"{raw_data}\\med_errors.csv",
            )

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_med_errors.process_med_errors()

class ProcessMeds(luigi.Task):
    new_filename = f"{processed_data}\\meds.csv"

    def requires(self):
        return GetPSFile(ps_filepath=f"{ehr_file_location}\\meds.xls", new_filepath=f"{raw_data}\\meds.csv")

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_medications.process_medications()
    
class ProcessPayments(luigi.Task):
    new_filename = f"{processed_data}\\payments.csv"

    def requires(self):
        return [GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\PCMPaymentRegister.csv",
                new_filepath=f"{raw_data}\\payments.csv",
            )]

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_payments.process_payments()

class ProcessPneumo(luigi.Task):
    new_filename = f"{processed_data}\\pneumo.csv"

    def requires(self):
        return [GetPSFile(ps_filepath=f"{ehr_file_location}\\pneumo.xls", 
        new_filepath=f"{raw_data}\\pneumo.csv"),
            GetPSFile(ps_filepath=f"{ehr_file_location}\\pneumo_contra.xls", 
            new_filepath=f"{raw_data}\\pneumo_contra.csv")]

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_pneumococcal.process_pneumococcal()

class ProcessQuickList(luigi.Task):
    new_filename = f"{processed_data}\\teams.csv"

    def requires(self):
        return [ProcessEnrollment(), GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\ParticipantQuickList.csv",
                new_filepath=f"{raw_data}\\ppt_quick_list.csv",
            )]

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_quick_list.process_quick_list(update=False)

class ProcessReferrals(luigi.Task):
    new_filename = f"{processed_data}\\referrals.csv"

    def requires(self):
        return GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\ReferralDetail.csv",
                new_filepath=f"{raw_data}\\referrals.csv",
            )

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_referrals.process_referrals()

class ProcessWounds(luigi.Task):
    new_filename = f"{processed_data}\\wounds.csv"

    def requires(self):
        return GetCognifyFile(
                cognify_filepath=f"{ehr_file_location}\\wound_grid.csv",
                new_filepath=f"{raw_data}\\wounds.csv",
            )

    def output(self):
        return luigi.LocalTarget(self.new_filename)
    
    def run(self):
        return process_wounds.process_wounds()


class PptsToSQL(luigi.Task):

    log_file = f"{update_logs_folder}\\ppts_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return ProcessEnrollment()

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        ppts_to_sql.ppts_to_sql(update=False)


class InpatientToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\inpatient_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessInpatient(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        inpatient_to_sql.inpatient_to_sql(update=False)


class AddressesToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\addresses_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessAddresses(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        addresses_to_sql.addresses_to_sql(update=False)

class ALfsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\alfs_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessAlfs(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        alfs_to_sql.alfs_to_sql(update=False)

class ApptsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\appointments_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessAppts(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        appts_to_sql.appts_to_sql(update=False)

class AuthsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\authorizations_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessAuths(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        auths_to_sql.auths_to_sql(update=False)

class BurnsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\burns_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessBurns(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        burns_to_sql.burns_to_sql(update=False)

class AdmitClaimsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\admission_claims_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessAdmitClaims(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        admission_claims_to_sql.admission_claims_to_sql(update=False)

class ClaimsDetailsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\claims_detail_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessClaimsDetails(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        claims_detail_to_sql.claims_detail_to_sql(update=False)

class PaymentsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\payments_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessPayments(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        payments_to_sql.payments_to_sql(update=False)

class CentersToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\centers_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessEnrollment(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        centers_to_sql.centers_to_sql(update=False)

class CenterDaysToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\center_days_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessCenterDays(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        center_days_to_sql.center_days_to_sql(update=False)

class DemographicsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\demographics_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessDemographics(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        demographics_to_sql.demographics_to_sql(update=False)


class DxToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\dx_{str(pd.to_datetime('today').date())}.txt"
    def requires(self):
        return [ProcessDx(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        dx_to_sql.dx_to_sql(update=False)


class EnrollmentToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\enrollment_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessEnrollment(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        enrollment_to_sql.enrollment_to_sql(update=False)


class EROnlyToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\er_only_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessEROnly(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        er_only_to_sql.er_only_to_sql(update=False)


class FallsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\falls_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessFalls(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        falls_to_sql.falls_to_sql(update=False)


class GrievancesToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\grievances_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessGrievances(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        grievances_to_sql.grievances_to_sql(update=False)


class InfectionsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\infections_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessInfections(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        infections_to_sql.infections_to_sql(update=False)


class InfluToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\influ_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessInflu(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        influ_to_sql.influ_to_sql(update=False)


class MedErrorsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\med_errors_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessMedErrors(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        med_errors_to_sql.med_errors_to_sql(update=False)

class MedsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\meds_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessMeds(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        meds_to_sql.meds_to_sql(update=False)

class PnuemoToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\pnuemo_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessPneumo(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        pnuemo_to_sql.pnuemo_to_sql(update=False)

class ReferralsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\referrals_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        referrals_to_sql.referrals_to_sql(update=False)

class TeamsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\teams_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessEnrollment(), ProcessQuickList(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        teams_to_sql.teams_to_sql(update=False)

class WoundsToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\wounds_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [ProcessWounds(), PptsToSQL()]

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        wounds_to_sql.wounds_to_sql(update=False)


class CensusToSQL(luigi.Task):
    log_file = f"{update_logs_folder}\\monthly_census_{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return EnrollmentToSQL()

    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        monthly_census_to_sql.monthly_census_to_sql(update=False)

class EnrollmentAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\enrollment_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(), CensusToSQL(), ReferralsToSQL()]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_enrollment_agg_table(update=False)
        atf.create_enrollment_agg_table(update=False, freq="QS")

class DemographicAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\demographic_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        DxToSQL(),
        ClaimsDetailsToSQL(),
        AdmitClaimsToSQL(),
        DemographicsToSQL(),
        CensusToSQL()]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_demographic_agg_table(update=False)
        atf.create_demographic_agg_table(update=False, freq="QS")

class FallsAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\falls_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        CensusToSQL(),
        BurnsToSQL(),
        FallsToSQL(),
        InfectionsToSQL(),
        MedErrorsToSQL(),
        WoundsToSQL()
        ]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_incidents_agg_tables(update=False, incident_table="falls")
        atf.create_incidents_agg_tables(update=False, incident_table="falls", freq="QS")

class InfectionsAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\infections_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        CensusToSQL(),
        BurnsToSQL(),
        FallsToSQL(),
        InfectionsToSQL(),
        MedErrorsToSQL(),
        WoundsToSQL()
        ]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_incidents_agg_tables(update=False, incident_table="infections")
        atf.create_incidents_agg_tables(update=False, incident_table="infections", freq="QS")

class MedErrorsAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\med_errors_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        CensusToSQL(),
        BurnsToSQL(),
        FallsToSQL(),
        InfectionsToSQL(),
        MedErrorsToSQL(),
        WoundsToSQL()
        ]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_incidents_agg_tables(update=False, incident_table="med_errors")
        atf.create_incidents_agg_tables(update=False, incident_table="med_errors", freq="QS")

class WoundsAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\wounds_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        CensusToSQL(),
        BurnsToSQL(),
        FallsToSQL(),
        InfectionsToSQL(),
        MedErrorsToSQL(),
        WoundsToSQL()
        ]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_incidents_agg_tables(update=False, incident_table="wounds")
        atf.create_incidents_agg_tables(update=False, incident_table="wounds", freq="QS")

class BurnsErrorsAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\burns_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        CensusToSQL(),
        BurnsToSQL(),
        FallsToSQL(),
        InfectionsToSQL(),
        MedErrorsToSQL(),
        WoundsToSQL()
        ]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_incidents_agg_tables(update=False, incident_table="burns")
        atf.create_incidents_agg_tables(update=False, incident_table="burns", freq="QS")

class UtilizationAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\utilization_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        CensusToSQL(),
        ClaimsDetailsToSQL(),
        AdmitClaimsToSQL(),
        DxToSQL(),
        InpatientToSQL(),
        EROnlyToSQL()
        ]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_utilization_table(update=False)
        atf.create_utilization_table(update=False, freq="QS")

class QualityAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\quality_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        CensusToSQL(),
        ClaimsDetailsToSQL(),
        AdmitClaimsToSQL(),
        DxToSQL(),
        InpatientToSQL(),
        EROnlyToSQL(),
        DemographicsToSQL()
        ]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_quality_agg_table(update=False)
        atf.create_quality_agg_table(update=False, freq="QS")


class TeamUtilizationAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\team_utilization_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        CensusToSQL(),
        ClaimsDetailsToSQL(),
        AdmitClaimsToSQL(),
        DxToSQL(),
        InpatientToSQL(),
        EROnlyToSQL(),
        TeamsToSQL()
        ]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_team_utl_agg_table(update=False)
        atf.create_team_utl_agg_table(update=False, freq="QS")


class TeamInfoAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\team_info_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        CensusToSQL(),
        ClaimsDetailsToSQL(),
        AdmitClaimsToSQL(),
        DxToSQL(),
        InpatientToSQL(),
        EROnlyToSQL(),
        DemographicsToSQL(),
        TeamsToSQL()
        ]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_team_info_agg_table(update=False)
        atf.create_team_info_agg_table(update=False, freq="QS")

class TeamIncidentsAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\team_incidents_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        CensusToSQL(),
        ClaimsDetailsToSQL(),
        AdmitClaimsToSQL(),
        DxToSQL(),
        InpatientToSQL(),
        EROnlyToSQL(),
        DemographicsToSQL(),
        BurnsToSQL(),
        FallsToSQL(),
        InfectionsToSQL(),
        MedErrorsToSQL(),
        TeamsToSQL(),
        WoundsToSQL()
        ]
    
    def output(self):
        return luigi.LocalTarget(self.log_file)

    def run(self):
        atf.create_team_incidents_agg_table(update=False)
        atf.create_team_incidents_agg_table(update=False, freq="QS")

class CenterAgg(luigi.Task):
    log_file = f"{update_logs_folder}\\center_agg{str(pd.to_datetime('today').date())}.txt"

    def requires(self):
        return [EnrollmentToSQL(),
        CensusToSQL(),
        ClaimsDetailsToSQL(),
        AdmitClaimsToSQL(),
        DxToSQL(),
        InpatientToSQL(),
        EROnlyToSQL(),
        DemographicsToSQL()
        ]

    def output(self):
        return luigi.LocalTarget(self.log_file)
        
    def run(self):
        atf.create_center_agg_table(update=False)
        atf.create_center_agg_table(update=False, freq="QS")

class ArchiveData(luigi.Task):
    
    def requires(self):
        return [AddressesToSQL(),
        AdmitClaimsToSQL(),
        ALfsToSQL(),
        ApptsToSQL(),
        AuthsToSQL(),
        BurnsToSQL(), 
        CenterDaysToSQL(),
        ClaimsDetailsToSQL(),
        DemographicsToSQL(),
        DxToSQL(),
        EnrollmentToSQL(),
        EROnlyToSQL(),
        FallsToSQL(),
        InfectionsToSQL(),
        InfluToSQL(),
        InpatientToSQL(),
        MedErrorsToSQL(),
        MedsToSQL(),
        PaymentsToSQL(),
        PnuemoToSQL(),
        ReferralsToSQL(),
        WoundsToSQL(),
        CensusToSQL(),
        EnrollmentAgg(),
        DemographicAgg(),
        FallsAgg(),
        InfectionsAgg(),
        MedErrorsAgg(),
        WoundsAgg(),
        BurnsErrorsAgg(),
        UtilizationAgg(),
        TeamUtilizationAgg(),
        TeamInfoAgg(),
        TeamIncidentsAgg(),
        CenterAgg(),
        QualityAgg(),
        CentersToSQL(),
        TeamsToSQL()]

    def output(self):
        return luigi.LocalTarget(
            f"{archive_data}\\{pd.datetime.today().date()}_creation.zip"
        )

    def run(self):
        if not os.path.exists(f"{archive_data}\\{pd.datetime.today().date()}_creation"):
            os.makedirs(f"{archive_data}\\{pd.datetime.today().date()}_creation")

        shutil.copytree(
            raw_data, f"{archive_data}\\{pd.datetime.today().date()}_creation\\raw"
        )
        shutil.copytree(
            processed_data,
            f"{archive_data}\\{pd.datetime.today().date()}_creation\\processed",
        )
        shutil.make_archive(
            f"{archive_data}\\{pd.datetime.today().date()}_creation",
            "zip",
            f"{archive_data}\\{pd.datetime.today().date()}_creation",
        )

        shutil.rmtree(
            f"{archive_data}\\{pd.datetime.today().date()}_creation", ignore_errors=True
        )



class CreateDatabasePipeline(luigi.Task):
    def requires(self):
        return [AddressesToSQL(),
        AdmitClaimsToSQL(),
        ALfsToSQL(),
        ApptsToSQL(),
        AuthsToSQL(),
        BurnsToSQL(), 
        CenterDaysToSQL(),
        ClaimsDetailsToSQL(),
        DemographicsToSQL(),
        DxToSQL(),
        EnrollmentToSQL(),
        EROnlyToSQL(),
        FallsToSQL(),
        InfectionsToSQL(),
        InfluToSQL(),
        InpatientToSQL(),
        MedErrorsToSQL(),
        MedsToSQL(),
        PaymentsToSQL(),
        PnuemoToSQL(),
        ReferralsToSQL(),
        WoundsToSQL(),
        CensusToSQL(),
        EnrollmentAgg(),
        DemographicAgg(),
        FallsAgg(),
        InfectionsAgg(),
        MedErrorsAgg(),
        WoundsAgg(),
        BurnsErrorsAgg(),
        UtilizationAgg(),
        TeamUtilizationAgg(),
        TeamInfoAgg(),
        TeamIncidentsAgg(),
        CenterAgg(),
        QualityAgg(),
        CentersToSQL(),
        TeamsToSQL(),
        ArchiveData()]

    def run(self):
        print("Complete")

if __name__ == "__main__":
    result = luigi.build([CreateDatabasePipeline()], local_scheduler=True)
    with open(luigi_log, "w") as myfile:
        myfile.write(f"Date: {str(pd.to_datetime('today').date())}{result}")
