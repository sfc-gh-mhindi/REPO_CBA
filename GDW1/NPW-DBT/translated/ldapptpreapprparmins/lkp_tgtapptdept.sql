{{ config(materialized='view', tags=['LdApptPreApprParmIns']) }}

WITH Lkp_TgtApptDept AS (
	SELECT
		{{ ref('TgtApptDeptInsertDS') }}.APPT_I,
		{{ ref('TgtApptDeptInsertDS') }}.APPT_PRE_APPR_PARM_I,
		{{ ref('TgtApptDeptInsertDS') }}.SRCE_SYST_PRE_APPR_PARM_I,
		{{ ref('TgtApptDeptInsertDS') }}.OVDT_INDX_R,
		{{ ref('TgtApptDeptInsertDS') }}.BUFR_R,
		{{ ref('TgtApptDeptInsertDS') }}.UNSC_CAPL_MIN_TSHD_A,
		{{ ref('TgtApptDeptInsertDS') }}.UNSC_CAPL_MAX_TSHD_A,
		{{ ref('TgtApptDeptInsertDS') }}.WRST_RISK_CRGD_C,
		{{ ref('TgtApptDeptInsertDS') }}.CRGD_MULT_R,
		{{ ref('TgtApptDeptInsertDS') }}.FORM_MULT_R,
		{{ ref('TgtApptDeptInsertDS') }}.APPT_MULT_R,
		{{ ref('TgtApptDeptInsertDS') }}.CRGD_MIN_APLC_A,
		{{ ref('TgtApptDeptInsertDS') }}.CRGD_HIGH_CAPL_TSHD_A,
		{{ ref('TgtApptDeptInsertDS') }}.PRE_APPR_A,
		{{ ref('TgtApptDeptInsertDS') }}.EFFT_D,
		{{ ref('TgtApptDeptInsertDS') }}.PROS_KEY_EFFT_I,
		{{ ref('TgtApptDeptInsertDS') }}.EROR_SEQN_I,
		{{ ref('TgtApptDeptInsertDS') }}.PRE_APPR_SRCE_PATY_I,
		{{ ref('TgtApptDeptInsertDS') }}.SCRD_CRGD_MULT_R,
		{{ ref('TgtApptDeptInsertDS') }}.SCRD_FORM_MULT_R,
		{{ ref('TgtApptDeptInsertDS') }}.SCRD_MAX_ACCF_TSHD_A,
		{{ ref('TgtApptDeptInsertDS') }}.EQIP_FNCL_MAX_TSHD_A,
		{{ ref('TgtApptDeptInsertDS') }}.TOTL_PRE_APPR_BUFR_RATE_PERC_V,
		{{ ref('TgtApptDeptInsertDS') }}.EQIP_FNCL_BUFR_RATE_PERC_V,
		{{ ref('TgtApptDeptInsertDS') }}.TOTL_SCRD_PRE_APPR_A,
		{{ ref('TgtApptDeptInsertDS') }}.EQIP_FNCL_PRE_APPR_A,
		{{ ref('TgtApptDeptInsertDS') }}.SCRD_APPT_MULT_R,
		{{ ref('TgtApptDeptInsertDS') }}.SCRD_MIN_ACPT_A,
		{{ ref('TgtApptDeptInsertDS') }}.EQIP_FNCL_MIN_ACPT_A,
		{{ ref('TgtApptDeptInsertDS') }}.SCRD_CAP_MAX_TSHD_A,
		{{ ref('TgtApptDeptInsertDS') }}.EQIP_FNCL_CAP_MAX_TSHD_A,
		{{ ref('TgtApptDeptInsertDS') }}.EQIP_FNCL_APPT_MULT_R,
		{{ ref('TgtApptDeptInsertDS') }}.EQIP_FNCL_FORM_MULT_R,
		{{ ref('TgtApptDeptInsertDS') }}.EQIP_FNCL_CRGD_MULT_R,
		{{ ref('TgtApptDeptInsertDS') }}.MODF_USER_I,
		{{ ref('TgtApptDeptInsertDS') }}.MODF_S,
		{{ ref('SrcTmpApptEvntGrupTera') }}.APPT_I AS APPT_I_1,
		{{ ref('SrcTmpApptEvntGrupTera') }}.APPT_PRE_APPR_PARM_I AS APPT_PRE_APPR_PARM_I_1
	FROM {{ ref('TgtApptDeptInsertDS') }}
	LEFT JOIN {{ ref('SrcTmpApptEvntGrupTera') }} ON {{ ref('TgtApptDeptInsertDS') }}.APPT_I = {{ ref('SrcTmpApptEvntGrupTera') }}.APPT_I
	AND {{ ref('TgtApptDeptInsertDS') }}.APPT_PRE_APPR_PARM_I = {{ ref('SrcTmpApptEvntGrupTera') }}.APPT_PRE_APPR_PARM_I
)

SELECT * FROM Lkp_TgtApptDept