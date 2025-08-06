{{ config(materialized='view', tags=['LdDltClp_App_Pdct_Feat']) }}

WITH Copy AS (
	SELECT
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_APPT_PDCT_I AS APPT_PDCT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_FEAT_I AS FEAT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_SRCE_SYST_APPT_FEAT_I AS SRCE_SYST_APPT_FEAT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_ACTL_VALU_R AS ACTL_VALU_R,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_APPT_PDCT_I AS APPT_PDCT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_FEAT_I AS FEAT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_SRCE_SYST_APPT_FEAT_I AS SRCE_SYST_APPT_FEAT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_ACTL_VALU_R AS ACTL_VALU_R
	FROM {{ ref('Src_Tera_APPT_PDCT_COND') }}
)

SELECT * FROM Copy