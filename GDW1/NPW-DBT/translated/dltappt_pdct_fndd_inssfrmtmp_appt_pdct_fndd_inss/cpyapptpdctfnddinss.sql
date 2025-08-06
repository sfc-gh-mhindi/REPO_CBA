{{ config(materialized='view', tags=['DltAPPT_PDCT_FNDD_INSSFrmTMP_APPT_PDCT_FNDD_INSS']) }}

WITH CpyApptPdctFnddInss AS (
	SELECT
		NEW_APPT_PDCT_I,
		NEW_APPT_PDCT_FNDD_I,
		NEW_APPT_PDCT_FNDD_METH_I,
		NEW_FNDD_INSS_METH_C,
		NEW_SRCE_SYST_FNDD_I,
		NEW_SRCE_SYST_FNDD_METH_I,
		NEW_SRCE_SYST_C,
		NEW_FNDD_D,
		NEW_FNDD_A,
		NEW_PDCT_SYST_ACCT_N,
		NEW_CMPE_I,
		NEW_CMPE_ACCT_BSB_N,
		NEW_CMPE_ACCT_N,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_APPT_PDCT_I AS NEW_APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_APPT_PDCT_FNDD_I AS NEW_APPT_PDCT_FNDD_I,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_APPT_PDCT_FNDD_METH_I AS NEW_APPT_PDCT_FNDD_METH_I,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_FNDD_INSS_METH_C AS NEW_FNDD_INSS_METH_C,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_SRCE_SYST_FNDD_I AS NEW_SRCE_SYST_FNDD_I,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_SRCE_SYST_FNDD_METH_I AS NEW_SRCE_SYST_FNDD_METH_I,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_SRCE_SYST_C AS NEW_SRCE_SYST_C,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_FNDD_D AS NEW_FNDD_D,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_FNDD_A AS NEW_FNDD_A,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_PDCT_SYST_ACCT_N AS NEW_PDCT_SYST_ACCT_N,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_CMPE_I AS NEW_CMPE_I,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_CMPE_ACCT_BSB_N AS NEW_CMPE_ACCT_BSB_N,
		{{ ref('SrcTmpApptPdctFnddInssTera') }}.OLD_CMPE_ACCT_N AS NEW_CMPE_ACCT_N,
		OLD_FNDD_INSS_METH_C,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptPdctFnddInssTera') }}
)

SELECT * FROM CpyApptPdctFnddInss