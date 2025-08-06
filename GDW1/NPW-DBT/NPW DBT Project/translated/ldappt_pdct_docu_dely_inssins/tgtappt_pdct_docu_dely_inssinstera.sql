{{ config(materialized='incremental', alias='appt_pdct_docu_dely_inss', incremental_strategy='append', tags=['LdAPPT_PDCT_DOCU_DELY_INSSIns']) }}

SELECT
	APPT_PDCT_I
	EFFT_D
	DOCU_DELY_METH_C
	PYAD_TYPE_C
	ADRS_LINE_1_X
	ADRS_LINE_2_X
	SURB_X
	PCOD_C
	STAT_X
	ISO_CNTY_C
	BRCH_N
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtAPPT_PDCT_DOCU_DELY_INSSInsertDS') }}