{{ config(materialized='view', tags=['LdMAP_CSE_SM_CASE']) }}

SELECT
	SM_CASE_ID
	TARG_I
	TARG_SUBJ
	EFFT_D 
FROM {{ ref('TgtMapCseSmCaseInsertDS') }}