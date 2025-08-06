{{ config(materialized='view', tags=['XfmComBusSmCaseFrmExt']) }}

WITH Join_212 AS (
	SELECT
		{{ ref('Data_Set_211') }}.SM_CASE_ID,
		{{ ref('Data_Set_211') }}.TARG_I,
		{{ ref('Data_Set_211') }}.TARG_SUBJ
	FROM {{ ref('Data_Set_211') }}
	INNER JOIN {{ ref('CpyRename') }} ON {{ ref('Data_Set_211') }}.SM_CASE_ID = {{ ref('CpyRename') }}.SM_CASE_ID
)

SELECT * FROM Join_212