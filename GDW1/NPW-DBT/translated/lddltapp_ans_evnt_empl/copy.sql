{{ config(materialized='view', tags=['LdDltAPP_ANS_EVNT_EMPL']) }}

WITH Copy AS (
	SELECT
		{{ ref('Src_Tera_EVNT_EMPL') }}.NEW_EVNT_I AS EVNT_I,
		{{ ref('Src_Tera_EVNT_EMPL') }}.NEW_EVNT_PATY_ROLE_TYPE_C AS EVNT_PATY_ROLE_TYPE_C,
		{{ ref('Src_Tera_EVNT_EMPL') }}.NEW_EMPL_I AS EMPL_I,
		{{ ref('Src_Tera_EVNT_EMPL') }}.OLD_EVNT_I AS EVNT_I,
		{{ ref('Src_Tera_EVNT_EMPL') }}.OLD_EVNT_PATY_ROLE_TYPE_C AS EVNT_PATY_ROLE_TYPE_C,
		{{ ref('Src_Tera_EVNT_EMPL') }}.OLD_EMPL_I AS EMPL_I
	FROM {{ ref('Src_Tera_EVNT_EMPL') }}
)

SELECT * FROM Copy