{{ config(materialized='view', tags=['XfmApptGnrcFile']) }}

WITH Join AS (
	SELECT
		{{ ref('Cpy') }}.APPT_I,
		{{ ref('Cpy') }}.DATE_ROLE_C,
		{{ ref('Cpy') }}.GNRC_ROLE_S,
		{{ ref('Cpy') }}.GNRC_ROLE_D,
		{{ ref('Cpy') }}.GNRC_ROLE_T,
		{{ ref('Cpy') }}.MODF_S,
		{{ ref('Cpy') }}.MODF_D,
		{{ ref('Cpy') }}.MODF_T,
		{{ ref('Cpy') }}.USER_I,
		{{ ref('Cpy') }}.CHNG_REAS_TYPE_C,
		{{ ref('Cpy') }}.promise_type,
		{{ ref('Remove_Duplicates') }}.MODF_S_TEMP
	FROM {{ ref('Cpy') }}
	LEFT JOIN {{ ref('Remove_Duplicates') }} ON {{ ref('Cpy') }}.APPT_I = {{ ref('Remove_Duplicates') }}.APPT_I
	AND {{ ref('Cpy') }}.promise_type = {{ ref('Remove_Duplicates') }}.promise_type
)

SELECT * FROM Join