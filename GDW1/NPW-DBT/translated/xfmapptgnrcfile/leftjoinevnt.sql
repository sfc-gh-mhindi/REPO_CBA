{{ config(materialized='view', tags=['XfmApptGnrcFile']) }}

WITH LeftJoinEvnt AS (
	SELECT
		{{ ref('CSE_CCL_CLI_DATE') }}.APPT_I,
		{{ ref('CSE_CCL_CLI_DATE') }}.DATE_ROLE_C,
		{{ ref('CSE_CCL_CLI_DATE') }}.MODF_S,
		{{ ref('CSE_CCL_CLI_DATE') }}.MODF_D,
		{{ ref('CSE_CCL_CLI_DATE') }}.MODF_T,
		{{ ref('CSE_CCL_CLI_DATE') }}.GNRC_ROLE_S,
		{{ ref('CSE_CCL_CLI_DATE') }}.GNRC_ROLE_D,
		{{ ref('CSE_CCL_CLI_DATE') }}.GNRC_ROLE_T,
		{{ ref('CSE_CCL_CLI_DATE') }}.USER_I,
		{{ ref('CSE_CCL_CLI_DATE') }}.CHNG_REAS_TYPE_C,
		{{ ref('CSE_CCL_CLI_DATE') }}.promise_type,
		{{ ref('ApptGnrcDate') }}.dummy
	FROM {{ ref('CSE_CCL_CLI_DATE') }}
	LEFT JOIN {{ ref('ApptGnrcDate') }} ON {{ ref('CSE_CCL_CLI_DATE') }}.APPT_I = {{ ref('ApptGnrcDate') }}.APPT_I
	AND {{ ref('CSE_CCL_CLI_DATE') }}.DATE_ROLE_C = {{ ref('ApptGnrcDate') }}.DATE_ROLE_C
	AND {{ ref('CSE_CCL_CLI_DATE') }}.GNRC_ROLE_D = {{ ref('ApptGnrcDate') }}.GNRC_ROLE_D
	AND {{ ref('CSE_CCL_CLI_DATE') }}.CHNG_REAS_TYPE_C = {{ ref('ApptGnrcDate') }}.CHNG_REAS_TYPE_C
)

SELECT * FROM LeftJoinEvnt