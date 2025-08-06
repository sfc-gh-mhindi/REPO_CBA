{{ config(materialized='view', tags=['LdDltClp_App_Pdct_Feat']) }}

WITH CC_Identify_Deltas AS (
	SELECT
		COALESCE({{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I, {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I) AS APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.FEAT_I, {{ ref('{{ ref('Copy') }}') }}.FEAT_I) AS FEAT_I,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.SRCE_SYST_APPT_FEAT_I, {{ ref('{{ ref('Copy') }}') }}.SRCE_SYST_APPT_FEAT_I) AS SRCE_SYST_APPT_FEAT_I,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.ACTL_VALU_R, {{ ref('{{ ref('Copy') }}') }}.ACTL_VALU_R) AS ACTL_VALU_R,
		CASE
			WHEN {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.FEAT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.FEAT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I = {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I AND {{ ref('{{ ref('Copy') }}') }}.FEAT_I = {{ ref('{{ ref('Copy') }}') }}.FEAT_I) AND ({{ ref('{{ ref('Copy') }}') }}.ACTL_VALU_R <> {{ ref('{{ ref('Copy') }}') }}.ACTL_VALU_R OR {{ ref('{{ ref('Copy') }}') }}.SRCE_SYST_APPT_FEAT_I <> {{ ref('{{ ref('Copy') }}') }}.SRCE_SYST_APPT_FEAT_I) THEN '3'
			WHEN {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I = {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I AND {{ ref('{{ ref('Copy') }}') }}.FEAT_I = {{ ref('{{ ref('Copy') }}') }}.FEAT_I AND {{ ref('{{ ref('Copy') }}') }}.ACTL_VALU_R = {{ ref('{{ ref('Copy') }}') }}.ACTL_VALU_R AND {{ ref('{{ ref('Copy') }}') }}.SRCE_SYST_APPT_FEAT_I = {{ ref('{{ ref('Copy') }}') }}.SRCE_SYST_APPT_FEAT_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('Copy') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('Copy') }}') }} 
	ON {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I = {{ ref('{{ ref('Copy') }}') }}.APPT_PDCT_I
	AND {{ ref('{{ ref('Copy') }}') }}.FEAT_I = {{ ref('{{ ref('Copy') }}') }}.FEAT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM CC_Identify_Deltas