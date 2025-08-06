{{ config(materialized='view', tags=['LdTMP_HLM_APP_FrmXfm']) }}

WITH TfmSrc AS (
	SELECT
		-- *SRC*: 'CSEHM' : FrmChlBusHlmApp.HLM_APP_ID,
		CONCAT('CSEHM', {{ ref('SrcChlBusHlmApp') }}.HLM_APP_ID) AS APPT_I
	FROM {{ ref('SrcChlBusHlmApp') }}
	WHERE {{ ref('SrcChlBusHlmApp') }}.HLM_APP_ID IS NOT NULL
)

SELECT * FROM TfmSrc