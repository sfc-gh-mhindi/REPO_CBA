{{ config(materialized='view', tags=['DltAPPT_ASET_SETL_LOCNFrmTMP_APPT_ASET_SETL_LOCN']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I, {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I) AS NEW_ASET_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I) AND ({{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.FRWD_DOCU_C <> {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.FRWD_DOCU_C OR {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.SETL_CMMT_X <> {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.SETL_CMMT_X OR {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.SETL_LOCN_X <> {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.SETL_LOCN_X) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I AND {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.FRWD_DOCU_C = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.FRWD_DOCU_C AND {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.SETL_CMMT_X = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.SETL_CMMT_X AND {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.SETL_LOCN_X = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.SETL_LOCN_X THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }} 
	ON {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_APPT_I
	AND {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture