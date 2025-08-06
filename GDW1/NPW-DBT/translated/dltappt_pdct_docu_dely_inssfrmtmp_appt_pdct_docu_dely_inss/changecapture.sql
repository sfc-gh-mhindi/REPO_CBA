{{ config(materialized='view', tags=['DltAPPT_PDCT_DOCU_DELY_INSSFrmTMP_APPT_PDCT_DOCU_DELY_INSS']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_PDCT_I, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_PDCT_I) AS NEW_APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DOCU_DELY_METH_C, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DOCU_DELY_METH_C) AS NEW_DOCU_DELY_METH_C,
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PYAD_TYPE_C, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PYAD_TYPE_C) AS NEW_PYAD_TYPE_C,
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_1_X, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_1_X) AS NEW_ADRS_LINE_1_X,
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_2_X, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_2_X) AS NEW_ADRS_LINE_2_X,
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_SURB_X, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_SURB_X) AS NEW_SURB_X,
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PCOD_C, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PCOD_C) AS NEW_PCOD_C,
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_STAT_X, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_STAT_X) AS NEW_STAT_X,
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ISO_CNTY_C, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ISO_CNTY_C) AS NEW_ISO_CNTY_C,
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_BRCH_N, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_BRCH_N) AS NEW_BRCH_N,
		CASE
			WHEN {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_PDCT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_PDCT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_PDCT_I) AND ({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DOCU_DELY_METH_C <> {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DOCU_DELY_METH_C OR {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PYAD_TYPE_C <> {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PYAD_TYPE_C OR {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_1_X <> {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_1_X OR {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_2_X <> {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_2_X OR {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_SURB_X <> {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_SURB_X OR {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PCOD_C <> {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PCOD_C OR {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_STAT_X <> {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_STAT_X OR {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ISO_CNTY_C <> {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ISO_CNTY_C OR {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_BRCH_N <> {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_BRCH_N) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DOCU_DELY_METH_C = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DOCU_DELY_METH_C AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PYAD_TYPE_C = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PYAD_TYPE_C AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_1_X = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_1_X AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_2_X = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ADRS_LINE_2_X AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_SURB_X = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_SURB_X AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PCOD_C = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_PCOD_C AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_STAT_X = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_STAT_X AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ISO_CNTY_C = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_ISO_CNTY_C AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_BRCH_N = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_BRCH_N THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }} 
	ON {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_PDCT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture