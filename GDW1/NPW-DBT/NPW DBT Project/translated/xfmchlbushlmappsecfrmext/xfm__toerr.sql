{{ config(materialized='view', tags=['XfmChlBusHlmAppSecFrmExt']) }}

WITH Xfm__ToErr AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToXfm.FRWD_DOCU_C) Then '9999' Else ToXfm.FRWD_DOCU_C,
		IFF({{ ref('LkpMapApptAsetStloHm') }}.FRWD_DOCU_C IS NULL, '9999', {{ ref('LkpMapApptAsetStloHm') }}.FRWD_DOCU_C) AS svEror,
		-- *SRC*: \(20)If ToXfm.FORWARD_DOC_TO = 9999 Then 'Y' Else 'N',
		IFF({{ ref('LkpMapApptAsetStloHm') }}.FORWARD_DOC_TO = 9999, 'Y', 'N') AS svSetlLocnFltr,
		{{ ref('LkpMapApptAsetStloHm') }}.HLM_APP_ID AS SRCE_KEY_I,
		'FORWARD_DOC_TO' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_APPT_ASET_STLO_HM' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS SRCE_EFFT_D,
		{{ ref('LkpMapApptAsetStloHm') }}.FORWARD_DOC_TO AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'FRWD_DOCU_C' AS TRSF_COLM_M,
		{{ ref('LkpMapApptAsetStloHm') }}.eror_seqn_i AS EROR_SEQN_I,
		FILE_NAME AS SRCE_FILE_M,
		GDW_PROS_ID AS PROS_KEY_EFFT_I,
		{{ ref('LkpMapApptAsetStloHm') }}.APPT_I AS TRSF_KEY_I
	FROM {{ ref('LkpMapApptAsetStloHm') }}
	WHERE svEror = '9999' AND svSetlLocnFltr = 'N'
)

SELECT * FROM Xfm__ToErr