{{ config(materialized='view', tags=['XfmAppt_Pdct_Chkl']) }}

WITH Xfrmr2_CHK2__ToErr AS (
	SELECT
		-- *SRC*: \(20)if IsNull(ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE) then 'Y' else  if trim(ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE) = 0 then 'Y' else  if trim(ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE) = '' then 'Y' else 'N',
		IFF({{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE IS NULL, 'Y', IFF(TRIM({{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE) = 0, 'Y', IFF(TRIM({{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE) = '', 'Y', 'N'))) AS svlsnullacceptscstflg,
		-- *SRC*: \(20)If IsValid('date', ( IF IsNotNull((ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE)) THEN (ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE) ELSE "")[1, 4] : '-' : ( IF IsNotNull((ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE)) THEN (ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE) ELSE "")[5, 2] : '-' : ( IF IsNotNull((ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE)) THEN (ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE) ELSE "")[7, 2]) Then 'Y' Else 'N',
		IFF(    
	    ISVALID('date', CONCAT(CONCAT(CONCAT(CONCAT(IFF({{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE IS NOT NULL, {{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE, ''), '-'), IFF({{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE IS NOT NULL, {{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE, '')), '-'), IFF({{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE IS NOT NULL, {{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE, ''))), 
	    'Y', 
	    'N'
	) AS svchkriskvaliddt,
		-- *SRC*: \(20)if IsNull(ToXfm2.APP_PROD_ID) then 'Y' else  if trim(ToXfm2.APP_PROD_ID) = 0 then 'Y' else  if trim(ToXfm2.APP_PROD_ID) = '' then 'Y' else 'N',
		IFF({{ ref('CpySrc') }}.APP_PROD_ID IS NULL, 'Y', IFF(TRIM({{ ref('CpySrc') }}.APP_PROD_ID) = 0, 'Y', IFF(TRIM({{ ref('CpySrc') }}.APP_PROD_ID) = '', 'Y', 'N'))) AS svIsNullAppProdId,
		{{ ref('CpySrc') }}.APP_PROD_ID AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'LOOKUP FAILURE ON MAP_CSE_APPT_PDCT_RPAY' AS CONV_MAP_RULE_M,
		'APPT_PDCT_CHKL' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS SRCE_EFFT_D,
		{{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		DSJobName AS TRSF_X,
		'STUS_D' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_CPO_BUS_APP_PROD' AS SRCE_FILE_M,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: "CSEPO" : ToXfm2.APP_PROD_ID,
		CONCAT('CSEPO', {{ ref('CpySrc') }}.APP_PROD_ID) AS TRSF_KEY_I
	FROM {{ ref('CpySrc') }}
	WHERE svchkriskvaliddt = 'N' AND svlsnullacceptscstflg = 'N' AND svIsNullAppProdId = 'N'
)

SELECT * FROM Xfrmr2_CHK2__ToErr