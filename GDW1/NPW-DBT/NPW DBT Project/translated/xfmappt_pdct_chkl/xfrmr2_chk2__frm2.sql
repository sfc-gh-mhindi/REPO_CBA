{{ config(materialized='view', tags=['XfmAppt_Pdct_Chkl']) }}

WITH Xfrmr2_CHK2__Frm2 AS (
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
		-- *SRC*: "CSEPO" : ToXfm2.APP_PROD_ID,
		CONCAT('CSEPO', {{ ref('CpySrc') }}.APP_PROD_ID) AS APPT_PDCT_I,
		'0002' AS CHKL_ITEM_C,
		-- *SRC*: \(20)If svchkriskvaliddt = 'Y' Then StringToDate(Trim(ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE[1, 4]) : '-' : Trim(ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE[5, 2]) : '-' : Trim(ToXfm2.ACCEPTS_COSTS_AND_RISKS_DATE[7, 2]), "%yyyy-%mm-%dd") Else pGDW_DEFAULT_DATE,
		IFF(
	    svchkriskvaliddt = 'Y', STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE, 1, 4)), '-'), TRIM(SUBSTRING({{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE, 5, 2))), '-'), TRIM(SUBSTRING({{ ref('CpySrc') }}.ACCEPTS_COSTS_AND_RISKS_DATE, 7, 2))), '%yyyy-%mm-%dd'), 
	    pGDW_DEFAULT_DATE
	) AS STUS_D,
		'ACPT' AS STUS_C,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: setNull(),
		SETNULL() AS CHKL_ITEM_X,
		pRUN_STRM_C AS RUN_STRM
	FROM {{ ref('CpySrc') }}
	WHERE svlsnullacceptscstflg = 'N' AND svIsNullAppProdId = 'N'
)

SELECT * FROM Xfrmr2_CHK2__Frm2