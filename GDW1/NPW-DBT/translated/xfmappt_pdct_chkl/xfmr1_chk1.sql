{{ config(materialized='view', tags=['XfmAppt_Pdct_Chkl']) }}

WITH Xfmr1_CHK1 AS (
	SELECT
		-- *SRC*: \(20)if IsNull(ToXfm1.READ_COSTS_AND_RISKS_FLAG) then 'Y' else  if trim(ToXfm1.READ_COSTS_AND_RISKS_FLAG) = 0 then 'Y' else  if trim(ToXfm1.READ_COSTS_AND_RISKS_FLAG) = '' then 'Y' else 'N',
		IFF({{ ref('CpySrc') }}.READ_COSTS_AND_RISKS_FLAG IS NULL, 'Y', IFF(TRIM({{ ref('CpySrc') }}.READ_COSTS_AND_RISKS_FLAG) = 0, 'Y', IFF(TRIM({{ ref('CpySrc') }}.READ_COSTS_AND_RISKS_FLAG) = '', 'Y', 'N'))) AS svlsnullReadcstFlg,
		-- *SRC*: \(20)if IsNull(ToXfm1.APP_PROD_ID) then 'Y' else  if trim(ToXfm1.APP_PROD_ID) = 0 then 'Y' else  if trim(ToXfm1.APP_PROD_ID) = '' then 'Y' else 'N',
		IFF({{ ref('CpySrc') }}.APP_PROD_ID IS NULL, 'Y', IFF(TRIM({{ ref('CpySrc') }}.APP_PROD_ID) = 0, 'Y', IFF(TRIM({{ ref('CpySrc') }}.APP_PROD_ID) = '', 'Y', 'N'))) AS svIsNullAppProdID,
		-- *SRC*: "CSEPO" : ToXfm1.APP_PROD_ID,
		CONCAT('CSEPO', {{ ref('CpySrc') }}.APP_PROD_ID) AS APPT_PDCT_I,
		'0001' AS CHKL_ITEM_C,
		-- *SRC*: setNull(),
		SETNULL() AS STUS_D,
		-- *SRC*: \(20)IF ToXfm1.READ_COSTS_AND_RISKS_FLAG = 'Y' then "READ" ELSE "NTRD",
		IFF({{ ref('CpySrc') }}.READ_COSTS_AND_RISKS_FLAG = 'Y', 'READ', 'NTRD') AS STUS_C,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: setNull(),
		SETNULL() AS CHKL_ITEM_X,
		pRUN_STRM_C AS RUN_STRM
	FROM {{ ref('CpySrc') }}
	WHERE svlsnullReadcstFlg = 'N' AND svIsNullAppProdID = 'N'
)

SELECT * FROM Xfmr1_CHK1