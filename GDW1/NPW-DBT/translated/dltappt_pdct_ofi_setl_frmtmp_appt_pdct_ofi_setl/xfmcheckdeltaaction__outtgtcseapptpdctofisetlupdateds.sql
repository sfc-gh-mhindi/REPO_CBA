{{ config(materialized='view', tags=['DltAPPT_PDCT_OFI_SETL_FrmTMP_APPT_PDCT_OFI_SETL']) }}

WITH XfmCheckDeltaAction__OutTgtCSeApptPdctOfiSetlUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		-- *SRC*: \(20)If IsNull(InTmpCSeApptPdctOfiSetlTera.NEW_OFI_IDNN_X) THEN 'N' ELSE  IF TRIM(InTmpCSeApptPdctOfiSetlTera.NEW_OFI_IDNN_X) = '' THEN 'N' ELSE  if num(trim(InTmpCSeApptPdctOfiSetlTera.NEW_OFI_IDNN_X)) then ( if StringToDecimal(InTmpCSeApptPdctOfiSetlTera.NEW_OFI_IDNN_X) = 0 then 'N' else 'Y') Else 'Y',
		IFF({{ ref('SrcTmpCSeApptPdctOfiSetlTera') }}.NEW_OFI_IDNN_X IS NULL, 'N', IFF(TRIM({{ ref('SrcTmpCSeApptPdctOfiSetlTera') }}.NEW_OFI_IDNN_X) = '', 'N', IFF(NUM(TRIM({{ ref('SrcTmpCSeApptPdctOfiSetlTera') }}.NEW_OFI_IDNN_X)), IFF(STRINGTODECIMAL({{ ref('SrcTmpCSeApptPdctOfiSetlTera') }}.NEW_OFI_IDNN_X) = 0, 'N', 'Y'), 'Y'))) AS svinvalid1,
		{{ ref('SrcTmpCSeApptPdctOfiSetlTera') }}.NEW_APPT_PDCT_I AS APPT_PDCT_I,
		{{ ref('SrcTmpCSeApptPdctOfiSetlTera') }}.OLD_EFFT_D AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('SrcTmpCSeApptPdctOfiSetlTera') }}
	WHERE {{ ref('SrcTmpCSeApptPdctOfiSetlTera') }}.REC_TYPE = 'U' OR {{ ref('SrcTmpCSeApptPdctOfiSetlTera') }}.REC_TYPE = 'D' OR {{ ref('SrcTmpCSeApptPdctOfiSetlTera') }}.REC_TYPE = 'C' AND svinvalid1 = 'N'
)

SELECT * FROM XfmCheckDeltaAction__OutTgtCSeApptPdctOfiSetlUpdateDS