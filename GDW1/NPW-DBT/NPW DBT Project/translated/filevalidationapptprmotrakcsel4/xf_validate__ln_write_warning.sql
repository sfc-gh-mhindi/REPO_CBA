{{ config(materialized='view', tags=['FileValidationApptPrmoTrakCSEL4']) }}

WITH xf_Validate__Ln_Write_Warning AS (
	SELECT
		-- *SRC*: \(20)If Ln_Recd_To_Vald.ROW_NUMBER = 0 And Ln_Recd_To_Vald.RECD_TYPE <> 'H' Then 0 else 1,
		IFF({{ ref('lk_GetTrailerInd') }}.ROW_NUMBER = 0 AND {{ ref('lk_GetTrailerInd') }}.RECD_TYPE <> 'H', 0, 1) AS svFirstRecd,
		-- *SRC*: \(20)If IsNull(Ln_Recd_To_Vald.ROW_NUMBER_1) Then 1 Else  If Ln_Recd_To_Vald.ROW_NUMBER_1 = 0 Then 1 Else  If Ln_Recd_To_Vald.ROW_NUMBER = Ln_Recd_To_Vald.ROW_NUMBER_1 And Ln_Recd_To_Vald.RECD_TYPE <> 'T' Then 0 Else 1,
		IFF({{ ref('lk_GetTrailerInd') }}.ROW_NUMBER_1 IS NULL, 1, IFF({{ ref('lk_GetTrailerInd') }}.ROW_NUMBER_1 = 0, 1, IFF({{ ref('lk_GetTrailerInd') }}.ROW_NUMBER = {{ ref('lk_GetTrailerInd') }}.ROW_NUMBER_1 AND {{ ref('lk_GetTrailerInd') }}.RECD_TYPE <> 'T', 0, 1))) AS svLastRecd,
		-- *SRC*: \(20)If Count(Ln_Recd_To_Vald.REST_OF_RECD, '|') + 1 <> pDELIMITER_COUNT Then 0 Else 1,
		IFF(COUNT({{ ref('lk_GetTrailerInd') }}.REST_OF_RECD, '|') + 1 <> pDELIMITER_COUNT, 0, 1) AS svDelimterChk,
		-- *SRC*: \(20)If Ln_Recd_To_Vald.RECD_TYPE = 'T' And Ln_Recd_To_Vald.ROW_NUMBER - 1 <> Field(Ln_Recd_To_Vald.REST_OF_RECD, '|', 1) Then 0 Else 1,
		IFF({{ ref('lk_GetTrailerInd') }}.RECD_TYPE = 'T' AND {{ ref('lk_GetTrailerInd') }}.ROW_NUMBER - 1 <> FIELD({{ ref('lk_GetTrailerInd') }}.REST_OF_RECD, '|', 1), 0, 1) AS svRecdCnt,
		-- *SRC*: \(20)If svFirstRecd = 0 Then 'First record is not a header' Else  If svLastRecd = 0 Then 'Last record is not a trailer' Else  If svDelimterChk = 0 Then 'Delimiter count does not match for the record ' : Ln_Recd_To_Vald.RECD_TYPE : '|' : Ln_Recd_To_Vald.REST_OF_RECD Else  If svRecdCnt = 0 Then 'Trailer record count doesnot match total number of detail records' Else '',
		IFF(
	    svFirstRecd = 0, 'First record is not a header',     
	    IFF(
	        svLastRecd = 0, 'Last record is not a trailer', 
	        IFF(svDelimterChk = 0, CONCAT(CONCAT(CONCAT('Delimiter count does not match for the record ', {{ ref('lk_GetTrailerInd') }}.RECD_TYPE), '|'), {{ ref('lk_GetTrailerInd') }}.REST_OF_RECD), IFF(svRecdCnt = 0, 'Trailer record count doesnot match total number of detail records', ''))
	    )
	) AS svWarning,
		svWarning AS MessageToWrite,
		'Warning' AS MessageSeverity
	FROM {{ ref('lk_GetTrailerInd') }}
	WHERE svWarning <> ''
)

SELECT * FROM xf_Validate__Ln_Write_Warning