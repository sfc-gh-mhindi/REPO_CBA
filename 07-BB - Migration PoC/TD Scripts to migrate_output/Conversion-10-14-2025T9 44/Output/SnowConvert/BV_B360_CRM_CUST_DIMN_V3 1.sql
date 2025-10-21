--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "P_V_USR_TEC_0.DIMN_CUST", "P_V_USR_TEC_0.L_PATY_IM_PATY", "P_V_USR_TEC_0.S_PATY_ATTR", "B_V_USR_TEC_0.BV_BPB_PATY_SEGM" **
CREATE OR REPLACE TEMPORARY TABLE VT_ALL_CUST_BASE
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
AS
(
	SELECT
		A.* FROM
		(
			SELECT DISTINCT
						A.*,
						B.PATY_IDNN_HK,
						C.REL_MNGE_EMPL_BK AS RM_EMPL_I,
						PRTF.PRTF_IDNN_BK AS PRTF_I,
						PRTF.EFFT_D AS PRTF_STRT_D
					FROM
					(
							SELECT DISTINCT
											PATY_IDNN_BK,
											REGEXP_REPLACE(PATY_IDNN_BK, 'SAP~PT~\+', 'CIFPT+') as PATY_I,
											CASE
												WHEN UPPER(RTRIM(CUST_TYPE_M)) = UPPER(RTRIM('PERSON'))
													THEN 'P'
												WHEN UPPER(RTRIM(CUST_TYPE_M)) = UPPER(RTRIM('ORGANISATION'))
													THEN 'O'
											END AS PATY_TYP_X
										FROM
											P_V_USR_TEC_0.DIMN_CUST -- ALL CUSTOMER DATA
										WHERE
											UPPER(RTRIM( MSTR_SRCE_SYST_C)) = UPPER(RTRIM('SAP'))
										AND EFFT_D <= CURRENT_DATE()
							QUALIFY
											ROW_NUMBER() OVER ( PARTITION BY PATY_IDNN_BK ORDER BY EFFT_D DESC NULLS LAST) = 1  -- TAKE LATEST RECORD
					) A
					LEFT JOIN
					(
											SELECT DISTINCT
															PATY_IDNN_HK,
															PATY_I
														FROM
															P_V_USR_TEC_0.L_PATY_IM_PATY
														WHERE RECORD_DELETED_FLAG = 0
														AND UPPER(RTRIM( EXPY_D)) = UPPER(RTRIM('9999-12-31'))
					) B
					ON A.PATY_I = B.PATY_I

					-- Added this join to fix multiple RM owner per customer issue. This table has only one active record per customer to pick the correct portfolio
					LEFT JOIN
					(
											SELECT
															PATY_IDNN_HK,
	PRTF_IDNN_BK,
	EFFT_D
FROM
															P_V_USR_TEC_0.S_PATY_ATTR
WHERE
															UPPER(RTRIM( EXPY_D)) = UPPER(RTRIM('9999-12-31'))
AND UPPER(RTRIM( PRTF_IDNN_BK)) <> UPPER(RTRIM('UNKN'))
					)PRTF
					ON B.PATY_IDNN_HK = PRTF.PATY_IDNN_HK

					LEFT JOIN
					(
											--This table might have multiple RM owners for same customer. Hence we are joining with the active portfolio record from S_PATY_ATTR
											SELECT DISTINCT
												PATY_I,
															PRTF_BK,
															REL_MNGE_EMPL_BK
			FROM
															B_V_USR_TEC_0.BV_BPB_PATY_SEGM
			WHERE
															UPPER(RTRIM( REL_MNGE_CUST_EMPL_ROLE_C)) = UPPER(RTRIM('OWN'))
			AND REL_MNGE_EMPL_BK IS NOT NULL
					) C
					ON A.PATY_I = C.PATY_I
					AND PRTF.PRTF_IDNN_BK = C.PRTF_BK
		)A
		WHERE RM_EMPL_I IS NOT NULL
)
--															--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--															ON COMMIT PRESERVE ROWS
															                       															;

--Delete existing data from the final table
															DELETE FROM
	U_D_DSV_001_QPD_1.BV_B360_CRM_CUST_DIMN;

--Insert data to BV_B360_CRM_CUST_DIMN
	--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "U_D_DSV_001_QPD_1.BV_B360_CRM_CUST_DIMN", "P_V_USR_TEC_0.S_PATY_GRUP_DETL", "P_V_USR_TEC_0.R_CSE_OU_TEAM_STRC" **
	INSERT INTO U_D_DSV_001_QPD_1.BV_B360_CRM_CUST_DIMN SELECT
	CB.PATY_I						AS PATY_I,
	LTRIM(SUBSTRING(CB.PATY_IDNN_BK, POSITION('+' IN (CB.PATY_IDNN_BK)) + 1), '0') AS CIF_I,
	CB.PATY_TYP_X                    AS PATY_TYP_X,
	CB.PRTF_I						 AS PRTF_I,
	CB.RM_EMPL_I                     AS EMPL_I,
	SSUC.LONG_NAME                   AS LAN_I,
PGD.PATY_GRUP_I                  AS SALE_GRUP_I,
	CB.PRTF_STRT_D 					 AS PRTF_STRT_D

FROM
	VT_ALL_CUST_BASE CB
/* PATY_GRUP DETAILS */
LEFT JOIN
		P_V_USR_TEC_0.S_PATY_GRUP_DETL PGD
	ON CB.PATY_IDNN_HK = PGD.PATY_IDNN_HK
AND UPPER(RTRIM( PGD.BUSN_END_D)) = UPPER(RTRIM('9999-12-31'))
AND UPPER(RTRIM( PGD.EXPY_D)) = UPPER(RTRIM('9999-12-31'))

/* LAN ID */
LEFT JOIN
		P_V_USR_TEC_0.R_CSE_OU_TEAM_STRC SSUC
	ON CB.RM_EMPL_I = SSUC.CBA_STAFF_NUMBER
	AND SSUC.LONG_NAME NOT ILIKE '%\_%'
	WHERE CB.RM_EMPL_I IS NOT NULL;