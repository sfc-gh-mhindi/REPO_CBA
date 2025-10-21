
-- Creates a temporary table `vt_payaways_base` containing transaction-level data related to payaways.
-- This includes details like transaction date, account number, amount, category, brand, and customer identifiers.
-- The data is sourced from a view that categorises payaway transactions.
-- This table serves as the foundational dataset for further analysis of payaway behaviours.
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "U_V_DSV_001_QPD_1.BV_B360_TRAN_CATG_PAY_AWAY_VW" **
CREATE OR REPLACE TEMPORARY TABLE vt_payaways_base
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
AS (
	SELECT
		a.FNCL_TRAN_REL_PAYT_SYST_DERV_BK,
		a.TRAN_D,
		a.ACCT_IDNN_BK,
		a.SRCE_ACCT_N,
		a.TRAN_CR_DR_F,
		a.TRAN_NOTE_X,
		a.TRAN_A,
		a.TRAN_BUSN_CATG_M,
		a.TRAN_DETL_CATG_M,
		a.TRAN_BRND_M,
		a.BANK_C,
		a.NEED_M,
		a.PRTF_I,
		a.CUST_M,
		a.PATY_I,
		a.PATY_GRUP_I,
		a.NO_OF_PAYTS_LAST_3M,
		a.ERLY_TRAN_D,
		a.LTST_TRAN_D
FROM
		U_V_DSV_001_QPD_1.BV_B360_TRAN_CATG_PAY_AWAY_VW a
   )
--		-- Optimise access by TRAN ID
--		--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--		ON COMMIT PRESERVE ROWS
		                       		;

-- Creates a temporary table `vt_payaways_ftp` to identify first-time payaway transactions.
		-- A transaction is considered first-time if:
		--   a) It is the earliest transaction for a given party and category.
		--   b) It occurred within the last 90 days.
		CREATE OR REPLACE TEMPORARY TABLE vt_payaways_ftp
		COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
		AS (
	SELECT
		PATY_I,
		PATY_GRUP_I,
		PRTF_I,
		TRAN_DETL_CATG_M,
		NEED_M,
		TRAN_BRND_M,
		MIN(TRAN_D) AS GROUP_MIN_TRAN_D,
		CASE
					WHEN MIN(TRAN_D) = MIN(ERLY_TRAN_D)
		 AND MIN(TRAN_D) >= (
						SELECT
							MAX(TRAN_D)
						     FROM
							vt_payaways_base
						   ) - INTERVAL '90 DAY'
						THEN 1
		  ELSE 0
		END AS FIRST_TIME_PAYMENT
FROM
		vt_payaways_base
GROUP BY
		PATY_I,
		PATY_GRUP_I,
		PRTF_I,
		TRAN_DETL_CATG_M,
		NEED_M,
		TRAN_BRND_M
	)
--		--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--		ON COMMIT PRESERVE ROWS
		                       		;

-- Creates a temporary table `vt_payaways_ftp_eal_dates` to capture details of first-time transactions.
		-- For each first-time payment group, it records:
		--   - The total transaction amount (summed if multiple transactions occurred on the same date).
		--   - The source account number used.
		-- This supports understanding the financial impact and account usage of first-time payaways.
		CREATE OR REPLACE TEMPORARY TABLE vt_payaways_ftp_eal_dates
		COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
		AS (
	SELECT
		B.PATY_I,
		B.PATY_GRUP_I,
		B.PRTF_I,
		B.TRAN_DETL_CATG_M,
		B.NEED_M,
		B.TRAN_BRND_M,
		SUM(ABS(B.TRAN_A)) AS FIRST_TIME_TRAN_AMT,
		MAX(B.SRCE_ACCT_N) AS FIRST_TIME_ACCT
		 FROM
		vt_payaways_base B
		   JOIN
					vt_payaways_ftp A
		   ON b.PATY_I = a.PATY_I
		       AND b.PATY_GRUP_I = a.PATY_GRUP_I
		       AND b.PRTF_I = a.PRTF_I
		       AND b.TRAN_DETL_CATG_M = a.TRAN_DETL_CATG_M
		       AND b.NEED_M = a.NEED_M
		       AND (
		         b.TRAN_BRND_M = a.TRAN_BRND_M
		         OR b.TRAN_BRND_M IS NULL
		         AND a.TRAN_BRND_M IS NULL
		       )

		   Where B.TRAN_D = A.GROUP_MIN_TRAN_D
		   and FIRST_TIME_PAYMENT = 1

		    GROUP BY
		       B.PATY_I,
		       B.PATY_GRUP_I,
		       B.PRTF_I,
		       B.TRAN_DETL_CATG_M,
		       B.NEED_M,
		       B.TRAN_BRND_M

		   )
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

-- Creates a temporary table `vt_payaways_ftp_lat_det` to capture the most recent transaction details per payaway group.
					-- It uses row numbering to select the latest transaction by date and amount.
					-- Outputs include the latest transaction date, amount, and account number.
					-- This helps track the most recent activity for each payaway group.
					CREATE OR REPLACE TEMPORARY TABLE vt_payaways_ftp_lat_det
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		PATY_GRUP_I,
		TRAN_DETL_CATG_M,
		TRAN_BRND_M,
		BANK_C,
		TRAN_D AS LATEST_TRAN_DATE,
		TRAN_A AS LATEST_TRAN_AMT,
		SRCE_ACCT_N AS LATEST_TRAN_ACCT
FROM (
					SELECT
						B.*,
						ROW_NUMBER() OVER (
						     PARTITION BY
						       PATY_GRUP_I,
						       TRAN_DETL_CATG_M, CASE
											WHEN TRAN_BRND_M IS NOT NULL
												THEN TRAN_BRND_M
							END, CASE
											WHEN TRAN_BRND_M IS NULL
												THEN BANK_C
							END
						     ORDER BY TRAN_D DESC NULLS LAST, TRAN_A DESC NULLS LAST
						   ) AS tran_d_rank
						 FROM
						vt_payaways_base B
) a
WHERE tran_d_rank = 1
			)
--						--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--						ON COMMIT PRESERVE ROWS
						                       						;

-- Creates a temporary table `vt_payaways_agg` that aggregates payaway data for insights generation.
						-- Combines data from base, first-time, and latest transaction tables.
						-- Outputs include:
						--   - Unique insight ID (`INSG_I`) based on hashed identifiers.
						--   - Total number and amount of transactions in the last 90 days.
						--   - First-time transaction details (if applicable).
						--   - Latest transaction details.
						--   - Customer and portfolio identifiers.
						-- This table is used to generate structured insights about payaway behaviour for reporting or modelling.
						--** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "PFTCF.HASH_MD5" **
						CREATE OR REPLACE TEMPORARY TABLE vt_payaways_agg
						COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
						AS (
	WITH CTE AS (
		SELECT
					'PAYAWAY' AS INSG_TYPE_M,
					s.CUST_M,
					s.PATY_I,
					s.PATY_GRUP_I,
					s.PRTF_I,
					s.TRAN_DETL_CATG_M,
					s.NEED_M,
					s.TRAN_BRND_M,
					CASE
						WHEN s.TRAN_BRND_M IS NULL
							THEN s.BANK_C
					    ELSE NULL
					END AS OFI_BANK_LIST,
					COUNT(*) AS NUM_PAYMENTS_90,
					SUM(CAST(ABS(s.TRAN_A) AS FLOAT)) AS TOTAL_TRANSACTION_AMOUNT,
					MAX(s.TRAN_D) AS LATEST_TRAN_D_90,
					MIN(s.TRAN_D) AS ERLY_TRAN_D_90,
					MIN(s.ERLY_TRAN_D) AS ERLY_TRAN_D
	FROM
					vt_payaways_base s
	GROUP BY 1,2,3,4,5,6,7,8,9
	   )
	SELECT DISTINCT
		CASE
					WHEN s.TRAN_BRND_M IS NOT NULL
						THEN MD5(COALESCE(UPPER(TRIM(s.PATY_GRUP_I)), 'NULL') || COALESCE(UPPER(TRIM(s.TRAN_DETL_CATG_M)), 'NULL') || COALESCE(UPPER(TRIM(s.TRAN_BRND_M)), 'NULL')
						           )
	           ELSE MD5(COALESCE(UPPER(TRIM(s.PATY_GRUP_I)), 'NULL') || COALESCE(UPPER(TRIM(s.TRAN_DETL_CATG_M)), 'NULL') || COALESCE(UPPER(TRIM(s.TRAN_BRND_M)), 'NULL') || COALESCE(UPPER(TRIM(s.OFI_BANK_LIST)), 'NULL')
	           )
		END AS INSG_I,
		CURRENT_DATE() AS CRAT_D,
		CURRENT_DATE() + 90 AS EXPY_D,
		s.INSG_TYPE_M,
		s.CUST_M,
		s.PATY_I,
		s.PATY_GRUP_I,
		s.PRTF_I,
		s.TRAN_DETL_CATG_M,
		s.NEED_M,
		s.TRAN_BRND_M,
		s.NUM_PAYMENTS_90,
		s.TOTAL_TRANSACTION_AMOUNT,
		s.LATEST_TRAN_D_90,
		s.ERLY_TRAN_D_90,
		s.ERLY_TRAN_D,
		CASE
					WHEN o.FIRST_TIME_PAYMENT > 0
						THEN 'FIRST_TIME_PAYAWAY'
		    ELSE NULL
		END AS SUB_INSG_TYPE_M,
		e.FIRST_TIME_TRAN_AMT,
		e.FIRST_TIME_ACCT,
		COALESCE(l.LATEST_TRAN_AMT, l2.LATEST_TRAN_AMT) AS LATEST_TRAN_AMT,
		COALESCE(l.LATEST_TRAN_ACCT, l2.LATEST_TRAN_ACCT) AS LATEST_TRAN_ACCT,
		COALESCE(s.OFI_BANK_LIST, l.BANK_C) AS OFI_BANK_LIST
FROM CTE s
LEFT JOIN
					vt_payaways_ftp_lat_det l
		ON s.PATY_GRUP_I = l.PATY_GRUP_I
		AND s.TRAN_DETL_CATG_M = l.TRAN_DETL_CATG_M
		AND s.TRAN_BRND_M = l.TRAN_BRND_M
LEFT JOIN
					vt_payaways_ftp_lat_det l2
		ON s.PATY_GRUP_I = l2.PATY_GRUP_I
		AND s.TRAN_DETL_CATG_M = l2.TRAN_DETL_CATG_M
		AND s.TRAN_BRND_M IS NULL AND l2.TRAN_BRND_M IS NULL
		AND ((s.OFI_BANK_LIST = l2.BANK_C) OR ((s.OFI_BANK_LIST IS NULL) and (l2.BANK_C IS NULL)))
LEFT JOIN
					vt_payaways_ftp o
		ON s.PATY_I = o.PATY_I
		AND s.PATY_GRUP_I = o.PATY_GRUP_I
		AND s.PRTF_I = o.PRTF_I
		AND s.TRAN_DETL_CATG_M = o.TRAN_DETL_CATG_M
		AND s.NEED_M = o.NEED_M
		AND (s.TRAN_BRND_M = o.TRAN_BRND_M OR (s.TRAN_BRND_M IS NULL AND o.TRAN_BRND_M IS NULL))
LEFT JOIN
					vt_payaways_ftp_eal_dates e
		ON s.PATY_GRUP_I = e.PATY_GRUP_I
		AND s.PRTF_I = e.PRTF_I
		AND s.TRAN_DETL_CATG_M = e.TRAN_DETL_CATG_M
		AND (s.TRAN_BRND_M = e.TRAN_BRND_M OR (s.TRAN_BRND_M IS NULL AND e.TRAN_BRND_M IS NULL))
					)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					drop table vt_payaways_base;
drop table vt_payaways_ftp_eal_dates;
drop table vt_payaways_ftp_lat_det;
drop table vt_payaways_ftp;

					-- Creates a temporary table `vt_payaways_descriptions` that generates both short and long descriptions for customer payaway behaviours.
					-- These descriptions are derived from the aggregated payaway insights stored in `vt_payaways_agg`.

					-- Purpose:
					-- To produce human-readable summaries that explain customer payment activity, especially highlighting first-time payments and recent trends.

					-- Outputs:
					-- 1. `SHRT_DESC_X`: A concise sentence summarising the customer's payaway behaviour.
					-- 2. `LONG_DESC_X`: A detailed narrative including dates, amounts, and account details of the most recent transactions.
					CREATE OR REPLACE TEMPORARY TABLE vt_payaways_descriptions
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	/*
	Create long and short descriptions
	 */
	-- Long and Short Descriptions for PayAways
	SELECT
INSG_I,
		CRAT_D,
		EXPY_D,
		INSG_TYPE_M,
		SUB_INSG_TYPE_M,
		CUST_M,
		PATY_I,
		PATY_GRUP_I,
		PRTF_I,
		TRAN_DETL_CATG_M,
		NEED_M,
		OFI_BANK_LIST,
		TRAN_BRND_M,
		NUM_PAYMENTS_90,
		TOTAL_TRANSACTION_AMOUNT,
		LATEST_TRAN_D_90,
		ERLY_TRAN_D_90,
		ERLY_TRAN_D,
		FIRST_TIME_TRAN_AMT,
		FIRST_TIME_ACCT,
		LATEST_TRAN_AMT,
		LATEST_TRAN_ACCT,
		CASE
					WHEN UPPER(RTRIM(INSG_TYPE_M)) = UPPER(RTRIM('PAYAWAY')) AND UPPER(RTRIM( SUB_INSG_TYPE_M)) = UPPER(RTRIM('FIRST_TIME_PAYAWAY')) AND TRAN_BRND_M IS NOT NULL
						THEN
						CUST_M ||
				' had their first payment with '
				|| TRAN_BRND_M ||
				' on ' || TO_CHAR(ERLY_TRAN_D_90, 'DD') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || ' ' || INITCAP(TO_CHAR(ERLY_TRAN_D_90, 'MON') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/) || ' ' || TO_CHAR(ERLY_TRAN_D_90, 'YYYY') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ ||
				' for ' || CASE
							WHEN FIRST_TIME_TRAN_AMT > 0
											THEN CONCAT('$',TRIM(TO_CHAR(FIRST_TIME_TRAN_AMT, '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
							WHEN FIRST_TIME_TRAN_AMT < 0
											THEN CONCAT('-$',TRIM(TO_CHAR(ABS(FIRST_TIME_TRAN_AMT), '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
						END || '.'
					WHEN UPPER(RTRIM(INSG_TYPE_M)) = UPPER(RTRIM('PAYAWAY')) AND UPPER(RTRIM( SUB_INSG_TYPE_M)) = UPPER(RTRIM('FIRST_TIME_PAYAWAY')) AND TRAN_BRND_M IS NULL AND OFI_BANK_LIST IS NOT NULL
						THEN
						CUST_M ||
					' had their first payment with an account at '
					|| OFI_BANK_LIST ||
					' on ' || TO_CHAR(ERLY_TRAN_D_90, 'DD') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || ' ' || INITCAP(TO_CHAR(ERLY_TRAN_D_90, 'MON') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/) || ' ' || TO_CHAR(ERLY_TRAN_D_90, 'YYYY') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ ||
					' for ' || CASE
							WHEN FIRST_TIME_TRAN_AMT > 0
											THEN CONCAT('$',TRIM(TO_CHAR(FIRST_TIME_TRAN_AMT, '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
							WHEN FIRST_TIME_TRAN_AMT < 0
											THEN CONCAT('-$',TRIM(TO_CHAR(ABS(FIRST_TIME_TRAN_AMT), '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
						END || '.'
					WHEN UPPER(RTRIM(INSG_TYPE_M)) = UPPER(RTRIM('PAYAWAY')) AND UPPER(RTRIM( SUB_INSG_TYPE_M)) = UPPER(RTRIM('FIRST_TIME_PAYAWAY')) AND TRAN_BRND_M IS NULL AND OFI_BANK_LIST IS NULL
						THEN
						CUST_M ||
					' had their first payment for '
					|| NEED_M ||
					' on ' || TO_CHAR(ERLY_TRAN_D_90, 'DD') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || ' ' || INITCAP(TO_CHAR(ERLY_TRAN_D_90, 'MON') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/) || ' ' || TO_CHAR(ERLY_TRAN_D_90, 'YYYY') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ ||
					' for ' || CASE
							WHEN FIRST_TIME_TRAN_AMT > 0
											THEN CONCAT('$',TRIM(TO_CHAR(FIRST_TIME_TRAN_AMT, '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
							WHEN FIRST_TIME_TRAN_AMT < 0
											THEN CONCAT('-$',TRIM(TO_CHAR(ABS(FIRST_TIME_TRAN_AMT), '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
						END || '.'
					WHEN UPPER(RTRIM(INSG_TYPE_M)) = UPPER(RTRIM('PAYAWAY')) AND SUB_INSG_TYPE_M IS NULL AND TRAN_BRND_M IS NOT NULL
						THEN
						'In the past 90 days, ' || CUST_M || ' has made ' || TO_CHAR(NUM_PAYMENTS_90) /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ ||' payment/s for ' || NEED_M || ' with ' || TRAN_BRND_M || ', totalling ' || CASE
							WHEN TOTAL_TRANSACTION_AMOUNT > 0
											THEN CONCAT('$',trim(TO_CHAR(TOTAL_TRANSACTION_AMOUNT, '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
							WHEN TOTAL_TRANSACTION_AMOUNT < 0
											THEN CONCAT('-$',trim(TO_CHAR(ABS(TOTAL_TRANSACTION_AMOUNT), '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
						END || '.'
					WHEN UPPER(RTRIM(INSG_TYPE_M)) = UPPER(RTRIM('PAYAWAY')) AND SUB_INSG_TYPE_M IS NULL AND TRAN_BRND_M IS NULL AND OFI_BANK_LIST IS NOT NULL
						THEN
						'In the past 90 days, ' || CUST_M || ' has made ' || TO_CHAR(NUM_PAYMENTS_90) /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ ||' payment/s for ' || NEED_M || ' with an account at ' || OFI_BANK_LIST || ', totalling ' || CASE
							WHEN TOTAL_TRANSACTION_AMOUNT > 0
											THEN CONCAT('$',trim(TO_CHAR(TOTAL_TRANSACTION_AMOUNT, '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
							WHEN TOTAL_TRANSACTION_AMOUNT < 0
											THEN CONCAT('-$',trim(TO_CHAR(ABS(TOTAL_TRANSACTION_AMOUNT), '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
						END || '.'
					WHEN UPPER(RTRIM(INSG_TYPE_M)) = UPPER(RTRIM('PAYAWAY')) AND SUB_INSG_TYPE_M IS NULL AND TRAN_BRND_M IS NULL AND OFI_BANK_LIST IS NULL
						THEN
						'In the past 90 days, ' || CUST_M || ' has made ' || TO_CHAR(NUM_PAYMENTS_90) /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || ' ' ||NEED_M ||' payment/s, totalling ' || CASE
							WHEN TOTAL_TRANSACTION_AMOUNT > 0
											THEN CONCAT('$',trim(TO_CHAR(TOTAL_TRANSACTION_AMOUNT, '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
							WHEN TOTAL_TRANSACTION_AMOUNT < 0
											THEN CONCAT('-$',trim(TO_CHAR(ABS(TOTAL_TRANSACTION_AMOUNT), '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
						END || '.'
		            ELSE NULL
		END AS SHRT_DESC_X,
		CASE
					WHEN UPPER(RTRIM(INSG_TYPE_M)) = UPPER(RTRIM('PAYAWAY'))
		    AND UPPER(RTRIM( SUB_INSG_TYPE_M)) = UPPER(RTRIM('FIRST_TIME_PAYAWAY'))
		    AND TRAN_BRND_M IS NOT NULL
						THEN
						'This is the first recorded ' || TRAN_DETL_CATG_M || ' payment with ' || TRAN_BRND_M ||
						' in the last 2 years. ' || CHR(10) || 'The most recent payment details are as follows:' || CHR(10) ||
						'- Date: ' || TO_CHAR(LATEST_TRAN_D_90, 'DD') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || ' ' || INITCAP(TO_CHAR(LATEST_TRAN_D_90, 'MON') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/) || ' ' || TO_CHAR(LATEST_TRAN_D_90, 'YYYY') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || CHR(10) ||
						'- Amount: ' || CASE
							WHEN LATEST_TRAN_AMT > 0
											THEN CONCAT('$', TRIM(TO_CHAR(LATEST_TRAN_AMT, '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
							WHEN LATEST_TRAN_AMT < 0
											THEN CONCAT('-$', TRIM(TO_CHAR(ABS(LATEST_TRAN_AMT), '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
						END || CHR(10) ||
						  '- Account: ' || LATEST_TRAN_ACCT
					WHEN UPPER(RTRIM(INSG_TYPE_M)) = UPPER(RTRIM('PAYAWAY'))
		    AND UPPER(RTRIM( SUB_INSG_TYPE_M)) = UPPER(RTRIM('FIRST_TIME_PAYAWAY'))
		    AND TRAN_BRND_M IS NULL
		    AND OFI_BANK_LIST IS NOT NULL
						THEN
						'This is the first recorded ' || TRAN_DETL_CATG_M || ' payment with an account at ' || OFI_BANK_LIST ||
						' in the last 2 years. ' || CHR(10) ||  'The most recent payment details are as follows:' || CHR(10) ||
						'- Date: ' || TO_CHAR(LATEST_TRAN_D_90, 'DD') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || ' ' || INITCAP(TO_CHAR(LATEST_TRAN_D_90, 'MON') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/) || ' ' || TO_CHAR(LATEST_TRAN_D_90, 'YYYY') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || CHR(10) ||
						'- Amount: ' || CASE
							WHEN LATEST_TRAN_AMT > 0
											THEN CONCAT('$', TRIM(TO_CHAR(LATEST_TRAN_AMT, '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
							WHEN LATEST_TRAN_AMT < 0
											THEN CONCAT('-$', TRIM(TO_CHAR(ABS(LATEST_TRAN_AMT), '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
						END || CHR(10) ||
						  '- Account: ' || LATEST_TRAN_ACCT
					WHEN UPPER(RTRIM(INSG_TYPE_M)) = UPPER(RTRIM('PAYAWAY'))
		    AND UPPER(RTRIM( SUB_INSG_TYPE_M)) = UPPER(RTRIM('FIRST_TIME_PAYAWAY'))
		    AND TRAN_BRND_M IS NULL
		    AND OFI_BANK_LIST IS NULL
						THEN
						'This is the first recorded ' || TRAN_DETL_CATG_M || ' payment in the last 2 years. ' || CHR(10) ||  'The most recent payment details are as follows:' || CHR(10) ||
						'- Date: ' || TO_CHAR(LATEST_TRAN_D_90, 'DD') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || ' ' || INITCAP(TO_CHAR(LATEST_TRAN_D_90, 'MON') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/) || ' ' || TO_CHAR(LATEST_TRAN_D_90, 'YYYY') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || CHR(10) ||
						'- Amount: ' || CASE
							WHEN LATEST_TRAN_AMT > 0
											THEN CONCAT('$', TRIM(TO_CHAR(LATEST_TRAN_AMT, '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
							WHEN LATEST_TRAN_AMT < 0
											THEN CONCAT('-$', TRIM(TO_CHAR(ABS(LATEST_TRAN_AMT), '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
						END || CHR(10) ||
						  '- Account: ' || LATEST_TRAN_ACCT
					WHEN UPPER(RTRIM(INSG_TYPE_M)) = UPPER(RTRIM('PAYAWAY')) AND SUB_INSG_TYPE_M IS NULL
						THEN
						'The earliest ' || (TRAN_DETL_CATG_M) || ' payment that ' || CUST_M ||
						' made in the last 90 days was on ' || TO_CHAR(ERLY_TRAN_D_90, 'DD') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || ' ' || INITCAP(TO_CHAR(ERLY_TRAN_D_90, 'MON') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/) || ' ' || TO_CHAR(ERLY_TRAN_D_90, 'YYYY') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ ||'.' || CHR(10) ||
						'The most recent payment details are as follows:' || CHR(10) ||
						'- Date: ' || TO_CHAR(LATEST_TRAN_D_90, 'DD') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || ' ' || INITCAP(TO_CHAR(LATEST_TRAN_D_90, 'MON') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/) || ' ' || TO_CHAR(LATEST_TRAN_D_90, 'YYYY') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ || CHR(10) ||
						'- Amount: ' || CASE
							WHEN LATEST_TRAN_AMT > 0
											THEN CONCAT('$', TRIM(TO_CHAR(LATEST_TRAN_AMT, '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
							WHEN LATEST_TRAN_AMT < 0
											THEN CONCAT('-$', TRIM(TO_CHAR(ABS(LATEST_TRAN_AMT), '999,999,999,990') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/))
						END || CHR(10) ||
						  '- Account: ' || LATEST_TRAN_ACCT
		       ELSE NULL
		END AS LONG_DESC_X
		      FROM
		vt_payaways_agg

					   )
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;
drop table vt_payaways_agg;

					-- Creates a temporary table `vt_payaways_ftp_ranks` that ranks insights related to first-time payaway transactions.
					-- Ranking is based on:
					--   - Earliest transaction date (most recent first)
					--   - Latest transaction date
					--   - Total transaction amount
					--   - Number of payments
					CREATE OR REPLACE TEMPORARY TABLE vt_payaways_ftp_ranks
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		A.INSG_I,
		ROW_NUMBER() OVER (
		  ORDER BY
		    ERLY_TRAN_D_90 DESC NULLS LAST,
		    LATEST_TRAN_D_90 DESC NULLS LAST,
		    TOTAL_TRANSACTION_AMOUNT DESC NULLS LAST,
		    NUM_PAYMENTS_90 DESC NULLS LAST
		) AS Ranking1
FROM
		vt_payaways_descriptions a
WHERE
		UPPER(RTRIM(
		SUB_INSG_TYPE_M)) = UPPER(RTRIM('FIRST_TIME_PAYAWAY'))
					       )
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Creates a temporary table `vt_payaways_nftp_recurring_ranking_rule` to rank recurring payaway behaviours that are not first-time.
			-- Criteria:
			--   - At least 3 payments in the last 90 days
			--   - Associated with a known brand or bank
			-- Ranking is based on:
			--   - Latest transaction date
			--   - Total transaction amount
			--   - Number of payments
			--   - Absolute value of the latest transaction amount
			-- This helps identify strong recurring payment patterns.
					CREATE OR REPLACE TEMPORARY TABLE vt_payaways_nftp_recurring_ranking_rule
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		b.INSG_I,
		ROW_NUMBER() OVER (
		  ORDER BY
		    LATEST_TRAN_D_90 DESC NULLS LAST,
		    TOTAL_TRANSACTION_AMOUNT DESC NULLS LAST,
		    NUM_PAYMENTS_90 DESC NULLS LAST, ABS(LATEST_TRAN_AMT) DESC NULLS LAST
		) AS Ranking1
FROM
		vt_payaways_descriptions b
WHERE
		SUB_INSG_TYPE_M IS NULL
		AND NUM_PAYMENTS_90 >= 3
		AND (
		  TRAN_BRND_M IS NOT NULL
		  OR OFI_BANK_LIST IS NOT NULL
		)
					      )
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Creates a temporary table `vt_payaways_nftp_nonrecurring_ranking_rule` to rank non-recurring payaway behaviours that are not first-time.
					-- Criteria:
					--   - Fewer than 3 payments in the last 90 days
					--   - Associated with a known brand or bank
					-- Ranking is based on similar metrics as recurring payaways.
					-- This helps surface occasional or one-off payment behaviours.
					CREATE OR REPLACE TEMPORARY TABLE vt_payaways_nftp_nonrecurring_ranking_rule
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		c.INSG_I,
		ROW_NUMBER() OVER (
		  ORDER BY
		   LATEST_TRAN_D_90 DESC NULLS LAST,
		    TOTAL_TRANSACTION_AMOUNT DESC NULLS LAST,
		    NUM_PAYMENTS_90 DESC NULLS LAST, ABS(LATEST_TRAN_AMT) DESC NULLS LAST
		) AS Ranking1
FROM
		vt_payaways_descriptions c
WHERE
		SUB_INSG_TYPE_M IS NULL
		AND NUM_PAYMENTS_90 < 3
		AND (
		  TRAN_BRND_M IS NOT NULL
		  OR OFI_BANK_LIST IS NOT NULL
		)
					   )
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Creates a temporary table `vt_payaways_nftp_missing_fields_ranking_rules` to rank payaway behaviours with missing brand and bank information.
					-- Criteria:
					--   - No brand or bank info available
					--   - Not a first-time payaway
					-- Ranking is based on:
					--   - Latest transaction date
					--   - Total transaction amount
					--   - Number of payments
					--   - Absolute value of the latest transaction amount
					-- This helps identify potentially incomplete or ambiguous payment records.
					CREATE OR REPLACE TEMPORARY TABLE vt_payaways_nftp_missing_fields_ranking_rules
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		d.INSG_I,
		ROW_NUMBER() OVER (
		  ORDER BY
		    LATEST_TRAN_D_90 DESC NULLS LAST,
		    TOTAL_TRANSACTION_AMOUNT DESC NULLS LAST,
		    NUM_PAYMENTS_90 DESC NULLS LAST, ABS(LATEST_TRAN_AMT) DESC NULLS LAST
		) AS Ranking1
FROM
		vt_payaways_descriptions d
WHERE
		SUB_INSG_TYPE_M IS NULL
		AND TRAN_BRND_M IS NULL
		AND OFI_BANK_LIST IS NULL
					   )
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Creates a temporary table `vt_payaways_final_ranks` that consolidates rankings from all previous ranking tables.
					-- Each source table is assigned a `source_order` to maintain priority:
					--   1 = First-time payaways
					--   2 = Recurring non-first-time
					--   3 = Non-recurring non-first-time
					--   4 = Missing brand/bank info
					-- Final ranking (`RowNum`) is assigned based on source priority and individual rank.
					-- This unified ranking supports consistent prioritisation across all payaway types.
					CREATE OR REPLACE TEMPORARY TABLE vt_payaways_final_ranks
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	WITH CTE AS (
		SELECT
					A.INSG_I,
					ROW_NUMBER() OVER (
					         ORDER BY
					           Ranking1
					       ) as RANK1,
					1 AS source_order FROM
					vt_payaways_ftp_ranks A
		   UNION ALL
		SELECT
					B.INSG_I,
					ROW_NUMBER() OVER (
		         ORDER BY
		           B.Ranking1
		       ) as RANK1,
					2 AS source_order FROM
					vt_payaways_nftp_recurring_ranking_rule B
		   UNION ALL
		SELECT
					C.INSG_I,
					ROW_NUMBER() OVER (
		         ORDER BY
		           C.Ranking1
		       ) as RANK1,
					3 AS source_order FROM
					vt_payaways_nftp_nonrecurring_ranking_rule C
		   UNION ALL
		SELECT
					D.INSG_I,
					ROW_NUMBER() OVER (
		         ORDER BY
		           D.Ranking1
		       ) as RANK1,
					4 AS source_order FROM
					vt_payaways_nftp_missing_fields_ranking_rules D
	)
	SELECT
		INSG_I,
		ROW_NUMBER() OVER (ORDER BY source_order ASC NULLS FIRST,RANK1 ASC NULLS FIRST) AS RowNum
	FROM CTE
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Creates a final table `vt_payaways_rankings` that merges descriptive payaway insights with their computed rankings.
					-- Combines:
					--   - Insight type and subtype
					--   - Customer and portfolio identifiers
					--   - Payment details (amounts, dates, accounts)
					--   - Short and long descriptions
					--   - Ranking number (`RANK_N`)
					--   - Status and feedback placeholders
					-- This table is ready for delivery, reporting, or further processing in downstream systems.
					CREATE OR REPLACE TEMPORARY TABLE vt_payaways_rankings
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		CAST(LEFT(
		CAST(A.INSG_TYPE_M AS VARCHAR), 60) AS VARCHAR(60)) AS INSG_TYPE_M,
		CAST(LEFT(
		CAST(A.SUB_INSG_TYPE_M AS VARCHAR), 100) AS VARCHAR(100)) AS SUB_INSG_TYPE_M,
		CAST(TRUNC(B.RowNum) AS INTEGER) !!!RESOLVE EWI!!! /*** SSC-EWI-TD0041 - TRUNC FUNCTION WAS ADDED TO ENSURE INTEGER. MAY NEED CHANGES IF NOT NUMERIC OR STRING. ***/!!! AS RANK_N,
		A.PRTF_I,
		A.CUST_M,
		A.PATY_I,
		A.PATY_GRUP_I,
		A.TRAN_DETL_CATG_M,
		A.TRAN_BRND_M,
		A.OFI_BANK_LIST,
		A.NEED_M,
		A.NUM_PAYMENTS_90 AS NUMBER_OF_PAYMENTS,
		A.TOTAL_TRANSACTION_AMOUNT AS TRAN_A,
		A.LATEST_TRAN_D_90 AS LATEST_TRAN_D,
		A.ERLY_TRAN_D_90,
		CAST(LEFT(
		CAST(A.SHRT_DESC_X AS VARCHAR), 600) AS VARCHAR(600)) AS SHRT_DESC_X,
		CAST(LEFT(
		CAST(A.LONG_DESC_X AS VARCHAR), 1000) AS VARCHAR(1000)) AS LONG_DESC_X,
		CAST('UNDELIVERED' AS VARCHAR(40)) AS STUS_M,
		A.CRAT_D,
		CAST(NULL AS VARCHAR(255)) AS FDBK_X,
		A.EXPY_D,
		A.INSG_I
FROM
		vt_payaways_descriptions A
LEFT JOIN
					vt_payaways_final_ranks B
		ON A.INSG_I = B.INSG_I
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;


--- Creating table to standardise all insights
-- TO DO: ADD union of the predicted needs and cashflow shortfall

  drop table vt_payaways_descriptions;
  drop table vt_payaways_ftp_ranks;
  drop table vt_payaways_nftp_recurring_ranking_rule;
  drop table vt_payaways_nftp_nonrecurring_ranking_rule;
  drop table vt_payaways_nftp_missing_fields_ranking_rules;
  drop table vt_payaways_final_ranks;

					---- ADDING IN PREDICTED NEEDS PROCESS WHICH ENTAILS THE FOLLOWIGN

					--- Step 1: Creating the filtered scores volatile table
					--** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_PRED_NEED_BASE" **
					CREATE OR REPLACE TEMPORARY TABLE vt_pn_filtered_scores
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	/*
	  Calculate Percentiles for all scores in the base table.
	  Keep rows which have scores in the 'High' decile/catergory
	*/
	WITH ranked_cte AS (
		SELECT
					a.*,
					ROW_NUMBER() OVER (
					    PARTITION BY
					        MODL_I
					    ORDER BY
					        SCOR_CURR DESC NULLS LAST
					) AS rn_curr,
					ROW_NUMBER() OVER (
					    PARTITION BY
					        MODL_I
					    ORDER BY
					        SCOR_PREV DESC NULLS LAST
					) AS rn_prev,
					COUNT(*) OVER (
					    PARTITION BY
					        MODL_I
					) AS cnt
	FROM
					U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_PRED_NEED_BASE a
	WHERE
					UPPER(RTRIM(CAST(RPAD( CAST(SCOR_CURR AS CHAR(20)), 20) AS CHAR(20)))) <> UPPER(RTRIM('********************')) AND SCOR_CURR IS NOT NULL
	        ),
	        decile_cte AS (
		SELECT
					a.*,
					CEIL(100 * rn_curr / cnt) AS PERC_CURR,
					CEIL(100 * rn_prev / cnt) AS PERC_PREV
	FROM
					ranked_cte a
	        ),
	        hml_cte AS (
		SELECT
					a.*,
					CASE
						WHEN PERC_CURR <= 5
							THEN 'H'
						WHEN PERC_CURR > 5
						                   AND PERC_CURR < 70
							THEN 'M'
					    ELSE 'L'
					END AS HML_CURR,
					CASE
						WHEN PERC_PREV <= 5
							THEN 'H'
						WHEN PERC_PREV > 5
						                   AND PERC_PREV < 70
							THEN 'M'
					    ELSE 'L'
					END AS HML_PREV
	FROM
					decile_cte a
	        )
	SELECT
		* FROM hml_cte
	        WHERE
		UPPER(RTRIM( HML_CURR)) = UPPER(RTRIM('H'))
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					--- Step 2: Enrich base table with customer details and consolia
					--** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "U_V_DSV_001_QPD_1.CUST_DIMN" **
					CREATE OR REPLACE TEMPORARY TABLE vt_pn_cust_detl
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	/*
	  Group together needs (eg. 4 Asset Finance Expansion Models --> 1 Asset Finance Need, etc)
	  Join customer details such as Name, Paty, Portfolio
	*/
	SELECT
	    a.PATY_GRUP_I,
		a.MODL_I,
		a.SCOR_CURR,
		a.SCOR_PREV,
		a.SCOR_CURR_D,
		a.SCOR_PREV_D,
		a.SEGM_C,
		a.TOP_FEAT_M,
		a.TOP_FEAT_X,
		a.PERC_CURR,
		a.PERC_PREV,
		a.HML_CURR,
		a.HML_PREV,
		b.CUSTOMER_NAME AS CUST_M,
		b.PATY_I AS PATY_I,
		b.PRTF_IDNN_BK AS PRTF_I,
		CASE
					WHEN UPPER(RTRIM(a.MODL_I)) IN (UPPER(RTRIM('BB_CEE_af_car_exp_eu')), UPPER(RTRIM('BB_CEE_af_car_exp_ex_2021')), UPPER(RTRIM('BB_CEE_af_equip_EU_needs')), UPPER(RTRIM('BB_CEE_af_equip_EX_needs')))
						THEN 'New Asset Finance Product'
					WHEN UPPER(RTRIM(a.MODL_I)) IN (UPPER(RTRIM('BB_CEE_merc_exp_new_EU')), UPPER(RTRIM('BB_CEE_merc_exp_new_EX')))
						THEN 'New Merchant Product'
					WHEN UPPER(RTRIM(a.MODL_I)) IN (UPPER(RTRIM('BB_CEE_bbl_exp_eu')), UPPER(RTRIM('BB_CEE_bbl_exp_ex')))
						THEN 'New Term Loan'
					WHEN UPPER(RTRIM(a.MODL_I)) = UPPER(RTRIM('BB_CEE_af_ret'))
						THEN 'Retain existing Asset Finance Product'
					WHEN UPPER(RTRIM(a.MODL_I)) = UPPER(RTRIM('BB_CEE_BBL_retention_final_enhanced'))
						THEN 'Retain existing Term Loan'
					WHEN UPPER(RTRIM(a.MODL_I)) = UPPER(RTRIM('BB_CEE_merc_ret_new'))
						THEN 'Retain existing Merchant Product'
					WHEN UPPER(RTRIM(a.MODL_I)) = UPPER(RTRIM('BB_IMT_H2O'))
						THEN 'IMT'
					WHEN UPPER(RTRIM(a.MODL_I)) = UPPER(RTRIM('Working Capital'))
						THEN 'New Working Capital Product'
					WHEN UPPER(RTRIM(a.MODL_I)) = UPPER(RTRIM('BB_CEE_bta_ret_new'))
						THEN 'Retain existing Transaction Account'
		  ELSE
		    a.MODL_I
		END AS MODEL_NAME
FROM
		vt_pn_filtered_scores a
		JOIN
					U_V_DSV_001_QPD_1.CUST_DIMN b ON a.PATY_GRUP_I = b.PATY_GRUP_I
		    AND UPPER(RTRIM( b.GRUP_PRIM_PATY_F)) = UPPER(RTRIM('Y'))
WHERE
		1 = 1
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					--- Step 3: Table for model definitions
					CREATE OR REPLACE TEMPORARY TABLE vt_pn_model_def
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	/*
	  Add model definitions and boilerplate text for each row
	  Get unique combinations of Sales group and Model Need (ie. Filter out having multiple models mapping to one need)
	*/
	SELECT
	    a.*,
		CASE
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_af_car_exp_ex_2021'))
						THEN DATEADD(MONTH, 5, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_af_car_exp_eu'))
						THEN DATEADD(MONTH, 5, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_af_equip_EX_needs'))
						THEN DATEADD(MONTH, 5, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_af_equip_EU_needs'))
						THEN DATEADD(MONTH, 5, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_af_ret'))
						THEN DATEADD(MONTH, 5, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_bbl_exp_eu'))
						THEN DATEADD(MONTH, 5, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_bbl_exp_ex'))
						THEN DATEADD(MONTH, 5, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_BBL_retention_final_enhanced'))
						THEN DATEADD(MONTH, 5, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_bta_ret_new'))
						THEN CAST(a.SCOR_CURR_D AS DATE) + INTERVAL '168 DAY'
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_merc_exp_new_EU'))
						THEN DATEADD(MONTH, 4, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_merc_exp_new_EX'))
						THEN DATEADD(MONTH, 4, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_CEE_merc_ret_new'))
						THEN DATEADD(MONTH, 4, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('BB_IMT_H2O'))
						THEN DATEADD(MONTH, 4, a.SCOR_CURR_D)
					WHEN UPPER(RTRIM(MODL_I)) = UPPER(RTRIM('Working Capital'))
						THEN DATEADD(MONTH, 4, a.SCOR_CURR_D)
	      ELSE NULL
		END AS MODEL_EXPIRY,
		CASE
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('Retain existing Term Loan'))
						THEN 'is forecasted to discharge a Term Loan product'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('New Term Loan'))
						THEN 'is forecasted to have a Term Loan need'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('New Working Capital Product'))
						THEN 'is forecasted to have a Working Capital need'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('IMT'))
						THEN 'is forecasted to perform an outgoing IMT'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('New Asset Finance Product'))
						THEN 'is forecasted to have need for a new Asset Finance Vehicle or Equipment product'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('Retain existing Merchant Product'))
						THEN 'is forecasted to close a merchant product'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('Retain existing Asset Finance Product'))
						THEN 'is forecasted to close an Asset Finance account'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('New Merchant Product'))
						THEN 'is forecasted to have a need for a new merchant product'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('Retain existing Transaction Account'))
						THEN 'is forecasted to have an inactive Business Transaction Account'
	      ELSE NULL
		END AS MODEL_DEFINITIONS,
		CASE
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('Retain existing Term Loan'))
						THEN 'Higher interest rate and high digital views on the account statement page'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('New Term Loan'))
						THEN 'Existing CBA commercial lending balance and high value of credit transaction in the last 30 days'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('New Working Capital Product'))
						THEN 'Value/volume of total transactions through Business Transaction Accounts and Business Transaction Account minimum balance trends'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('IMT'))
						THEN 'Online activity, including interactions with the send money overseas digital page and the value/volume of debit transactions through Business Transaction Accounts'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('New Asset Finance Product'))
						THEN 'High number of debit transactions and high value of credit turnover in all cheque accounts held by the customer'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('Retain existing Merchant Product'))
						THEN 'Number of merchant products held by the customer and low volume/value of transactions in the last 30 days'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('Retain existing Asset Finance Product'))
						THEN 'Difference between the customers maximum & minimum interest rates and likely probability of loss (based on the borrowers behaviour, such as payment history or credit score)'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('New Merchant Product'))
						THEN 'Low number of days with an overdrawn Business Account and higher number/value of credit transactions'
					WHEN UPPER(RTRIM(a.MODEL_NAME)) = UPPER(RTRIM('Retain existing Transaction Account'))
						THEN 'Decrease in transaction volumes and/or values and decrease in the number of accounts with linked debit cards'
	      ELSE NULL
		END AS MODEL_GENERAL_FEATS,
		CASE
					WHEN TOP_FEAT_M IS NULL
						THEN NULL
					WHEN TOP_FEAT_M ILIKE '% is high'
						THEN TRIM('High ' || SUBSTRING(TOP_FEAT_M, 1, LENGTH(TOP_FEAT_M) - 7))
	      ELSE TRIM(TOP_FEAT_M)
		END AS DRIVER_FORMATTED,
		CASE
					WHEN TOP_FEAT_X IS NULL
						THEN NULL
					WHEN POSITION('These drivers look at ' IN (TOP_FEAT_X)) = 1
						THEN
						-- Remove prefix, uppercase first char after prefix, lowercase rest, remove final period and append text
						RTRIM(UPPER(SUBSTRING(TOP_FEAT_X, 23, 1)) -- First char after prefix
						  || LOWER(SUBSTRING(TOP_FEAT_X, 24))    -- Rest of the sentence
						, '.')
					WHEN POSITION('These drivers predict ' IN (TOP_FEAT_X)) = 1
						THEN RTRIM(UPPER(SUBSTRING(TOP_FEAT_X, 23, 1)) || LOWER(SUBSTRING(TOP_FEAT_X, 24)), '.')
	    ELSE
					-- For everything else, just remove final period and append text
					RTRIM(TOP_FEAT_X, '.')
		END AS DESC_FORMATTED
	  FROM (
					SELECT
						b.*,
						ROW_NUMBER() OVER (
						  PARTITION BY
						    PATY_GRUP_I,
						    MODEL_NAME
						  ORDER BY
						    PERC_CURR ASC NULLS FIRST,
						    SCOR_CURR DESC NULLS LAST,
						    PERC_PREV ASC NULLS FIRST,
						    SCOR_PREV DESC NULLS LAST
						) AS rn
				FROM
						vt_pn_cust_detl b
	        ) a
	  WHERE a.rn = 1
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					--- Step 4: Table for increasing propensity
					CREATE OR REPLACE TEMPORARY TABLE vt_pn_increasing_propensity
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		a.*,
		CUST_M || ' ' || MODEL_DEFINITIONS || '.' AS SHRT_DESC_X,
		CASE
					WHEN TOP_FEAT_X IS NOT NULL
						THEN 'This is based on ' || CUST_M || ' having ' || DRIVER_FORMATTED || '. Key indicator supporting this forecast: ' || CHR(10) || DESC_FORMATTED || '.'
					WHEN TOP_FEAT_X IS NULL
						THEN 'The AI model that supports this prediction takes into account the customer''s ' || LOWER(SUBSTR(MODEL_GENERAL_FEATS, 1, 1)) || SUBSTR(MODEL_GENERAL_FEATS, 2) || '.'
		  ELSE CAST(NULL AS VARCHAR(1000))
		END AS LONG_DESC_X,
		a.MODEL_EXPIRY AS EXPY_D,
		CAST('INCREASING_PROPENSITY' AS VARCHAR(100)) AS SUB_INSG_TYPE_M
FROM
		vt_pn_model_def a
WHERE
		UPPER(RTRIM( HML_CURR)) = UPPER(RTRIM('H')) AND UPPER(RTRIM( HML_PREV)) = UPPER(RTRIM('L'))
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					--- Step 5: Creating the multiple needs table
					CREATE OR REPLACE TEMPORARY TABLE vt_pn_multiple_needs
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		t.PATY_GRUP_I,
		CAST('MULTIPLE_NEEDS' AS VARCHAR(100)) AS SUB_INSG_TYPE_M,
		MAX(t.CUST_M) AS CUST_M,
		MAX(t.PRTF_I) AS PRTF_I,
		MAX(t.SEGM_C) AS SEGM_C,
		MAX(t.PATY_I) AS PATY_I,
		MIN(PERC_CURR) AS PERC_CURR,
		MAX(SCOR_CURR) AS SCOR_CURR,
		MIN(PERC_PREV) AS PERC_PREV,
		MAX(SCOR_PREV) AS SCOR_PREV,
		MAX(SCOR_CURR_D) AS SCOR_CURR_D,
		MAX(SCOR_PREV_D) AS SCOR_PREV_D,
		MAX(MODEL_EXPIRY) AS EXPY_D,
		COUNT(*) AS NUM_NEEDS,
		CASE
					WHEN COUNT(*) = 2
						THEN
						'The top ' || CAST(LEFT(TO_VARCHAR(COUNT(*), 'TM'), 10) AS VARCHAR(10)) || ' predicted needs for ' || MAX(CUST_M) || ' are ' || MAX(CASE
							WHEN t.rnk = 1
											THEN t.MODEL_NAME
						END) || ' and ' || MAX(CASE
							WHEN t.rnk = 2
											THEN t.MODEL_NAME
						END) || '.'
					WHEN COUNT(*) = 3
						THEN
						'The top ' || CAST(LEFT(TO_VARCHAR(COUNT(*), 'TM'), 10) AS VARCHAR(10)) || ' predicted needs for ' || MAX(CUST_M) || ' are ' || MAX(CASE
							WHEN t.rnk = 1
											THEN t.MODEL_NAME
						END) || ', ' || MAX(CASE
							WHEN t.rnk = 2
											THEN t.MODEL_NAME
						END) || ' and '
						|| MAX(CASE
							WHEN t.rnk = 3
											THEN t.MODEL_NAME
						END) || '.'
		END AS SHRT_DESC_X,
		CASE
					WHEN COUNT(*) = 2
		  AND MAX(CASE
						WHEN t.rnk = 1
							THEN t.TOP_FEAT_M
					END) IS NOT NULL
		  AND MAX(CASE
						WHEN t.rnk = 2
							THEN t.TOP_FEAT_M
					END) IS NOT NULL
						THEN
						'These needs are driven by ' || MAX(CUST_M) || '''s following activities:'
						|| CHR(10) || CHR(10) ||
						'- ' || MAX(CASE
							WHEN t.rnk = 1
											THEN t.MODEL_NAME
						END) || ': ' || MAX(CASE
							WHEN t.rnk = 1
											THEN t.DESC_FORMATTED
						END) || CHR(10) ||
						'- ' || MAX(CASE
							WHEN t.rnk = 2
											THEN t.MODEL_NAME
						END) || ': ' || MAX(CASE
							WHEN t.rnk = 2
											THEN t.DESC_FORMATTED
						END)
					WHEN COUNT(*) = 3
		  AND MAX(CASE
						WHEN t.rnk = 1
							THEN t.TOP_FEAT_M
					END) IS NOT NULL
		  AND MAX(CASE
						WHEN t.rnk = 2
							THEN t.TOP_FEAT_M
					END) IS NOT NULL
		  AND MAX(CASE
						WHEN t.rnk = 3
							THEN t.TOP_FEAT_M
					END) IS NOT NULL
						THEN
						'These needs are driven by ' || MAX(CUST_M) || '''s following activities:'
						|| CHR(10) || CHR(10) ||
						'- ' || MAX(CASE
							WHEN t.rnk = 1
											THEN t.MODEL_NAME
						END) || ': ' || MAX(CASE
							WHEN t.rnk = 1
											THEN t.DESC_FORMATTED
						END) || CHR(10) ||
						'- ' || MAX(CASE
							WHEN t.rnk = 2
											THEN t.MODEL_NAME
						END) || ': ' || MAX(CASE
							WHEN t.rnk = 2
											THEN t.DESC_FORMATTED
						END) || CHR(10) ||
						'- ' || MAX(CASE
							WHEN t.rnk = 3
											THEN t.MODEL_NAME
						END) || ': ' || MAX(CASE
							WHEN t.rnk = 3
											THEN t.DESC_FORMATTED
						END)
					WHEN COUNT(*) = 2
		  AND (MAX(CASE
						WHEN t.rnk = 1
							THEN t.TOP_FEAT_M
					END) IS NULL
		  OR MAX(CASE
						WHEN t.rnk = 2
							THEN t.TOP_FEAT_M
					END) IS NULL)
						THEN
						'The AI models that support these predictions may consider the following information:' || CHR(10) || CHR(10) ||
						'- ' || MAX(CASE
							WHEN t.rnk = 1
											THEN t.MODEL_NAME
						END) || ': ' || MAX(CASE
							WHEN t.rnk = 1
											THEN COALESCE(t.DESC_FORMATTED, t.MODEL_GENERAL_FEATS)
						END) || CHR(10) ||
						'- ' || MAX(CASE
							WHEN t.rnk = 2
											THEN t.MODEL_NAME
						END) || ': ' || MAX(CASE
							WHEN t.rnk = 2
											THEN COALESCE(t.DESC_FORMATTED, t.MODEL_GENERAL_FEATS)
						END)
					WHEN COUNT(*) = 3
		  AND (MAX(CASE
						WHEN t.rnk = 1
							THEN t.TOP_FEAT_M
					END) IS NULL
		  OR MAX(CASE
						WHEN t.rnk = 2
							THEN t.TOP_FEAT_M
					END) IS NULL
		  OR MAX(CASE
						WHEN t.rnk = 3
							THEN t.TOP_FEAT_M
					END) IS NULL)
						THEN
						'The AI models that support these predictions may consider the following information:' || CHR(10) || CHR(10) ||
						'- ' || MAX(CASE
							WHEN t.rnk = 1
											THEN t.MODEL_NAME
						END) || ': ' || MAX(CASE
							WHEN t.rnk = 1
											THEN COALESCE(t.DESC_FORMATTED, t.MODEL_GENERAL_FEATS)
						END) || CHR(10) ||
						'- ' || MAX(CASE
							WHEN t.rnk = 2
											THEN t.MODEL_NAME
						END) || ': ' || MAX(CASE
							WHEN t.rnk = 2
											THEN COALESCE(t.DESC_FORMATTED, t.MODEL_GENERAL_FEATS)
						END) || CHR(10) ||
						'- ' || MAX(CASE
							WHEN t.rnk = 3
											THEN t.MODEL_NAME
						END) || ': ' || MAX(CASE
							WHEN t.rnk = 3
											THEN COALESCE(t.DESC_FORMATTED, t.MODEL_GENERAL_FEATS)
						END)
		END AS LONG_DESC_X
FROM (
					SELECT
						m.*,
						ROW_NUMBER() OVER (
						    PARTITION BY m.PATY_GRUP_I
						    ORDER BY m.PERC_CURR ASC NULLS FIRST, m.SCOR_CURR DESC NULLS LAST, m.PERC_PREV ASC NULLS FIRST, m.SCOR_PREV DESC NULLS LAST
						) AS rnk
		FROM
						vt_pn_model_def m
		WHERE
						(m.PATY_GRUP_I, m.MODEL_NAME) NOT IN (
							SELECT
											PATY_GRUP_I,
											MODEL_NAME FROM
											vt_pn_increasing_propensity
		)
) t
WHERE t.rnk <= 3
GROUP BY t.PATY_GRUP_I
HAVING
		COUNT(*) > 1
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					--- Step 6: Creating a high needs table
					CREATE OR REPLACE TEMPORARY TABLE vt_pn_high_need
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		a.*,
		CUST_M || ' ' || MODEL_DEFINITIONS || '.' AS SHRT_DESC_X,
		CASE
					WHEN TOP_FEAT_X IS NOT NULL
						THEN 'This is based on ' || CUST_M || ' having ' || DRIVER_FORMATTED || '. Key indicator supporting this forecast: ' || CHR(10) || DESC_FORMATTED || '.'
					WHEN TOP_FEAT_X IS NULL
						THEN 'The AI model that supports this prediction takes into account the customer''s ' || LOWER(SUBSTR(MODEL_GENERAL_FEATS, 1, 1)) || SUBSTR(MODEL_GENERAL_FEATS, 2) || '.'
		  ELSE CAST(NULL AS VARCHAR(1000))
		END AS LONG_DESC_X,
		CAST('HIGH_NEED' AS VARCHAR(200)) AS SUB_INSG_TYPE_M,
		a.MODEL_EXPIRY AS EXPY_D
FROM
		vt_pn_model_def a
WHERE
		(PATY_GRUP_I, MODL_I) NOT IN (
					SELECT
						PATY_GRUP_I,
						MODEL_NAME
				FROM
						vt_pn_increasing_propensity
		)
AND PATY_GRUP_I IN (
					SELECT
						PATY_GRUP_I
				FROM
						vt_pn_model_def a
				GROUP BY PATY_GRUP_I
				HAVING
						COUNT(*) = 1
)
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					--- Step 7: Creating combined table with all three insight areas
					CREATE OR REPLACE TEMPORARY TABLE vt_pn_combined_insights
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		SUB_INSG_TYPE_M,
		PRTF_I,
		CUST_M,
		PATY_I,
		PATY_GRUP_I,
		SHRT_DESC_X,
		LONG_DESC_X,
		EXPY_D,
		CAST(NULL AS INTEGER) AS NUM_NEEDS,
		SCOR_CURR,
		SCOR_PREV,
		PERC_CURR,
		PERC_PREV,
		MODL_I
FROM
		vt_pn_increasing_propensity

	 UNION ALL
	SELECT
	   SUB_INSG_TYPE_M,
		PRTF_I,
		CUST_M,
		PATY_I,
		PATY_GRUP_I,
		SHRT_DESC_X,
		LONG_DESC_X,
		EXPY_D,
		NUM_NEEDS,
		SCOR_CURR,
		SCOR_PREV,
		PERC_CURR,
		PERC_PREV,
		CAST(NULL AS VARCHAR(100)) AS MODL_I
FROM
		vt_pn_multiple_needs

	 UNION ALL
	SELECT
	   SUB_INSG_TYPE_M,
		PRTF_I,
		CUST_M,
		PATY_I,
		PATY_GRUP_I,
		SHRT_DESC_X,
		LONG_DESC_X,
		EXPY_D,
		CAST(NULL AS INTEGER) AS NUM_NEEDS,
		SCOR_CURR,
		SCOR_PREV,
		PERC_CURR,
		PERC_PREV,
		MODL_I
FROM
		vt_pn_high_need
)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Step 8: Ranked Predicted Needs output
					--** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "PFTCF.HASH_MD5" **
					CREATE OR REPLACE TEMPORARY TABLE vt_pn_ranked_insights
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		a.*,
		  a.PATY_GRUP_I || a.SUB_INSG_TYPE_M || a.MODL_I AS hash_modl_i,
		  a.PATY_GRUP_I || a.SUB_INSG_TYPE_M AS hash_mn,
		CASE
					WHEN UPPER(RTRIM(SUB_INSG_TYPE_M)) IN (UPPER(RTRIM('INCREASING_PROPENSITY')), UPPER(RTRIM('HIGH_NEED')))
						THEN MD5(hash_modl_i)
					WHEN UPPER(RTRIM(SUB_INSG_TYPE_M)) = UPPER(RTRIM('MULTIPLE_NEEDS'))
						THEN MD5(hash_mn)
		END AS INSG_I,
		ROW_NUMBER() OVER (
		      ORDER BY CASE
					a.SUB_INSG_TYPE_M
					WHEN 'INCREASING_PROPENSITY'
						THEN 1
					WHEN 'MULTIPLE_NEEDS'
						THEN 2
					WHEN 'HIGH_NEED'
						THEN 3
		              ELSE 4
		END, COALESCE(a.NUM_NEEDS, 0) DESC NULLS LAST,
		          a.PERC_CURR ASC NULLS FIRST,
		          a.SCOR_CURR DESC NULLS LAST,
		          a.PERC_PREV ASC NULLS FIRST,
		          a.SCOR_PREV DESC NULLS LAST
		  ) AS RANK_N
FROM
		vt_pn_combined_insights a
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					--- Step 9: Final Predicted Needs Output
					CREATE OR REPLACE TEMPORARY TABLE vt_pn_final
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		INSG_I,
		CAST('PREDICTED_NEEDS' AS VARCHAR(60)) AS INSG_TYPE_M,
		SUB_INSG_TYPE_M,
		CAST(RANK_N AS INTEGER) AS RANK_N,
		PRTF_I,
		CUST_M,
		PATY_I,
		PATY_GRUP_I,
		CAST(LEFT(SHRT_DESC_X, 600) AS VARCHAR(600)) AS SHRT_DESC_X,
		CAST(LEFT(LONG_DESC_X, 1000) AS VARCHAR(1000)) AS LONG_DESC_X,
		CAST('UNDELIVERED' AS VARCHAR(40)) AS STUS_M,
		CURRENT_DATE() AS CRAT_D,
		EXPY_D,
		CAST(NULL AS VARCHAR(255)) AS FDBK_X
FROM
		vt_pn_ranked_insights
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

drop table vt_pn_filtered_scores;
drop table vt_pn_cust_detl;
drop table vt_pn_model_def;
drop table vt_pn_increasing_propensity;
drop table vt_pn_multiple_needs;
drop table vt_pn_high_need;
drop table vt_pn_combined_insights;
drop table vt_pn_ranked_insights;

					---- Adding the cashflow logic

					-- Step 1: Join with CUST DIMN to get Customer Names and filter for CB, MCG, RAG, SBB segments.


					-- drop table cashflow_shortfall_description;
					--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "OREPLACE", "U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_CSHF_SHRF", "U_D_DSV_001_QPD_1.CUST_DIMN" **
					CREATE OR REPLACE TEMPORARY TABLE cashflow_shortfall_description
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS
					(
	SELECT
		A.*,
		B.customer_name,
		B.PRTF_IDNN_BK AS PRTF_I,
		REPLACE(A.INSG_SHRT_X, '<NAME>', COALESCE(B.customer_name, '')) AS short_description_updated,
		CASE
					WHEN UPPER(RTRIM(A.TRAN_DATE_FLAG)) = UPPER(RTRIM('True'))
						THEN REPLACE(A.INSG_LONG_X, '<NAME>', COALESCE(B.customer_name, '')) || CHR(10) || A.CUST_ACCT_LIST_X || CHR(10) || '- Data as at: ' || A.RUN_DATE_X
		    ELSE REPLACE(A.INSG_LONG_X, '<NAME>', COALESCE(b.customer_name, '')) || CHR(10) || A.CUST_ACCT_LIST_X
		END AS long_description_updated,
		REPLACE(a.INSG_PATY_GRUP_LEVL_X, '<NAME>', COALESCE(b.customer_name, '')) AS sales_group_lvl_updated
FROM
		U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_CSHF_SHRF A
JOIN
					U_D_DSV_001_QPD_1.CUST_DIMN B ON A.PATY_IDNN_BK = B.PATY_IDNN_BK
where
		UPPER(RTRIM( B.FINAL_SEGMENT)) IN (UPPER(RTRIM('CB')), UPPER(RTRIM('MCG')), UPPER(RTRIM('RAB')), UPPER(RTRIM('SBB')))
					)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Step 2: Rollup CIF Insights to SG

					-- drop table grouped_descriptions;
					CREATE OR REPLACE TEMPORARY TABLE grouped_descriptions
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		PATY_GRUP_I,
'Across the ' || PATY_GRUP_I || ' Sales Group, over the past 5 weeks: ' || CHR(10) || CAST(LEFT(CAST(LISTAGG ( CONCAT_WS('', '- ', TRIM(sales_group_lvl_updated)), CHR(10))
		WITHIN GROUP(
 ORDER BY rn) AS VARCHAR), 1000) AS VARCHAR(1000)) AS combined_description

FROM (
					SELECT
						PATY_GRUP_I,
						sales_group_lvl_updated,
						ROW_NUMBER() OVER (PARTITION BY PATY_GRUP_I ORDER BY PATY_IDNN_BK) AS rn
				FROM
						cashflow_shortfall_description
				WHERE
						UPPER(RTRIM(UPPER(MLTI_INSG_F))) = UPPER(RTRIM('True'))
) t
GROUP BY PATY_GRUP_I
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Step 3: Create the non_multiple_parties table directly with data

					-- drop table non_multiple_parties;
					CREATE OR REPLACE TEMPORARY TABLE non_multiple_parties
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		CAST(LEFT(
		CAST(t.PATY_IDNN_BK AS VARCHAR), 100) AS VARCHAR(100)) AS PATY_IDNN_BK,
		t.customer_name,
		t.PATY_GRUP_I,
		t.CNSV_DCLN_BALN_N,
		t.LTST_WEEK_BALN_A,
		t.SCND_LTST_WEEK_BALN_A,
		t.MIN_BALN_DROP_R,
		t.MIN_BALN_DROP_A,
		t.INFW_DROP_R,
		t.INFW_DROP_A,
		t.OUTF_CHNG_R,
		t.AVRG_OUTF_DIFF_A,
		t.AVRG_BALN_DROP_R,
		t.AVRG_BALN_DROP_A,
		t.CR_TRAN_Q,
		t.DR_TRAN_Q,
		t.UPDT_D,
		t.PRTF_I,
		CAST(t.FIRST_TRAN_D AS DATE) AS FIRST_TRAN_D,
		CAST(LEFT(
		CAST(t.INSG_I AS VARCHAR), 16) AS VARCHAR(16)) AS INSG_I,
		CAST(LEFT(
		CAST(t.SUB_INSG_TYP_C AS VARCHAR), 26) AS VARCHAR(26)) AS SUB_INSG_TYP_C,
		CAST(LEFT(
		CAST(t.DCLN_BALN_TRND_F AS VARCHAR), 5) AS VARCHAR(5)) AS DCLN_BALN_TRND_F,
		CAST(LEFT(
		CAST(t.REDT_INFW_F AS VARCHAR), 5) AS VARCHAR(5)) AS REDT_INFW_F,
		CAST(LEFT(
		CAST(t.INCS_OUTF_F AS VARCHAR), 5) AS VARCHAR(5)) AS INCS_OUTF_F,
		CAST(LEFT(
		CAST(t.BALN_DROP_F AS VARCHAR), 5) AS VARCHAR(5)) AS BALN_DROP_F,
		CAST(LEFT(
		CAST(t.REDT_MIN_BALN_F AS VARCHAR), 5) AS VARCHAR(5)) AS REDT_MIN_BALN_F,
		CAST(LEFT(
		CAST(t.ISUF_CASH_MOVE_F AS VARCHAR), 5) AS VARCHAR(5)) AS ISUF_CASH_MOVE_F,
		t.short_description_updated AS INSG_SHRT_X,
		CAST(LEFT(
		CAST(t.long_description_updated AS VARCHAR), 1000) AS VARCHAR(1000)) AS INSG_LONG_X,
		CAST(LEFT(
		CAST(t.MLTI_INSG_F AS VARCHAR), 5) AS VARCHAR(5)) AS MLTI_INSG_F
FROM
		cashflow_shortfall_description t
WHERE
		UPPER(RTRIM(UPPER(t.MLTI_INSG_F))) <> UPPER(RTRIM('TRUE'))
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Step 4: Create the multiple_parties table directly with data

					-- drop table multiple_parties;
					CREATE OR REPLACE TEMPORARY TABLE multiple_parties
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		CAST(NULL AS VARCHAR(100)) AS PATY_IDNN_BK,
		t.customer_name,
		t.PATY_GRUP_I,
		t.CNSV_DCLN_BALN_N,
		t.LTST_WEEK_BALN_A,
		t.SCND_LTST_WEEK_BALN_A,
		t.MIN_BALN_DROP_R,
		t.MIN_BALN_DROP_A,
		t.INFW_DROP_R,
		t.INFW_DROP_A,
		t.OUTF_CHNG_R,
		t.AVRG_OUTF_DIFF_A,
		t.AVRG_BALN_DROP_R,
		t.AVRG_BALN_DROP_A,
		t.CR_TRAN_Q,
		t.DR_TRAN_Q,
		t.UPDT_D,
		t.PRTF_I,
		CAST(t.FIRST_TRAN_D AS DATE) AS FIRST_TRAN_D,
		'ACTI~CASHFLOW~07' AS INSG_I,
		'MULTI_CASHFLOW_INSIGHTS' AS SUB_INSG_TYP_C,
		'False' AS DCLN_BALN_TRND_F,
		'False' AS REDT_INFW_F,
		'False' AS INCS_OUTF_F,
		'False' AS BALN_DROP_F,
		'False' AS REDT_MIN_BALN_F,
		'False' AS ISUF_CASH_MOVE_F,
		MIN(t.INSG_SHRT_X) AS INSG_SHRT_X,
		g.combined_description AS INSG_LONG_X,
		'True' AS MLTI_INSG_F
FROM (
					SELECT DISTINCT
						     customer_name,
						     PATY_GRUP_I,
						     INSG_SHRT_X,
						     CNSV_DCLN_BALN_N,
						     LTST_WEEK_BALN_A,
						     SCND_LTST_WEEK_BALN_A,
						     MIN_BALN_DROP_R,
						     MIN_BALN_DROP_A,
						     INFW_DROP_R,
						     INFW_DROP_A,
						     OUTF_CHNG_R,
						     AVRG_OUTF_DIFF_A,
						     AVRG_BALN_DROP_R,
						     AVRG_BALN_DROP_A,
						     CR_TRAN_Q,
						     DR_TRAN_Q,
						     UPDT_D,
						     PRTF_I,
						     FIRST_TRAN_D
						   FROM
						cashflow_shortfall_description
						   WHERE
						UPPER(RTRIM(UPPER(MLTI_INSG_F))) = UPPER(RTRIM('TRUE'))
) t
LEFT JOIN
					grouped_descriptions g ON t.PATY_GRUP_I = g.PATY_GRUP_I
GROUP BY t.PATY_GRUP_I, g.combined_description, t.customer_name,t.PATY_GRUP_I,
		t.CNSV_DCLN_BALN_N,
		t.LTST_WEEK_BALN_A,
		t.SCND_LTST_WEEK_BALN_A,
		t.MIN_BALN_DROP_R,
		t.MIN_BALN_DROP_A,
		t.INFW_DROP_R,
		t.INFW_DROP_A,
		t.OUTF_CHNG_R,
		t.AVRG_OUTF_DIFF_A,
		t.AVRG_BALN_DROP_R,
		t.AVRG_BALN_DROP_A,
		t.CR_TRAN_Q,
		t.DR_TRAN_Q,
		t.UPDT_D,
		t.PRTF_I,
		t.FIRST_TRAN_D
	QUALIFY
		ROW_NUMBER() OVER (PARTITION BY t.PATY_GRUP_I ORDER BY t.customer_name) = 1
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Step 5: Join Everything Together

					-- drop table final_results;
					CREATE OR REPLACE TEMPORARY TABLE final_results
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		*
	       FROM
		non_multiple_parties
	   UNION ALL
	SELECT
		*
	       FROM
		multiple_parties
	   )
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Step 6: Update Customer Name and PATY_IDNN_BK with primary details of the sales group

					-- drop table all_cashflow_insights;
					--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "u_v_dsv_001_qpd_1.cust_dimn", "OREPLACE" **
					CREATE OR REPLACE TEMPORARY TABLE all_cashflow_insights
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		t.*,
		CASE
					WHEN UPPER(RTRIM(t.MLTI_INSG_F)) = UPPER(RTRIM('True'))
						THEN s.customer_name
		    ELSE t.customer_name
		END AS new_customer_name,
		CASE
					WHEN UPPER(RTRIM(t.MLTI_INSG_F)) = UPPER(RTRIM('True'))
						THEN s.PATY_IDNN_BK
		    ELSE t.PATY_IDNN_BK
		END AS new_paty_idnn_bk,
		CASE
					WHEN UPPER(RTRIM(t.MLTI_INSG_F)) = UPPER(RTRIM('True'))
						THEN s.PRTF_IDNN_BK
		    ELSE t.PRTF_I
		END AS new_PRTF_I
FROM
		final_results t
LEFT JOIN
					u_v_dsv_001_qpd_1.cust_dimn s
ON CAST(LEFT( CAST(t.PATY_GRUP_I AS VARCHAR), 50) AS VARCHAR(50)) = CAST(LEFT(CAST(REPLACE(s.paty_grup_i, 'SAPSG', '') AS VARCHAR), 50) AS VARCHAR(50))
AND UPPER(RTRIM( s.GRUP_PRIM_PATY_F)) = UPPER(RTRIM('Y'))
where new_paty_idnn_bk is not null
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- Step 7: Create a volatile table with custom ranking per PATY_IDNN_BK group

					-- drop table ranked_insights;
					CREATE OR REPLACE TEMPORARY TABLE ranked_insights
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS
					(
	SELECT
		a.*,
		ROW_NUMBER() OVER (
		       ORDER BY CASE
					a.INSG_I
					WHEN 'ACTI~CASHFLOW~07'
						THEN 1
					WHEN 'ACTI~CASHFLOW~01'
						THEN 2
					WHEN 'ACTI~CASHFLOW~05'
						THEN 3
					WHEN 'ACTI~CASHFLOW~02'
						THEN 4
					WHEN 'ACTI~CASHFLOW~03'
						THEN 5
					WHEN 'ACTI~CASHFLOW~04'
						THEN 6
					WHEN 'ACTI~CASHFLOW~06'
						THEN 7
		       ELSE 999 -- Default value for any unmapped INSG_I
		END, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~01'))
						THEN CNSV_DCLN_BALN_N
		       ELSE 0
		END DESC NULLS LAST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~01'))
						THEN LTST_WEEK_BALN_A
		       ELSE 0
		END DESC NULLS LAST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~01'))
						THEN SCND_LTST_WEEK_BALN_A
		       ELSE 0
		END DESC NULLS LAST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~05'))
						THEN MIN_BALN_DROP_R
		       ELSE 0
		END DESC NULLS LAST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~05'))
						THEN MIN_BALN_DROP_A
		       ELSE 0
		END DESC NULLS LAST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~02'))
						THEN INFW_DROP_R
		       ELSE 0
		END DESC NULLS LAST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~02'))
						THEN INFW_DROP_A
		       ELSE 0
		END DESC NULLS LAST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~03'))
						THEN CASE
							WHEN UPPER(RTRIM(RPAD(CAST(OUTF_CHNG_R AS CHAR), 1))) = UPPER(RTRIM('*'))
											THEN 0
		       ELSE OUTF_CHNG_R
						END
		       ELSE 0
		END DESC NULLS LAST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~03'))
						THEN AVRG_OUTF_DIFF_A
		       ELSE 0
		END DESC NULLS LAST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~04'))
						THEN AVRG_BALN_DROP_R
		       ELSE 0
		END DESC NULLS LAST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~04'))
						THEN AVRG_BALN_DROP_A
		       ELSE 0
		END DESC NULLS LAST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~06'))
						THEN CR_TRAN_Q
		       ELSE 999
		END ASC NULLS FIRST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~06'))
						THEN
						DR_TRAN_Q
		       ELSE 999
		END ASC NULLS FIRST, CASE
					WHEN UPPER(RTRIM(a.INSG_I)) = UPPER(RTRIM('ACTI~CASHFLOW~06'))
						THEN FIRST_TRAN_D
		END ASC NULLS FIRST) AS ranking
		       FROM
		all_cashflow_insights a)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON
--    COMMIT PRESERVE ROWS
                        					;

					-- Step 8: Create the final actionable insights table for cashflow shortfall

					-- drop table final_cashflow_table;
					--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "PFTCF.HASH_MD5", "oreplace" **
					CREATE OR REPLACE TEMPORARY TABLE final_cashflow_table
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		MD5(new_paty_idnn_bk || '' || SUB_INSG_TYP_C) AS INSG_I,
		'CASHFLOW_SHORTFALL' AS INSG_TYPE_M,
		SUB_INSG_TYP_C AS SUB_INSG_TYPE_M,
		ranking AS RANK_N,
		new_PRTF_I as PRTF_I,
		new_customer_name AS CUST_M,
		'CIFPT+' || SUBSTR(new_paty_idnn_bk, LENGTH(new_paty_idnn_bk) - 9) AS PATY_I,
		'SAPSG'|| PATY_GRUP_I AS PATY_GRUP_I,
		INSG_SHRT_X AS SHRT_DESC_X,
		REPLACE(INSG_LONG_X, '\', '') AS LONG_DESC_X,
		CAST('UNDELIVERED' AS VARCHAR(20)) AS STUS_M,
		CURRENT_DATE() AS CRAT_D,
		CAST(UPDT_D AS DATE) + 43 AS EXPY_D,
		CAST(NULL AS VARCHAR(255)) AS FDBK_X
		FROM
		ranked_insights
					   )
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;


drop table cashflow_shortfall_description;
drop table grouped_descriptions;
drop table non_multiple_parties;
drop table multiple_parties;
drop table final_results;
drop table all_cashflow_insights;
drop table ranked_insights;

					CREATE OR REPLACE TEMPORARY TABLE vt_stnd
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		INSG_I,
		INSG_TYPE_M,
		SUB_INSG_TYPE_M,
		RANK_N,
		PRTF_I,
		CUST_M,
		PATY_I,
		PATY_GRUP_I,
		SHRT_DESC_X,
		LONG_DESC_X,
		STUS_M,
		CRAT_D,
		EXPY_D,
		FDBK_X
FROM
		vt_payaways_rankings

UNION ALL
	SELECT
	   INSG_I,
		INSG_TYPE_M,
		SUB_INSG_TYPE_M,
		RANK_N,
		PRTF_I,
		CUST_M,
		PATY_I,
		PATY_GRUP_I,
		SHRT_DESC_X,
		LONG_DESC_X,
		STUS_M,
		CRAT_D,
		EXPY_D,
		FDBK_X
FROM
		vt_pn_final

UNION ALL
	SELECT
	   INSG_I,
		INSG_TYPE_M,
		SUB_INSG_TYPE_M,
		RANK_N,
		PRTF_I,
		CUST_M,
		PATY_I,
		PATY_GRUP_I,
		SHRT_DESC_X,
		LONG_DESC_X,
		STUS_M,
		CRAT_D,
		EXPY_D,
		FDBK_X
FROM
		final_cashflow_table
	)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

drop table vt_payaways_rankings;
drop table vt_pn_final;
drop table final_cashflow_table;

					CREATE OR REPLACE TEMPORARY TABLE vt_stnd_comb
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		a.INSG_I,
		a.INSG_TYPE_M,
		a.SUB_INSG_TYPE_M,
		ROW_NUMBER() OVER (ORDER BY a.RANK_N, a.INSG_TYPE_M) AS RANK_N,
		a.PRTF_I,
		a.CUST_M,
		a.PATY_I,
		PATY_GRUP_I,
		SHRT_DESC_X,
		LONG_DESC_X,
		STUS_M,
		CRAT_D,
		EXPY_D,
		FDBK_X

FROM
		vt_stnd a
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

drop table vt_stnd;

					---CREATE REF TABLE FOR NEW INSIGHT ID CREATION LATER
					       -- FIND MAX ID INDEX NO FOR EACH INSIGHT TYPE
					        -- FIND VALID PERIOD FOR EACH INSIGHT TYPE BASED ON CURRENT TABLE, IF NULL FROM HISTORY
					        -- DEFAULT 90 DAYS VALID PERIOD
					        -- PREFIX IS COMBINED BY SHORT FORM OF INSIGHT TYPE + LAST 2 DIGITS OF CURRENT YEAR


					-- DROP TABLE VT_REF
					--** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_ACTN_INSG_HIST" **
					CREATE OR REPLACE TEMPORARY TABLE VT_REF
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		INSG_TYPE_M,
		SUB_INSG_TYPE_M,
		YR_QLFY_C,
		INSG_TYPE_C || '~' || YR_QLFY_C || '~' AS PREFIX,
		MAX_INDEX

FROM (
					SELECT DISTINCT
						   C.INSG_TYPE_M,
						   C.SUB_INSG_TYPE_M,
						--ADD ADDITIONAL PREFIX HERE FOR OTHER TYPES IN THE FUTURE
						CASE
							WHEN UPPER(RTRIM(C.INSG_TYPE_M)) = UPPER(RTRIM('PAYAWAY'))
											THEN 'PAYA'
							WHEN UPPER(RTRIM(C.INSG_TYPE_M)) = UPPER(RTRIM('PREDICTED_NEEDS')) AND UPPER(RTRIM( SUB_INSG_TYPE_M)) = UPPER(RTRIM('MULTIPLE_NEEDS'))
											THEN 'PNMN'
							WHEN UPPER(RTRIM(C.INSG_TYPE_M)) = UPPER(RTRIM('PREDICTED_NEEDS'))
											THEN 'PNIP'
							WHEN UPPER(RTRIM(C.INSG_TYPE_M)) = UPPER(RTRIM('CASHFLOW_SHORTFALL'))
											THEN 'CFSF'
						   ELSE NULL
						END AS INSG_TYPE_C,
						--LAST 2 DIGITS OF CURRENT YEAR
						CAST(LEFT(
						   CAST(TO_CHAR(CURRENT_DATE(), 'YY') /*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/ AS VARCHAR), 2) AS VARCHAR(2)) AS YR_QLFY_C,
						COALESCE(H.MAX_INDEX,0) AS MAX_INDEX --DEFAULT 0 SO INDEX RESTARTS AT 0 WHEN ROLLING TO NEW YEAR

					FROM
						vt_stnd_comb C
					LEFT JOIN (
											SELECT
												INSG_TYPE_M,
												RIGHT(LEFT(HASH_ID, 7) , 2) AS YR_QLFY_C,
												MAX(CAST(LTRIM(RIGHT(HASH_ID, 10), '0') AS INTEGER)) AS MAX_INDEX


								FROM
												U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_ACTN_INSG_HIST -- REPLACE ACTUAL TABLE NAME

								WHERE
												RIGHT(LEFT(HASH_ID, 7) , 2) = TO_CHAR(CURRENT_DATE(), 'YY') --ALWAYS CURRENT YEAR SO PREFIX ROLLS OVER TO NEW YEAR FOR NEW INSIGHTS
												/*** SSC-FDM-TD0029 - SNOWFLAKE SUPPORTED FORMATS FOR TO_CHAR DIFFER FROM TERADATA AND MAY FAIL OR HAVE DIFFERENT BEHAVIOR ***/

								GROUP BY 1,2
						 ) H

					ON C.INSG_TYPE_M = H.INSG_TYPE_M

) A

					)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					-- CREATE TEMP TABLE BY JOINNING CURRENT TABLE WITH HISTORY TABLE AND FEEDBACK TABLE



					-- DROP TABLE VT_COMBINED;
					--** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_ACTN_INSG_HIST" **
					CREATE OR REPLACE TEMPORARY TABLE VT_COMBINED
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		*
	   FROM
		vt_stnd_comb A
	   LEFT JOIN (
						SELECT
							H.HASH_ID,
							H.INSG_I AS HIST_INSG_ID,
							H.INSG_TYPE_M AS PREV_INSG_TYPE_M,
							H.SUB_INSG_TYPE_M AS PREV_SUB_INSG_TYPE_M,
							H.CRAT_D AS PREV_CRAT_D,
							H.EXPY_D AS PREV_EXPY_D
			FROM
							U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_ACTN_INSG_HIST AS H
						QUALIFY
							ROW_NUMBER() OVER (PARTITION BY H.INSG_I ORDER BY H.ETL_TS DESC NULLS LAST) = 1 -- LATEST FEEDBACK + HISTORY
	   ) B ON A.INSG_I = B.HIST_INSG_ID
				)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;


drop table vt_stnd_comb;

					/*
					 SPLIT DATA IN GROUP AND APPLY REFRESH LOGICS

					 EXISTING INSIGHTS -CREATE LIST OF INSIGHTS THAT SATIFIY THE FOLLOWING CONDITIONS:
					    - PREVIOUSLY CREATED AND ASSIGNED Hash ID AT SAME GROUP KEY LEVEL, I.E., NON-NULL IN HISTORY TABLE
					    - UNEXPIRED AS OF TODAY
					    - ELIGIBLE INSIGHTS AS OF TODAY (STILL SHOW UP IN REFRESHED BASE TABLE)

					ACTION:
					    - USE SAME HASH ID
					    - USE SAME CREATED DATE
					    - UPDATE DESCRIPTION

					*/
					--DROP TABLE VT_UPDATED;
					CREATE OR REPLACE TEMPORARY TABLE VT_UPDATED
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT

		A.HASH_ID,
		--EXISTING INSIGHT ID
		A.INSG_I,
		A.INSG_TYPE_M,
		A.SUB_INSG_TYPE_M,
		A.RANK_N,
		A.PRTF_I,
		A.CUST_M,
		A.PATY_I,
		A.PATY_GRUP_I,
		A.SHRT_DESC_X,
		A.LONG_DESC_X,
		A.STUS_M,
		A.FDBK_X,
		A.PREV_CRAT_D AS CRAT_D,
		A.PREV_EXPY_D as EXPY_D,
		A.PREV_EXPY_D,
		CURRENT_DATE() as UPDT_D

FROM
		VT_COMBINED A

WHERE  (A.HASH_ID IS NOT NULL AND A.PREV_EXPY_D >= CURRENT_DATE()) --UNDELIVERED & UNEXPIRED & STILL VALID
					)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					/*

					NEW INSIGHTS - CREATE LIST OF INSIGHTS THAT SATIFIY THE FLLOWING CONDITIONS:

					      1. COMPLETELY NEW, I.E., NEVER CREATED BEFORE WITH SAME GROUPKEY (NULL IN HISTORY TABLE)
					      2. PREVIOUSLY CREATED & EXPIRED, I.E., NO NEW TXNS FOR 3 MTHS AND HAPPENED AGAIN IN PAYAWAY CASE

					ACTION:
					    - ASSIGN NEW INSIGHTS ID, PREFIX + 8 DIGITS FROM LAST MAX INDEX NUMBER PER TYPE
					    - USE CURRENT DATE AS CREATED DATE
					    - USE CURRENT DATE + INSIGHTS VALID DAYS FROM REF TABLE AS EXPIRY DATE


					*/
					--DROP TABLE VT_NEW;
					CREATE OR REPLACE TEMPORARY TABLE VT_NEW
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		PREFIX || LPAD(CAST(LEFT(CAST((MAX_INDEX + ROW_NUMBER() OVER (PARTITION BY INSG_TYPE_M ORDER BY RANK_N)) AS VARCHAR), 10) AS VARCHAR(10)), 10, '0') AS HASH_ID,
		--EXISTING INSIGHT ID
		INSG_I,
		INSG_TYPE_M,
		SUB_INSG_TYPE_M,
		RANK_N,
		PRTF_I,
		CUST_M,
		PATY_I,
		PATY_GRUP_I,
		SHRT_DESC_X,
		LONG_DESC_X,
		STUS_M,
		FDBK_X,
		CRAT_D,
		EXPY_D,
		PREV_EXPY_D,
		UPDT_D

FROM
(
					SELECT
						A.*,
						CURRENT_DATE() as UPDT_D,
						--CURRENT_DATE + REF.VALID_D AS EXPY_D,
						REF.PREFIX,
						REF.MAX_INDEX

						FROM
						VT_COMBINED A
						LEFT JOIN
							VT_REF REF
						ON REF.INSG_TYPE_M = A.INSG_TYPE_M
						AND (REF.SUB_INSG_TYPE_M = A.SUB_INSG_TYPE_M) OR ( REF.SUB_INSG_TYPE_M is null and A.SUB_INSG_TYPE_M is null)

						WHERE
						    (HASH_ID IS NULL) --COMPLETELY NEW
						    OR (HASH_ID IS NOT NULL AND A.PREV_EXPY_D < CURRENT_DATE()) -- PREVIOUSLY CREATED INSIGHT & NOW EXPIRED
		    --OR (LATEST_STUS_M IS NULL AND INSIGHT_ID IS NOT NULL AND PREV_EXPY_D < CURRENT_DATE-90) --PREVIOUSLY UNDELIVERED BUT EXPIRED OVER 90 DAYS

) A

					)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;


 /*





STEP 5: UNION VT THAT CONTAINS THE FOLLOWING TYPES OF INSIGHTS:
    - STEP 4.1 EXISTING INSIGHTS
    - STEP 4.2 NEW INSIGHTS
    - TBC

*/
-- drop tables not required to be more efficient and avoid spool issues
drop table vt_combined;
drop table vt_ref;

					--DROP TABLE VT_REFRESHED
					CREATE OR REPLACE TEMPORARY TABLE VT_REFRESHED
					COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "10/14/2025",  "domain": "snowflake" }}'
					AS (
	SELECT
		* FROM
		VT_UPDATED
	    UNION
	SELECT
		* FROM
		VT_NEW

	)
--					--** SSC-FDM-0008 - ON COMMIT NOT SUPPORTED **
--					ON COMMIT PRESERVE ROWS
					                       					;

					--Delete existing data from the final table
					DELETE FROM
	U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_ACTN_INSG;

					--- Inserting refreshed data
					--** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_ACTN_INSG" **
					INSERT INTO U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_ACTN_INSG SELECT
	INSG_I,
	INSG_TYPE_M,
	SUB_INSG_TYPE_M,
	RANK_N,
	PRTF_I,
	CUST_M,
	PATY_I,
	PATY_GRUP_I,
	SHRT_DESC_X,
	LONG_DESC_X,
	STUS_M,
	CRAT_D,
	UPDT_D,
	EXPY_D,
	FDBK_X,
	HASH_ID
FROM
	VT_REFRESHED;

					--- Loading data into History Table
					--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_ACTN_INSG_HIST", "U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_ACTN_INSG" **
					INSERT INTO U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_ACTN_INSG_HIST SELECT
	INSG_I,
	INSG_TYPE_M,
	SUB_INSG_TYPE_M,
	RANK_N,
	PRTF_I,
	CUST_M,
	PATY_I,
	PATY_GRUP_I,
	SHRT_DESC_X,
	LONG_DESC_X,
	STUS_M,
	CRAT_D,
	UPDT_D,
	EXPY_D,
	FDBK_X,
	HASH_ID,
	CURRENT_TIMESTAMP() as ETL_TS
	FROM
	U_D_DSV_001_QPD_1.BV_B360_CUST_VALU_MGMT_ACTN_INSG
					;