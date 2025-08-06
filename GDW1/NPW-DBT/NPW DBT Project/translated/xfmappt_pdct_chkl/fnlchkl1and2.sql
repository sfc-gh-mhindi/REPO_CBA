{{ config(materialized='view', tags=['XfmAppt_Pdct_Chkl']) }}

WITH FnlChkl1and2 AS (
	SELECT
		APPT_PDCT_I as APPT_PDCT_I,
		CHKL_ITEM_C as CHKL_ITEM_C,
		STUS_D as STUS_D,
		STUS_C as STUS_C,
		SRCE_SYST_C as SRCE_SYST_C,
		CHKL_ITEM_X as CHKL_ITEM_X,
		RUN_STRM as RUN_STRM
	FROM {{ ref('Xfrmr2_CHK2__Frm2') }}
	UNION ALL
	SELECT
		APPT_PDCT_I,
		CHKL_ITEM_C,
		STUS_D,
		STUS_C,
		SRCE_SYST_C,
		CHKL_ITEM_X,
		RUN_STRM
	FROM {{ ref('Xfmr1_CHK1') }}
)

SELECT * FROM FnlChkl1and2