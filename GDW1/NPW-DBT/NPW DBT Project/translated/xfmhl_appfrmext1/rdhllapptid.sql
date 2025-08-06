{{ config(materialized='view', tags=['XfmHL_APPFrmExt1']) }}

WITH RdHllApptId AS (
	SELECT APPT_I, APPT_ACTV_Q, RUN_STRM 
	FROM (
		SELECT APPT_I, APPT_ACTV_Q, RUN_STRM,
		 ROW_NUMBER() OVER (PARTITION BY APPT_I ORDER BY 1 ASC) AS ROW_NUM 
		FROM {{ ref('IgnrNulls__FrmTrnsfrmr') }}
	) AS RdHllApptId_TEMP
	WHERE ROW_NUM = 1
)

SELECT * FROM RdHllApptId