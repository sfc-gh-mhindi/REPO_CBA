{{ config(materialized='view', tags=['XfmHL_APPFrmExt1']) }}

WITH RdHllApptIdToInss AS (
	SELECT APPT_I, DOCU_DELY_RECV_C, RUN_STRM 
	FROM (
		SELECT APPT_I, DOCU_DELY_RECV_C, RUN_STRM,
		 ROW_NUMBER() OVER (PARTITION BY APPT_I ORDER BY 1 ASC) AS ROW_NUM 
		FROM {{ ref('Xfm__FrmXfm') }}
	) AS RdHllApptIdToInss_TEMP
	WHERE ROW_NUM = 1
)

SELECT * FROM RdHllApptIdToInss