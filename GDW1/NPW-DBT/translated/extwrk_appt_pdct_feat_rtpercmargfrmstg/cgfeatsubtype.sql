{{ config(materialized='view', tags=['ExtWRK_APPT_PDCT_FEAT_RTPERCMARGFrmStg']) }}

WITH --Manual Task - ColumnGenerator - CgFeatSubtype

SELECT * FROM CgFeatSubtype