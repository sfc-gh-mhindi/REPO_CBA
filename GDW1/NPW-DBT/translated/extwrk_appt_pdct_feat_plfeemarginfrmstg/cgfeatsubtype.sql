{{ config(materialized='view', tags=['ExtWRK_APPT_PDCT_FEAT_PLFEEMARGINFrmStg']) }}

WITH --Manual Task - ColumnGenerator - CgFeatSubtype

SELECT * FROM CgFeatSubtype