{{ config(materialized='view', tags=['ExtWRK_APPT_PDCT_FEATFrmStg_IntRateAmtMargin']) }}

WITH --Manual Task - ColumnGenerator - CgFeatSubtype

SELECT * FROM CgFeatSubtype