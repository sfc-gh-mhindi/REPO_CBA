{{ config(materialized='view', tags=['XfmPlIntRateAmtMarginFrmExt']) }}

WITH --Manual Task - ColumnGenerator - CgAddMapTypeC

SELECT * FROM CgAddMapTypeC