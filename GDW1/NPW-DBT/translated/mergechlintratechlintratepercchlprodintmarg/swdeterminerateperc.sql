{{ config(materialized='view', tags=['MergeChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH --Manual Task - PxSwitch - SwDetermineRatePerc

SELECT * FROM SwDetermineRatePerc