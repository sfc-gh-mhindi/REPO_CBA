{{ config(materialized='view', tags=['XfmChlBusFeeDiscFeeFrmExt']) }}

WITH --Manual Task - ColumnGenerator - AddColMapTypeLkp

SELECT * FROM AddColMapTypeLkp