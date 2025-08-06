{{ config(materialized='view', tags=['XfmEvntRel']) }}

WITH --Manual Task - DRSConnectorPX - REJT_EVNT_REL

SELECT * FROM REJT_EVNT_REL