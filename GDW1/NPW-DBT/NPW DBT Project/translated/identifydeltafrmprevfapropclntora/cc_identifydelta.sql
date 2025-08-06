{{ config(materialized='view', tags=['IdentifyDeltaFrmPrevFaPropClntOra']) }}

WITH CC_IdentifyDelta AS (
	SELECT
		COALESCE({{ ref('Copy') }}.FA_PROPOSED_CLIENT_ID, {{ ref('SrcFAPropClientPrevOra') }}.FA_PROPOSED_CLIENT_ID) AS FA_PROPOSED_CLIENT_ID,
		COALESCE({{ ref('Copy') }}.COIN_ENTITY_ID, {{ ref('SrcFAPropClientPrevOra') }}.COIN_ENTITY_ID) AS COIN_ENTITY_ID,
		COALESCE({{ ref('Copy') }}.CLIENT_CORRELATION_ID, {{ ref('SrcFAPropClientPrevOra') }}.CLIENT_CORRELATION_ID) AS CLIENT_CORRELATION_ID,
		COALESCE({{ ref('Copy') }}.COIN_ENTITY_NAME, {{ ref('SrcFAPropClientPrevOra') }}.COIN_ENTITY_NAME) AS COIN_ENTITY_NAME,
		COALESCE({{ ref('Copy') }}.FA_ENTITY_CAT_ID, {{ ref('SrcFAPropClientPrevOra') }}.FA_ENTITY_CAT_ID) AS FA_ENTITY_CAT_ID,
		COALESCE({{ ref('Copy') }}.FA_UNDERTAKING_ID, {{ ref('SrcFAPropClientPrevOra') }}.FA_UNDERTAKING_ID) AS FA_UNDERTAKING_ID,
		COALESCE({{ ref('Copy') }}.FA_PROPOSED_CLIENT_CAT_ID, {{ ref('SrcFAPropClientPrevOra') }}.FA_PROPOSED_CLIENT_CAT_ID) AS FA_PROPOSED_CLIENT_CAT_ID,
		CASE
			WHEN {{ ref('SrcFAPropClientPrevOra') }}.FA_UNDERTAKING_ID IS NULL AND {{ ref('SrcFAPropClientPrevOra') }}.FA_PROPOSED_CLIENT_ID IS NULL THEN '1'
			WHEN {{ ref('Copy') }}.FA_UNDERTAKING_ID IS NULL AND {{ ref('Copy') }}.FA_PROPOSED_CLIENT_ID IS NULL THEN '2'
			WHEN ({{ ref('SrcFAPropClientPrevOra') }}.FA_UNDERTAKING_ID = {{ ref('Copy') }}.FA_UNDERTAKING_ID AND {{ ref('SrcFAPropClientPrevOra') }}.FA_PROPOSED_CLIENT_ID = {{ ref('Copy') }}.FA_PROPOSED_CLIENT_ID) AND ({{ ref('SrcFAPropClientPrevOra') }}.COIN_ENTITY_ID <> {{ ref('Copy') }}.COIN_ENTITY_ID OR {{ ref('SrcFAPropClientPrevOra') }}.CLIENT_CORRELATION_ID <> {{ ref('Copy') }}.CLIENT_CORRELATION_ID OR {{ ref('SrcFAPropClientPrevOra') }}.COIN_ENTITY_NAME <> {{ ref('Copy') }}.COIN_ENTITY_NAME OR {{ ref('SrcFAPropClientPrevOra') }}.FA_ENTITY_CAT_ID <> {{ ref('Copy') }}.FA_ENTITY_CAT_ID OR {{ ref('SrcFAPropClientPrevOra') }}.FA_PROPOSED_CLIENT_CAT_ID <> {{ ref('Copy') }}.FA_PROPOSED_CLIENT_CAT_ID) THEN '3'
			WHEN {{ ref('SrcFAPropClientPrevOra') }}.FA_UNDERTAKING_ID = {{ ref('Copy') }}.FA_UNDERTAKING_ID AND {{ ref('SrcFAPropClientPrevOra') }}.FA_PROPOSED_CLIENT_ID = {{ ref('Copy') }}.FA_PROPOSED_CLIENT_ID AND {{ ref('SrcFAPropClientPrevOra') }}.COIN_ENTITY_ID = {{ ref('Copy') }}.COIN_ENTITY_ID AND {{ ref('SrcFAPropClientPrevOra') }}.CLIENT_CORRELATION_ID = {{ ref('Copy') }}.CLIENT_CORRELATION_ID AND {{ ref('SrcFAPropClientPrevOra') }}.COIN_ENTITY_NAME = {{ ref('Copy') }}.COIN_ENTITY_NAME AND {{ ref('SrcFAPropClientPrevOra') }}.FA_ENTITY_CAT_ID = {{ ref('Copy') }}.FA_ENTITY_CAT_ID AND {{ ref('SrcFAPropClientPrevOra') }}.FA_PROPOSED_CLIENT_CAT_ID = {{ ref('Copy') }}.FA_PROPOSED_CLIENT_CAT_ID THEN '0'
		END AS change_code 
	FROM {{ ref('SrcFAPropClientPrevOra') }} 
	FULL OUTER JOIN {{ ref('Copy') }} 
	ON {{ ref('SrcFAPropClientPrevOra') }}.FA_UNDERTAKING_ID = {{ ref('Copy') }}.FA_UNDERTAKING_ID
	AND {{ ref('SrcFAPropClientPrevOra') }}.FA_PROPOSED_CLIENT_ID = {{ ref('Copy') }}.FA_PROPOSED_CLIENT_ID
	WHERE change_code = '1' OR change_code = '3' OR change_code = '2'
)

SELECT * FROM CC_IdentifyDelta