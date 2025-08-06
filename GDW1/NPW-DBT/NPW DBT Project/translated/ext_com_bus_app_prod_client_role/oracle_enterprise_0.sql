{{ config(materialized='view', tags=['EXT_COM_BUS_APP_PROD_CLIENT_ROLE']) }}

WITH 
,
Oracle_Enterprise_0 AS (SELECT "RPT_ROW"
     FROM (SELECT    'D|'
                  || TO_CHAR (MOD_TIMESTAMP, 'YYYYMMDD')
                  || '|'
                  || APP_PROD_CLIENT_ROLE_ID
                  || '|'
                  || ROLE_CAT_ID
                  || '|'
                  || CIF_CODE
                  || '|'
                  || APP_PROD_ID
                  || '|'
                  || SUBTYPE_CODE
                  || '|' RPT_ROW
             FROM (
				   SELECT APCR.MOD_TIMESTAMP,
				   		  APCR.APP_PROD_CLIENT_ROLE_ID,
						  PRC.ROLE_CAT_ID,
						  CB.CIF_CODE,
						  APCR.APP_PROD_ID,
						  AP.SUBTYPE_CODE
                     FROM APP_PROD_CLIENT_ROLE APCR,
                          PCD_OWNER.APP_PROD AP,
					 	  PCD_OWNER.PROD_ROLE_CAT PRC,
						  PFS_OWNER.CS$CLIENT_BASE CB
                    WHERE APCR.PROD_ROLE_CAT_ID = PRC.PROD_ROLE_CAT_ID
                          AND APCR.APP_PROD_ID = AP.APP_PROD_ID
                          AND AP.SUBTYPE_CODE in ('CLP')
						  AND APCR.CLIENT_ID = CB.CLIENT_ID
						  AND CB.commsee_client_code in ('C', 'DC')
						  AND APCR.mod_timestamp BETWEEN TO_DATE ('{{ var('FBIZDATE') }}',
                                                          'YYYYMMDDHH24MISS'
                                                         )
                                             AND TO_DATE ('{{ var('TBIZDATE') }}',
                                                          'YYYYMMDDHH24MISS'
                                                         ))))


SELECT * FROM Oracle_Enterprise_0