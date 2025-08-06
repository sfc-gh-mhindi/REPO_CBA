{{ config(materialized='view', tags=['EXT_CLP_BUS_CLNT_ANS']) }}

WITH 
,
Oracle_Enterprise_0 AS (SELECT RPT_ROW
     FROM (SELECT    'D|'
                  ||TO_CHAR (MOD_TIMESTAMP, 'YYYYMMDD')
                  || '|'
                  ||APP_ID
                  || '|'
                  ||APP_PROD_ID
                  || '|'
                  ||SUBTYPE_CODE
                  || '|'
                  ||CLP_CLIENT_ANSWER_VN
                  || '|'
                  ||CLP_CLIENT_ANSWER_ID
                  || '|'
                  ||CIF_CODE
                  || '|'
                  ||QA_QUESTION_ID
                  || '|'
                  ||QA_ANSWER_ID
                  || '|'
                  ||TEXT_ANSWER
                  || '|'
                  ||TO_CHAR (DATE_ANSWER, 'YYYYMMDD')
                  || '|'
                  ||YN_FLAG_ANSWER
                  || '|'
                  ||NUMERIC_ANSWER
                  || '|'
                  ||MOD_USER_ID
                  || '|' RPT_ROW
             FROM (SELECT 			CCA.MOD_TIMESTAMP,
						A.APP_ID,
						AP.APP_PROD_ID,
						A.SUBTYPE_CODE,
						CCA.CLP_CLIENT_ANSWER_VN,
			 	  		CCA.CLP_CLIENT_ANSWER_ID,
						(select c.CIF_CODE from
						cs$client_base c
						where c.CLIENT_ID=cca.CLIENT_ID) as
						CIF_CODE,
						CCA.QA_QUESTION_ID,
						CCA.QA_ANSWER_ID,
						translate(CCA.TEXT_ANSWER,chr(10)||chr(13)||'|','   ') as TEXT_ANSWER,
						CCA.DATE_ANSWER,
						CCA.YN_FLAG_ANSWER,
						CCA.NUMERIC_ANSWER,
						CCA.MOD_USER_ID
              FROM
				app a,
				pcd_owner.app_prod ap,
				pin_owner.clp_client_answer cca
	     where
				a.APP_ID=ap.APP_ID
				and ap.APP_PROD_ID=cca.CLP_APP_PROD_ID
				AND CCA.mod_timestamp BETWEEN TO_DATE ('{{ var('FBIZDATE') }}','YYYYMMDDHH24MISS'
                                                         )
                                             AND TO_DATE ('{{ var('TBIZDATE') }}','YYYYMMDDHH24MISS'))))


SELECT * FROM Oracle_Enterprise_0