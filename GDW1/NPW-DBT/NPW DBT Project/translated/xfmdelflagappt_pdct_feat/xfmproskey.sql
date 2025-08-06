{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_FEAT']) }}

WITH XfmProsKey AS (
	SELECT
		-- *SRC*: \(20)If OutJoin.DLTD_TABL_NAME = 'CCL_APP_PROD' Then 'CSE_CCL_BUS_APP_PROD_APPT_PDCT_FEAT1' Else ( If OutJoin.DLTD_TABL_NAME = 'HL_APP_PROD' Then 'CSE_CHL_BUS_APP_PROD_APPT_PDCT_FEAT5' Else ( If OutJoin.DLTD_TABL_NAME = 'PL_APP_PROD' Then 'CSE_CPL_BUS_APP_PROD_APPT_PDCT_FEAT5' Else ( If OutJoin.DLTD_TABL_NAME = 'CC_APP_PROD' Then 'CSE_CCC_BUS_APP_PROD_APPT_PDCT_FEAT5' Else ( If OutJoin.DLTD_TABL_NAME = 'HL_FEATURE_ATTR' Then 'CSE_CHL_BUS_FEAT_ATTR_APPT_PDCT_FEAT2' Else ( If OutJoin.DLTD_TABL_NAME = 'PL_FEE' Then 'CSE_CPL_BUS_FEE_MARGIN_APPT_PDCT_FEAT3' Else ( If OutJoin.DLTD_TABL_NAME = 'HL_FEE' OR OutJoin.DLTD_TABL_NAME = 'HL_FEE_DISCOUNT' Then 'CSE_CHL_BUS_FEE_DISC_FEE_APPT_PDCT_FEAT3' Else ( If OutJoin.DLTD_TABL_NAME = 'HL_INT_RATE' OR OutJoin.DLTD_TABL_NAME = 'HL_PROD_INT_MARGIN' Then 'CSE_CHL_BUS_INT_RT_PRC_PRD_INT_MRG_APPT_PDCT_FEAT3' Else ( If OutJoin.DLTD_TABL_NAME = 'PL_INT_RATE' OR OutJoin.DLTD_TABL_NAME = 'PL_MARGIN' Then 'CSE_CPL_BUS_INT_RATE_AMT_MARGIN_APPT_PDCT_FEAT3' Else ( If OutJoin.DLTD_TABL_NAME = 'CCL_APP_FEE' Then 'CSE_CCL_BUS_APP_FEE_APPT_PDCT_FEAT3' Else ''))))))))),
		IFF(
	    {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'CCL_APP_PROD', 'CSE_CCL_BUS_APP_PROD_APPT_PDCT_FEAT1',     
	    IFF(
	        {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'HL_APP_PROD', 'CSE_CHL_BUS_APP_PROD_APPT_PDCT_FEAT5',         
	        IFF(
	            {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'PL_APP_PROD', 'CSE_CPL_BUS_APP_PROD_APPT_PDCT_FEAT5',             
	            IFF(
	                {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'CC_APP_PROD', 'CSE_CCC_BUS_APP_PROD_APPT_PDCT_FEAT5',                 
	                IFF(
	                    {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'HL_FEATURE_ATTR', 'CSE_CHL_BUS_FEAT_ATTR_APPT_PDCT_FEAT2',                     
	                    IFF(
	                        {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'PL_FEE', 'CSE_CPL_BUS_FEE_MARGIN_APPT_PDCT_FEAT3',                         
	                        IFF(
	                            {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'HL_FEE'
	                        or {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'HL_FEE_DISCOUNT', 'CSE_CHL_BUS_FEE_DISC_FEE_APPT_PDCT_FEAT3',                             
	                            IFF(
	                                {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'HL_INT_RATE'
	                            or {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'HL_PROD_INT_MARGIN', 'CSE_CHL_BUS_INT_RT_PRC_PRD_INT_MRG_APPT_PDCT_FEAT3',                                 
	                                IFF(
	                                    {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'PL_INT_RATE'
	                                or {{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'PL_MARGIN', 'CSE_CPL_BUS_INT_RATE_AMT_MARGIN_APPT_PDCT_FEAT3', 
	                                    IFF({{ ref('TMP_DELETED') }}.DLTD_TABL_NAME = 'CCL_APP_FEE', 'CSE_CCL_BUS_APP_FEE_APPT_PDCT_FEAT3', '')
	                                )
	                            )
	                        )
	                    )
	                )
	            )
	        )
	    )
	) AS svConvM,
		{{ ref('TMP_DELETED') }}.DLTD_TABL_NAME AS DELETED_TABLE_NAME,
		{{ ref('TMP_DELETED') }}.DLTD_KEY1_VALU AS APP_PROD_ID,
		{{ ref('TMP_DELETED') }}.DLTD_KEY1_VALU AS DELETED_KEY_1_VALUE,
		{{ ref('TMP_DELETED') }}.DLTD_KEY2_VALU AS DELETED_KEY_2_VALUE,
		{{ ref('TMP_DELETED') }}.DLTD_KEY3_VALU AS DELETED_KEY_3_VALUE,
		svConvM AS CONV_M,
		APPT_PDCT_I,
		SRCE_SYST_APPT_FEAT_I
	FROM {{ ref('TMP_DELETED') }}
	WHERE 
)

SELECT * FROM XfmProsKey