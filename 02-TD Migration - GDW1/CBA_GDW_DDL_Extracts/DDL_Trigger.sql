--
/* <sc-trigger> PDI3ED.cnsd_pros_actv_trig </sc-trigger> */
REPLACE TRIGGER PDI3ED.cnsd_pros_actv_trig
AFTER INSERT ON pdi3devl._cnsd_pros_actv_log
REFERENCING NEW AS a2  FOR EACH ROW
(

INSERT INTO pdi3devl.cnsd_pros_logs
( pros_key_n
, pros_logs_c
, pros_logs_desn_x
, pros_actv_c
, pros_valu_c
, pros_valu_n )
SELECT a1.pros_key_n
     , a1.pros_logs_c
     , a1.pros_logs_desn_x
     , a2.pros_actv_c
     , a2.pros_valu_c
     , a2.pros_valu_n
FROM pvi3view._cnsd_pros_key_vary a1;

DELETE FROM pdi3devl._cnsd_pros_actv_log ALL ;

);

--
/* <sc-trigger> PEARSODCE3._cnsd_pros_step_note </sc-trigger> */
REPLACE TRIGGER pearsodcE3._cnsd_pros_step_note
AFTER INSERT ON udmabtch._cnsd_pros_step_note
REFERENCING NEW AS a2  FOR EACH ROW

UPDATE PDI3DEVL._cnsd_pros_key_vary 
SET pros_vary_5_n = COALESCE(pros_vary_5_n +1,0)
WHERE join_key = 1
;

--
/* <sc-trigger> PDSECURITY.TU_GDW_USER_TYPE </sc-trigger> */


REPLACE TRIGGER PDSECURITY.TU_GDW_USER_TYPE
AFTER UPDATE ON PDSECURITY.GDW_USER_TYPE
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO PDSECURITY.GDW_USER_TYPE_LOG (
      USER_TYPE
      ,USER_TYPE_DESC
      ,USER_TYPE_CLASS
      ,LAST_UPDATE_BY
      ,LAST_UPDATE_AT
      ,LOG_ACTION
      ,LOG_BY
      ,LOG_AT
  ) VALUES (
      O.USER_TYPE
      ,O.USER_TYPE_DESC
      ,O.USER_TYPE_CLASS
      ,O.LAST_UPDATE_BY
      ,O.LAST_UPDATE_AT
      ,'UPDATE'
      ,USER
      ,Current_Timestamp(6)
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_LKP_0.BKEY_TI_Key_Set </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER P_P00_D_TCF_001_LKP_0.BKEY_TI_Key_Set
AFTER INSERT ON P_P00_D_TCF_001_LKP_0.BKEY_T_Key_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_LKP_0.BKEY_T_Key_Set_Log (
      Key_Set_Id
    , Description
    , Key_Table_Name
    , Key_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Description
    , n.Key_Table_Name
    , n.Key_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_P01_D_OPS_001_STD_0.TU_Job_Param </sc-trigger> */
REPLACE TRIGGER P_P01_D_OPS_001_STD_0.TU_Job_Param
AFTER UPDATE ON P_P01_D_OPS_001_STD_0.Job_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P01_D_OPS_001_STD_0.Job_Param_Log (
   Param_File_Name
 , Param_Group
 , Param_Name
 , Param_Value
 , Param_Desc
  ,Param_Std_Code
 , CR_No
 , Update_Action_Code
 , Update_CR_No
 , Update_Session_ID
 , Update_Date
 , Update_User
 , Update_Time
 , Update_Ts
  ) VALUES (
   n.Param_File_Name
 , n.Param_Group
 , n.Param_Name
 , n.Param_Value
 , n.Param_Desc
  ,n.Param_Std_Code
 , n.CR_No
 , 'U'
 , n.CR_No
 , SESSION
 , DATE
 , USER
 , TIME
 , CURRENT_TIMESTAMP(6)
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Stream_Id </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Stream_Id
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Id_Log (
      Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST_TRIG_I </sc-trigger> */
-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST
 
 --------------------------------------------------------------------- 
 --DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receivingInstnCode,
receivingInstnName,
receivingInstnCity,
receivingInstnCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_INST',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receivingInstnCode,
n.receivingInstnName,
n.receivingInstnCity,
n.receivingInstnCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_OMR_001_STD_0.OMR_Alrt_Rqst_AIT </sc-trigger> */
REPLACE TRIGGER P_D_OMR_001_STD_0.OMR_Alrt_Rqst_AIT
AFTER INSERT ON P_D_OMR_001_STD_0.OMR_Alrt_Rqst
REFERENCING NEW ROW AS n
FOR EACH ROW  (
 INSERT INTO DBCMNGR.AlertRequest (
   ReqDate
  ,ReqTime
  ,JobName
  ,Description
  ,EventValue
  ,ActionCode
  ,RepeatPeriod
  ,Destination
  ,Message
 ) VALUES (
   n.ReqDate
  ,n.ReqTime
  ,n.JobName
  ,n.Description
  ,n.EventValue
  ,n.ActionCode
  ,n.RepeatPeriod
  ,n.Destination
  ,TRIM(n.Destination) || ': ' || TRIM(n.Message)
 );
);

--
/* <sc-trigger> BENCHMARK.BUSN_PTNR_TGR_UPD </sc-trigger> */
REPLACE  TRIGGER BUSN_PTNR_TGR_UPD
AFTER UPDATE OF (BUSN_PTNR_NAME) ON BUSN_PTNR_TGR
REFERENCING OLD ROW as BUSN_PTNR_old_row
NEW row  as BUSN_PTNR_new_row
FOR EACH ROW
update BUSN_PTNR_TGR
set UPD_LOAD_S=current_timestamp(6);

--
/* <sc-trigger> P_D_OAC_001_STD_0.OAC_TDPID_Map_AIT </sc-trigger> */
REPLACE TRIGGER P_D_OAC_001_STD_0.OAC_TDPID_Map_AIT
  AFTER INSERT ON P_D_OAC_001_STD_0.OAC_TDPID_Map
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_OAC_001_STD_0.OAC_TDPID_Map_Hist
  (
  TDPID_TDS
 ,TDPID_BAR
 ,First_Type_Cd
 ,First_Id
 ,Create_User
 ,Create_Date
 ,Create_Ts
 ,Update_User
 ,Update_Date
 ,Update_Ts
 ,Modify_Action
   ) VALUES (
  n.TDPID_TDS
 ,n.TDPID_BAR
 ,n.First_Type_Cd
 ,n.First_Id
 ,n.Create_User
 ,n.Create_Date
 ,n.Create_Ts
 ,n.Update_User
 ,n.Update_Date
 ,n.Update_Ts
 ,'I'
   );

--
/* <sc-trigger> B_D_OPS_001_STD_0.R_DIGT_SCHE_FRWK_CHEK_OUT </sc-trigger> */
REPLACE TRIGGER B_D_OPS_001_STD_0.R_DIGT_SCHE_FRWK_CHEK_OUT
BEFORE INSERT ON  U_D_DSV_001_DCD_0.R_DIGT_SCHE_FRWK_PROS
REFERENCING NEW ROW AS n
FOR EACH ROW
WHEN ((SELECT count(*) from U_D_DSV_001_DCD_0.R_DIGT_SCHE_FRWK_PROS)>0)
(
ABORT 'U_D_DSV_001_DCD_0.R_DIGT_SCHE_FRWK_PROS table is being updated by another user. Please check and re-try again later';
);

--
/* <sc-trigger> PDEVNT_H.EVNT_TRG_INS </sc-trigger> */
CREATE TRIGGER PDEVNT_H.EVNT_TRG_INS
 AFTER INSERT ON PDEVNT_H.EVNT_ETL_DATE 
 FOR EACH STATEMENT
 INSERT INTO DBCMNGR.AlertRequest 
 VALUES (DATE, CAST(SUBSTR(CAST(CURRENT_TIME AS CHAR(5)),1,2)||SUBSTR(CAST(CURRENT_TIME AS CHAR(5)),
 4,2)AS INTEGER),'PDEVNT_H','PDEVNT_H.EVNT_ETL_DATE - Insert detected',
 NULL,'E',0,'ESITDSITSMOBusinessIntelligenceOperations.BIGDW@cba.com.au','0A'XC ||'0A'XC ||'Hi Team, '||'0A'XC ||'0A'XC ||'PDEVNT_H.EVNT_ETL_DATE has been changed '|| '0A'XC ||'Please verfiy if PDEVNT_H archival process is running and sync up the relevant PDEVNT_H tables in PROD4. '|| '0A'XC || '0A'XC || '*** PLEASE DO NOT REPLY TO THIS EMAIL ***');

--
/* <sc-trigger> UDGDWOPSWRK.TD_Sandpit_Archive_Param </sc-trigger> */
REPLACE TRIGGER UDGDWOPSWRK.TD_Sandpit_Archive_Param
AFTER DELETE ON UDGDWOPSWRK.Sandpit_Archive_Param
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO UDGDWOPSWRK.Sandpit_Archive_Param_Log (
    Param_Group
    ,Param_Name
    ,Param_Value
    ,Param_Desc
    ,Efft_Date
    ,Expy_Date
    ,Last_Update_By
    ,Last_Update_At
    ,Log_User
    ,Log_Ts
  ) VALUES (
    o.Param_Group
    ,o.Param_Name
    ,o.Param_Value
    ,o.Param_Desc
    ,o.Efft_Date
    ,o.Expy_Date
    ,o.Last_Update_By
    ,o.Last_Update_At
    ,USER
    ,CURRENT_TIMESTAMP(6)
  );
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_System </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_System
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_System_Log (
      Ctl_Id
    , System_Name
    , Path_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.System_Name
    , n.Path_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_D00_D_TCF_001_LKP_0.BMAP_TU_Code_Set </sc-trigger> */


REPLACE TRIGGER D_D00_D_TCF_001_LKP_0.BMAP_TU_Code_Set
AFTER UPDATE ON D_D00_D_TCF_001_LKP_0.BMAP_T_Code_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_LKP_0.BMAP_T_Code_Set_Log (
      Code_Set_Id
    , Description
    , Map_Table_Name
    , Map_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Description
    , n.Map_Table_Name
    , n.Map_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Sys_Name
    ,Update_Tally
    ,Elapsed_Secs
    ,Key_Type1
    ,Key_Type2
    ,Key_Type3
    ,Key_Value
    ,Key_Where_Flag
    ,Key_Where_Val
    ,Key_Where_Cond
    ,Key_Partition_Flag
    ,Key_Partition_Val
    ,Key_Partition_Col
    ,Prev_Create_Date
    ,Prev_Create_Ts
    ,Is_Current_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Db_Name
    ,o.Obj_Name
    ,o.Obj_Type
    ,o.Sys_Name
    ,o.Update_Tally
    ,o.Elapsed_Secs
    ,o.Key_Type1
    ,o.Key_Type2
    ,o.Key_Type3
    ,o.Key_Value
    ,o.Key_Where_Flag
    ,o.Key_Where_Val
    ,o.Key_Where_Cond
    ,o.Key_Partition_Flag
    ,o.Key_Partition_Val
    ,o.Key_Partition_Col
    ,o.Prev_Create_Date
    ,o.Prev_Create_Ts
    ,o.Is_Current_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_TTN_REML_PRJC_MSTR_HIST_DEL </sc-trigger> */

/*CREATE DEL TRIGGER FOR  TTN_REML_PRJC_MSTR_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_TTN_REML_PRJC_MSTR_HIST_DEL 
AFTER DELETE ON U_D_DSV_001_REM_1.TTN_REML_PRJC_MSTR_HIST 
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.TTN_REML_PRJC_MSTR_HIST_LOG  SELECT A.*,USER,'DEL', CURRENT_TIMESTAMP FROM O1 AS A;

--
/* <sc-trigger> PEARSODCD3.dilg_run_data_purge </sc-trigger> */
CREATE TRIGGER pearsodcd3.dilg_run_data_purge
AFTER INSERT ON udmacamp.dilg_purge_tmp
REFERENCING NEW AS n  FOR EACH ROW
(
DEL FROM udmacamp.dilg_run_data d 
WHERE d.acct_i = n.acct_i 
AND d.dilg_run_cfig_i=n.dilg_run_cfig_i;
);

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Param_Group </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Param_Group
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Param_Group
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Param_Group_Log (
      Param_Group
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Param_Group
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Transform </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Transform
AFTER INSERT ON PDGENTCF.CTLFW_T_Transform
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Transform_Log 
 (
 Process_Name
 ,Business_Date
 ,Business_Date_Cycle_Num
 ,Business_Date_Cycle_Start_Ts
 ,Process_Id
 ,TD_Session_Id
 ,Tool_Session_Id
 ,In_DB_Name
 ,In_Object_Name
 ,Out_DB_Name
 ,Out_Object_Name
 ,Rows_Inserted
 ,Rows_Updated
 ,Rows_Deleted
 ,Run_Date
 ,Start_Time
 ,End_Time
 ,Update_Date
 ,Update_User
 ,Update_Time
 ,Update_Ts
 ) 
VALUES 
 (
 n.Process_Name
 ,n.Business_Date
 ,n.Business_Date_Cycle_Num
 ,n.Business_Date_Cycle_Start_Ts
 ,n.Process_Id
 ,n.TD_Session_Id
 ,n.Tool_Session_Id
 ,n.In_DB_Name
 ,n.In_Object_Name
 ,n.Out_DB_Name
 ,n.Out_Object_Name
 ,n.Rows_Inserted
 ,n.Rows_Updated
 ,n.Rows_Deleted
 ,n.Run_Date
 ,n.Start_Time
 ,n.End_Time
 ,n.Update_Date
 ,n.Update_User
 ,n.Update_Time
 ,n.Update_Ts
 );
);

--
/* <sc-trigger> PDBIDM.TU_STRM_PARAM_CNTL </sc-trigger> */
REPLACE TRIGGER PDBIDM.TU_STRM_PARAM_CNTL
AFTER UPDATE ON PDBIDM.STRM_PARAM_CNTL
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDBIDM.STRM_PARAM_CNTL_LOG (
     STREAM_KEY
     ,STREAM_ID
     ,PARAM_DATE_NAME_1
     ,PARAM_DATE_VALU_1
     ,PARAM_DATE_NAME_2
     ,PARAM_DATE_VALU_2
     ,PARAM_DATE_NAME_3
     ,PARAM_DATE_VALU_3
     ,PARAM_DATE_NAME_4
     ,PARAM_DATE_VALU_4
     ,PARAM_DATE_NAME_5
     ,PARAM_DATE_VALU_5
     ,PARAM_DTTS_NAME_1
     ,PARAM_DTTS_VALU_1
     ,PARAM_DTTS_NAME_2
     ,PARAM_DTTS_VALU_2
     ,PARAM_DTTS_NAME_3
     ,PARAM_DTTS_VALU_3
     ,PARAM_DTTS_NAME_4
     ,PARAM_DTTS_VALU_4
     ,PARAM_DTTS_NAME_5
     ,PARAM_DTTS_VALU_5
     ,PARAM_VARC_NAME_1
     ,PARAM_VARC_VALU_1
     ,PARAM_VARC_NAME_2
     ,PARAM_VARC_VALU_2
     ,PARAM_VARC_NAME_3
     ,PARAM_VARC_VALU_3
     ,PARAM_VARC_NAME_4
     ,PARAM_VARC_VALU_4
     ,PARAM_VARC_NAME_5
     ,PARAM_VARC_VALU_5
     ,PARAM_ITGR_NAME_1
     ,PARAM_ITGR_VALU_1
     ,PARAM_ITGR_NAME_2
     ,PARAM_ITGR_VALU_2
     ,PARAM_ITGR_NAME_3
     ,PARAM_ITGR_VALU_3
     ,PARAM_ITGR_NAME_4
     ,PARAM_ITGR_VALU_4
     ,PARAM_ITGR_NAME_5
     ,PARAM_ITGR_VALU_5
     ,PARAM_DCML_NAME_1
     ,PARAM_DCML_VALU_1
     ,PARAM_DCML_NAME_2
     ,PARAM_DCML_VALU_2
     ,PARAM_DCML_NAME_3
     ,PARAM_DCML_VALU_3
     ,PARAM_DCML_NAME_4
     ,PARAM_DCML_VALU_4
     ,PARAM_DCML_NAME_5
     ,PARAM_DCML_VALU_5
     ,LOG_DATE
     ,LOG_TIME
     ,LOG_DTTS
     ,LOG_USER
  ) 
  SELECT 
     n.STREAM_KEY
	 ,STRMID.STREAM_ID
     ,n.PARAM_DATE_NAME_1
     ,n.PARAM_DATE_VALU_1
     ,n.PARAM_DATE_NAME_2
     ,n.PARAM_DATE_VALU_2
     ,n.PARAM_DATE_NAME_3
     ,n.PARAM_DATE_VALU_3
     ,n.PARAM_DATE_NAME_4
     ,n.PARAM_DATE_VALU_4
     ,n.PARAM_DATE_NAME_5
     ,n.PARAM_DATE_VALU_5
     ,n.PARAM_DTTS_NAME_1
     ,n.PARAM_DTTS_VALU_1
     ,n.PARAM_DTTS_NAME_2
     ,n.PARAM_DTTS_VALU_2
     ,n.PARAM_DTTS_NAME_3
     ,n.PARAM_DTTS_VALU_3
     ,n.PARAM_DTTS_NAME_4
     ,n.PARAM_DTTS_VALU_4
     ,n.PARAM_DTTS_NAME_5
     ,n.PARAM_DTTS_VALU_5
     ,n.PARAM_VARC_NAME_1
     ,n.PARAM_VARC_VALU_1
     ,n.PARAM_VARC_NAME_2
     ,n.PARAM_VARC_VALU_2
     ,n.PARAM_VARC_NAME_3
     ,n.PARAM_VARC_VALU_3
     ,n.PARAM_VARC_NAME_4
     ,n.PARAM_VARC_VALU_4
     ,n.PARAM_VARC_NAME_5
     ,n.PARAM_VARC_VALU_5
     ,n.PARAM_ITGR_NAME_1
     ,n.PARAM_ITGR_VALU_1
     ,n.PARAM_ITGR_NAME_2
     ,n.PARAM_ITGR_VALU_2
     ,n.PARAM_ITGR_NAME_3
     ,n.PARAM_ITGR_VALU_3
     ,n.PARAM_ITGR_NAME_4
     ,n.PARAM_ITGR_VALU_4
     ,n.PARAM_ITGR_NAME_5
     ,n.PARAM_ITGR_VALU_5
     ,n.PARAM_DCML_NAME_1
     ,n.PARAM_DCML_VALU_1
     ,n.PARAM_DCML_NAME_2
     ,n.PARAM_DCML_VALU_2
     ,n.PARAM_DCML_NAME_3
     ,n.PARAM_DCML_VALU_3
     ,n.PARAM_DCML_NAME_4
     ,n.PARAM_DCML_VALU_4
     ,n.PARAM_DCML_NAME_5
     ,n.PARAM_DCML_VALU_5
	 ,DATE
	 ,TIME
	 ,CURRENT_TIMESTAMP(0)
	 ,USER
FROM PVGENTCF.CTLFW_Stream_Id STRMID
WHERE STRMID.STREAM_KEY = n.STREAM_KEY
;
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Stus_ADT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Stus_ADT
  AFTER DELETE ON PDGDWPIIOBF.Dtf_Job_Stus
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Stus_Hst
  (
     Stus_Cd
    ,Stus_Desn
    ,Modify_Action
   ) VALUES (
     o.Stus_Cd
    ,o.Stus_Desn
    ,'D'
   );

--
/* <sc-trigger> PDGEN_REF.BMAP_TU_Code_Set </sc-trigger> */
REPLACE TRIGGER PDGEN_REF.BMAP_TU_Code_Set
AFTER UPDATE ON PDGEN_REF.BMAP_T_Code_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGEN_REF.BMAP_T_Code_Set_Log (
      Code_Set_Id
    , Description
    , Map_Table_Name
    , Map_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Description
    , n.Map_Table_Name
    , n.Map_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_S00_D_TCF_001_LKP_0.BKEY_TU_Domain </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_LKP_0.BKEY_TU_Domain
AFTER UPDATE ON D_S00_D_TCF_001_LKP_0.BKEY_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_LKP_0.BKEY_T_Domain_Log (
      Key_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDHCLWR.BUSN_PTNR_TGR_UPD </sc-trigger> */
REPLACE  TRIGGER pdhclwr.BUSN_PTNR_TGR_UPD
AFTER UPDATE OF (BUSN_PTNR_NAME) ON pdhclwr.BUSN_PTNR_TGR
REFERENCING OLD ROW as BUSN_PTNR_old_row
 NEW row  as BUSN_PTNR_new_row
 FOR EACH ROW
update pdhclwr.BUSN_PTNR_TGR
set UPD_LOAD_S=current_timestamp(6);

--
/* <sc-trigger> P_D_OPS_001_STD_0.TI_Job_Param </sc-trigger> */
REPLACE TRIGGER P_D_OPS_001_STD_0.TI_Job_Param
AFTER INSERT ON P_D_OPS_001_STD_0.Job_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_D_OPS_001_STD_0.Job_Param_Log (
   Param_File_Name
 , Param_Group
 , Param_Name
 , Param_Value
 , Param_Desc
 , Param_Std_Code
 , CR_No
 , Update_Action_Code
 , Update_CR_No
 , Update_Session_ID
 , Update_Date
 , Update_User
 , Update_Time
 , Update_Ts
  ) VALUES (
   n.Param_File_Name
 , n.Param_Group
 , n.Param_Name
 , n.Param_Value
 , n.Param_Desc
  ,n.Param_Std_Code
 , n.CR_No
 , 'I'
 , n.CR_No
 , SESSION
 , DATE
 , USER
 , TIME
 , CURRENT_TIMESTAMP(6)
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL_TRIG_I </sc-trigger> */


REPLACE TRIGGER U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL_TRIG_I
AFTER INSERT ON U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL_AUDT
(
BSB                           
,ACCT_I                        
,AMT                           
,CUST_I                        
,MNTR_ACCT_N                   
,STMT_X                        
,SRCE                          
,BENE_IMPCT_D                  
,NOTE                          
,BENE_N                        
,STUS                          
,STUS_N                        
,STUS_X                        
,STUS_UPDT_DT                  
,IMPT_FILE_NAME                
,EXPT_FILE_NAME                
,FINL_ALT_BSB_1                
,FINL_ALT_ACCT_I_1             
,FINL_ALT_BSB_2                
,FINL_ALT_ACCT_I_2             
,FINL_ALT_BSB_3                
,FINL_ALT_ACCT_I_3             
,FINL_ALT_ACCT_UPDT_DT         
,UPDT_D                        
,RECD_I                                          
,RECON_TRAN_I
,EVNT_I
,RECON_TRAN_D
,RECON_ACCT_I
,AMT_TYPE                      
,REFN_I                        
,REFN_INTL                     
,REFN_EXTL    
,MODF_BY     
,MODF_DTTS                     
,OPER_TYPE                                            

)
VALUES
(
 n.BSB                           
,n.ACCT_I                        
,n.AMT                           
,n.CUST_I                        
,n.MNTR_ACCT_N                   
,n.STMT_X                        
,n.SRCE                          
,n.BENE_IMPCT_D                  
,n.NOTE                          
,n.BENE_N                        
,n.STUS                          
,n.STUS_N                        
,n.STUS_X                        
,n.STUS_UPDT_DT                  
,n.IMPT_FILE_NAME                
,n.EXPT_FILE_NAME                
,n.FINL_ALT_BSB_1                
,n.FINL_ALT_ACCT_I_1             
,n.FINL_ALT_BSB_2                
,n.FINL_ALT_ACCT_I_2             
,n.FINL_ALT_BSB_3                
,n.FINL_ALT_ACCT_I_3             
,n.FINL_ALT_ACCT_UPDT_DT         
,n.UPDT_D                        
,n.RECD_I    
,n.RECON_TRAN_I
,n.EVNT_I
,n.RECON_TRAN_D
,n.RECON_ACCT_I
,n.AMT_TYPE                      
,n.REFN_I                        
,n.REFN_INTL                     
,n.REFN_EXTL 
,USER
,Current_Timestamp(6)
,'INSERT'
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Repl_Type_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Repl_Type_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Repl_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Repl_Type_Hist
  (
     Repl_Type_Cd
    ,Repl_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Repl_Type_Cd
    ,n.Repl_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> DRUMMOMAD3.cnsd_pros_actv_trig </sc-trigger> */
--------------------------------------------------------------------------------

REPLACE TRIGGER drummomad3.cnsd_pros_actv_trig
AFTER INSERT ON pdi3devl._cnsd_pros_actv_log
REFERENCING NEW AS a2  FOR EACH ROW
(

INSERT INTO pdi3devl.cnsd_pros_logs
( pros_key_n
, pros_logs_c
, pros_logs_desn_x
, pros_actv_c
, pros_valu_c
, pros_valu_n )
SELECT a1.pros_key_n
     , a1.pros_logs_c
     , a1.pros_logs_desn_x
     , a2.pros_actv_c
     , a2.pros_valu_c
     , a2.pros_valu_n
FROM pvi3view._cnsd_pros_key_vary a1;

DELETE FROM pdi3devl._cnsd_pros_actv_log ALL ;
);

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_System_File_Param </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_System_File_Param
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_System_File_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_System_File_Param_Log (
      Ctl_Id
 , File_Id
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
 , n.File_Id
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> UDGDWOPS.TU_Sandpit_Archive_Param </sc-trigger> */
REPLACE TRIGGER UDGDWOPS.TU_Sandpit_Archive_Param
AFTER UPDATE ON UDGDWOPS.Sandpit_Archive_Param
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO UDGDWOPS.Sandpit_Archive_Param_Log (
    Param_Group
    ,Param_Name
    ,Param_Value
    ,Param_Desc
    ,Efft_Date
    ,Expy_Date
    ,Last_Update_By
    ,Last_Update_At
    ,Log_User
    ,Log_Ts
  ) VALUES (
    o.Param_Group
    ,o.Param_Name
    ,o.Param_Value
    ,o.Param_Desc
    ,o.Efft_Date
    ,o.Expy_Date
    ,o.Last_Update_By
    ,o.Last_Update_At
    ,USER
    ,CURRENT_TIMESTAMP(6)
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.MT202_EXC_RPRT_AUDT
(
REPORT                        
,SWIFT_MESSAGE_TEXT            
,UNIQUE_IDENTIFIER             
,MSG_ID                        
,MSG_SEQ                       
,MSG_TYPE                      
,DB_DATE                       
,SOURCE                        
,BRAND                         
,CBA_BIC                       
,EXTERNAL_PARTY_BIC            
,INP_OUT_IDENTIFIER            
,AMOUNT                        
,CURRENCY                      
,VAL_DATE                      
,REMITTER                      
,BENEFICIARY                   
,FIELD_20                      
,FIELD_21                      
,CREATED_DATE                  
,CDT_ID                        
,CDT_NAME                      
,DBT_ID                        
,DBT_NAME                      
,TAG_52                        
,TAG_58                        
,TAG_72                        
,SWF_FILE_REF                  
,TRN_PROC_PMS                  
,BUSN_DAYS                     
,IS_REVIEWED                   
,TEAM_RESP                     
,INVESTIGATION_STUS            
,TRAN_CATG                     
,PRODUCT                       
,OTHR_CMMT                     
,VERF_BY_IFTI_MNGR             
,IFTI_RPRT                     
,SUBM_FILE_NAME                
,SUBM_DATE                     
,SUBM_RCPT_NO                  
,INSERTION_DATE                
,EXC_OFCR                      
,EXC_OFCR_DTTM                 
,VERF_OFCR_CMMT                
,VERF_OFCR                     
,VERF_OFCR_DTTM                
,QA_OFCR_CMMT                  
,QA_OFCR                       
,QA_OFCR_DTTM                  
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(
n.REPORT                        
,n.SWIFT_MESSAGE_TEXT            
,n.UNIQUE_IDENTIFIER             
,n.MSG_ID                        
,n.MSG_SEQ                       
,n.MSG_TYPE                      
,n.DB_DATE                       
,n.SOURCE                        
,n.BRAND                         
,n.CBA_BIC                       
,n.EXTERNAL_PARTY_BIC            
,n.INP_OUT_IDENTIFIER            
,n.AMOUNT                        
,n.CURRENCY                      
,n.VAL_DATE                      
,n.REMITTER                      
,n.BENEFICIARY                   
,n.FIELD_20                      
,n.FIELD_21                      
,n.CREATED_DATE                  
,n.CDT_ID                        
,n.CDT_NAME                      
,n.DBT_ID                        
,n.DBT_NAME                      
,n.TAG_52                        
,n.TAG_58                        
,n.TAG_72                        
,n.SWF_FILE_REF                  
,n.TRN_PROC_PMS                  
,n.BUSN_DAYS                     
,n.IS_REVIEWED                   
,n.TEAM_RESP                     
,n.INVESTIGATION_STUS            
,n.TRAN_CATG                     
,n.PRODUCT                       
,n.OTHR_CMMT                     
,n.VERF_BY_IFTI_MNGR             
,n.IFTI_RPRT                     
,n.SUBM_FILE_NAME                
,n.SUBM_DATE                     
,n.SUBM_RCPT_NO                  
,n.INSERTION_DATE                
,n.EXC_OFCR                      
,n.EXC_OFCR_DTTM                 
,n.VERF_OFCR_CMMT                
,n.VERF_OFCR                     
,n.VERF_OFCR_DTTM                
,n.QA_OFCR_CMMT                  
,n.QA_OFCR                       
,n.QA_OFCR_DTTM                  
,USER
,Current_Timestamp(6)
,'DELETE' 
);

--
/* <sc-trigger> PDDQRM.TRG_DQRM_DATA_QUALITY_RULE_AU </sc-trigger> */
REPLACE TRIGGER PDDQRM.TRG_DQRM_DATA_QUALITY_RULE_AU 
AFTER UPDATE ON PDDQRM.DQRM_DATA_QUALITY_RULE 
REFERENCING OLD ROW AS O FOR EACH ROW 
INSERT INTO PDDQRM.DQ_RULE_HIST                                                                                                                           
(RULE_IDNN
,RULE_M
,RULE_X
,RULE_N
,RULE_RVSD
,RULE_STRT_D
,RULE_END_D
,QUAL_TSHD
,QUAL_GOAL_STAT
,FNCL_IMPA_A
,FNCL_IMPA_UNIT
,BUSN_IMPA_NOTE
,RULE_RVSD_NOTE
,DEVT_COND_CLAU
,FROM_CLAU
,WHER_CLAU
,MTRC_SQL_LOCK_F
,RUN_PRE_F
,RUN_POST_F
,MTRC_SQL
,PRE_PROS_SQL
,GENR_PRE_PROS_SQL
,POST_PROS_SQL
,GENR_POST_PROS_SQL
,STOR_DETL_DEVT_F
,DELT_PREV_DEVT_F
,DEVT_SQL_LOCK_F
,HIST_DAY
,HIST_ROW
,DEVT_SQL
,DQ_RULE_PARN_IDNN
,APPT_ITEM_IDNN
,STUS_ITEM_IDNN
,FREQ_ITEM_IDNN
,TYPE_ITEM_IDNN
,SBTY_ITEM_IDNN
,DATA_CSTI_ITEM_IDNN
,WGHT_ITEM_IDNN
,VERS
,MODF_D
,MODF_BY
,STOR_DETL_CNT_F
,DELT_PREV_CNT_F
,CNT_SQL_LOCK_F
,CNT_HIST_DAY
,CNT_HIST_ROW
,CNT_SQL      
,ACTV_BY_I
,TRIG_S
,TRIG_ACTV_X 
,METRIC_USE_PROCESS_ID_FLAG
,DEVIATION_USE_PROCESS_ID_FLAG  
)
values 
(
o.RULE_ID  
,o.RULE_NAME
,o.RULE_DESC
,o.RULE_NUMBER
,o.RULE_REVISION
,o.RULE_START_DATE
,o.RULE_END_DATE
,o.QLTY_THRESHOLD
,o.QLTY_GOAL_STATE
,o.FIN_IMPACT_AMT
,o.FIN_IMPACT_UNIT
,o.BUS_IMPACT_NOTES
,o.RULE_REVISION_NOTES
,o.DEVIATION_COND_CLAUSE
,o.FROM_CLAUSE
,o.WHERE_CLAUSE
,o.METRIC_SQL_LOCK_FLAG
,o.RUN_PRE_FLAG
,o.RUN_POST_FLAG
,o.METRIC_SQL
,o.PRE_PROC_SQL
,o.GENERATED_PRE_PROC_SQL
,o.POST_PROC_SQL
,o.GENERATED_POST_PROC_SQL
,o.STORE_DETAIL_DEV_FLAG
,o.DELETE_PREV_DEV_FLAG
,o.DEV_SQL_LOCK_FLAG
,o.HISTORY_DAYS
,o.HISTORY_ROWS
,o.DEVIATION_SQL
,o.DQR_PARENT_ID
,o.APPLICATION_ITEM_ID
,o.STATUS_ITEM_ID
,o.FREQUENCY_ITEM_ID
,o.TYPE_ITEM_ID
,o.SUBTYPE_ITEM_ID
,o.DATA_STEWARD_ITEM_ID
,o.WEIGHTING_ITEM_ID
,o.VERSION
,o.MODIFIED_DATE
,o.MODIFIED_BY
,o.STORE_DETAIL_COUNTS_FLAG
,o.DELETE_PREV_COUNTS_FLAG
,o.COUNTS_SQL_LOCK_FLAG
,o.COUNTS_HISTORY_DAYS
,o.COUNTS_HISTORY_ROWS
,o.COUNTS_SQL
,USER   
,CURRENT_TIMESTAMP(0)   
,'UPDATE' 
,o.METRIC_USE_PROCESS_ID_FLAG
,o.DEVIATION_USE_PROCESS_ID_FLAG
);

--
/* <sc-trigger> U_D_DSV_001_CMP_0.PROCESSTBL_UpdTrg </sc-trigger> */
replace trigger U_D_DSV_001_CMP_0.PROCESSTBL_UpdTrg
after update on U_D_DSV_001_CMP_0.PROCESSTBL
referencing new row as n
for each row
insert into U_D_DSV_001_CMP_0.PROCESSTBL_LOG 
(
     Process_Name 
	,Process_Frequency 
    ,Business_Date 
    ,Business_End_Date
    ,Status	
    ,CommentString	
)
values 
(
     n.Process_Name
	,n.Process_Frequency 
    ,n.Business_Date 
    ,n.Business_End_Date    
    ,n.Status	
    ,n.CommentString	
) ;

--
/* <sc-trigger> Which_System.Repl_Matrix_AUT </sc-trigger> */
REPLACE TRIGGER Which_System.Repl_Matrix_AUT
  AFTER UPDATE ON Which_System.Repl_Matrix
  REFERENCING NEW ROW AS o FOR EACH ROW
  INSERT INTO Which_System.Repl_Matrix_Hist
  (
     Platform_Name
,Description
,Platform_Role
,Platform_Stat
,Platform_Function
,Platform_Repl_Prfl
,TDS_Env
,Create_User
,Create_Date
,Create_Ts
,Update_User
,Update_Date
,Update_Ts
,Modify_Action

   ) VALUES (
o.Platform_Name
,o.Description
,o.Platform_Role
,o.Platform_Stat
,o.Platform_Function
,o.Platform_Repl_Prfl
,o.TDS_Env
,o.Create_User
,o.Create_Date
,o.Create_Ts
,o.Update_User
,o.Update_Date
,o.Update_Ts
,'U'
   );

--
/* <sc-trigger> PDTRMEXTD.GENID_STEP </sc-trigger> */
--------------------------------------------------------------------------------

REPLACE TRIGGER PDTRMEXTD.GENID_STEP
AFTER INSERT ON PDTRMCORE.EX_STEP
	REFERENCING NEW AS NEWROW 
	FOR EACH ROW
(

LOCKING TABLE PDTRMCORE.EX_STEP FOR WRITE

/* set CM3_Step_Id */
UPDATE A 
FROM PDTRMCORE.EX_STEP AS A
, ( 
	SELECT	CPS.Step_Id, CPS.Display_Ord AS New_Step_Id
	FROM	PDTRMCORE.CM_COMM_PLAN_STEP CPS
	INNER JOIN
		PDTRMCORE.EX_STEP EXS
	ON	CPS.Step_Id = EXS.Step_Id
	WHERE	EXS.CM3_Step_Id IS NULL
) B
SET	CM3_Step_Id  = B.New_Step_Id
WHERE	A.Step_Id = NEWROW.Step_Id
AND	B.Step_Id = NEWROW.Step_Id
 ;   
);

--
/* <sc-trigger> PDSECURITY.TD_GDW_BATCH_USER_INFO_STG </sc-trigger> */
REPLACE TRIGGER PDSECURITY.TD_GDW_BATCH_USER_INFO_STG
AFTER DELETE ON PDSECURITY.GDW_BATCH_USER_INFO_STG
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO PDSECURITY.GDW_BATCH_USER_INFO_STG_LOG (
    USERNAME
    ,MY_SERVICE_NO
    ,CMMT
    ,APP_CODE
    ,TGT_ENV
    ,SUBS_NAME
    ,STMT_TYPE
    ,SESSION_ID
    ,UPDT_USERNAME
    ,UPDT_DATE
    ,UPDT_DTTS
  ) VALUES (
    O.USERNAME
    ,O.MY_SERVICE_NO
    ,O.CMMT
    ,O.APP_CODE
    ,O.TGT_ENV
    ,O.SUBS_NAME
    ,'DELETE'
    ,SESSION
    ,USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(0)
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_LKP_0.BMAP_TI_Valid_Language </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER P_P00_D_TCF_001_LKP_0.BMAP_TI_Valid_Language
AFTER INSERT ON P_P00_D_TCF_001_LKP_0.BMAP_T_Valid_Language
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_LKP_0.BMAP_T_Valid_Language_Log (
      Language_Id
    , Language_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Language_Id
    , n.Language_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_P01_D_OPS_001_STD_0.TD_Job_Param </sc-trigger> */
REPLACE TRIGGER P_P01_D_OPS_001_STD_0.TD_Job_Param
AFTER DELETE ON P_P01_D_OPS_001_STD_0.Job_Param
REFERENCING OLD ROW AS O  
FOR EACH ROW  (
INSERT INTO P_P01_D_OPS_001_STD_0.Job_Param_Log (
   Param_File_Name
 , Param_Group
 , Param_Name
 , Param_Value
 , Param_Desc
  ,Param_Std_Code
 , CR_No
 , Update_Action_Code
 , Update_CR_No
 , Update_Session_ID
 , Update_Date
 , Update_User
 , Update_Time
 , Update_Ts
  ) VALUES (
   o.Param_File_Name
 , o.Param_Group
 , o.Param_Name
 , o.Param_Value
 , o.Param_Desc
  ,o.Param_Std_Code
 , o.CR_No
 , 'D'
 , NULL
 , SESSION
 , DATE
 , USER
 , TIME
 , CURRENT_TIMESTAMP(6)
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_File_Process </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_File_Process
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_File_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_File_Process_Log (
      Process_Name
    , Ctl_Id
    , File_Id
    , Check_Load_Status
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Ctl_Id
    , n.File_Id
    , n.Check_Load_Status
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */

Replace TRIGGER U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_AUDT
(stmt_line_sk
,unique_id
,rprt_name
,acct_no
,acct_name
,acct_cncy
,acct_baln_pool
,date_tag_created
,value_date
,amt
,txn_type
,internal_ref
,external_ref
,description
,swf_file_ref
,mtch_type
,team_resp
,investigation_stus
,tran_catg
,is_reviewed
,othr_cmmt
,busn_days
,ifti_rprt
,subm_date
,subm_file_name
,subm_rcpt_no
,imt_mtch_grup_id
,imt_refn_1
,imt_refn_2
,imt_refn_3
,pms_ref_no
,pms_dbt_name
,pms_sbk_ref_no
,wss_srce
,wss_portfolio
,wss_audt_note
,wss_net_info
,wss_area
,itc_case_no
,wss_extl_deal_numb
,action_date
,EXC_OFCR
,EXC_OFCR_DTTM
,VERF_OFCR_CMMT
,VERF_OFCR
,VERF_OFCR_DTTM
,QA_OFCR_CMMT
,QA_OFCR
,QA_OFCR_DTTM
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(n.stmt_line_sk
,n.unique_id
,n.rprt_name
,n.acct_no
,n.acct_name
,n.acct_cncy
,n.acct_baln_pool
,n.date_tag_created
,n.value_date
,n.amt
,n.txn_type
,n.internal_ref
,n.external_ref
,n.description
,n.swf_file_ref
,n.mtch_type
,n.team_resp
,n.investigation_stus
,n.tran_catg
,n.is_reviewed
,n.othr_cmmt
,n.busn_days
,n.ifti_rprt
,n.subm_date
,n.subm_file_name
,n.subm_rcpt_no
,n.imt_mtch_grup_id
,n.imt_refn_1
,n.imt_refn_2
,n.imt_refn_3
,n.pms_ref_no
,n.pms_dbt_name
,n.pms_sbk_ref_no
,n.wss_srce
,n.wss_portfolio
,n.wss_audt_note
,n.wss_net_info
,n.wss_area
,n.itc_case_no
,n.wss_extl_deal_numb
,n.action_date
,n.EXC_OFCR
,n.EXC_OFCR_DTTM
,n.VERF_OFCR_CMMT
,n.VERF_OFCR
,n.VERF_OFCR_DTTM
,n.QA_OFCR_CMMT
,n.QA_OFCR
,n.QA_OFCR_DTTM
,USER
,Current_Timestamp(6)
,'DELETE' 
);

--
/* <sc-trigger> P_D_OAC_001_STD_0.OAC_TDPID_Map_ADT </sc-trigger> */
REPLACE TRIGGER P_D_OAC_001_STD_0.OAC_TDPID_Map_ADT
  AFTER DELETE ON P_D_OAC_001_STD_0.OAC_TDPID_Map
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_OAC_001_STD_0.OAC_TDPID_Map_Hist
  (
  TDPID_TDS
 ,TDPID_BAR
 ,First_Type_Cd
 ,First_Id
 ,Create_User
 ,Create_Date
 ,Create_Ts
 ,Update_User
 ,Update_Date
 ,Update_Ts
 ,Modify_Action
   ) VALUES (
  o.TDPID_TDS
 ,o.TDPID_BAR
 ,o.First_Type_Cd
 ,o.First_Id
 ,o.Create_User
 ,o.Create_Date
 ,o.Create_Ts
 ,CURRENT_USER
 ,CURRENT_DATE
 ,CURRENT_TIMESTAMP(6)
 ,'D'
   );

--
/* <sc-trigger> B_D_OPS_001_STD_0.R_DIGT_SCHE_FRWK_ISRT </sc-trigger> */
REPLACE TRIGGER B_D_OPS_001_STD_0.R_DIGT_SCHE_FRWK_ISRT
BEFORE INSERT ON  B_D_OPS_001_STD_0.R_DIGT_SCHE_FRWK_PROS
REFERENCING NEW ROW AS newrow
FOR EACH row
WHEN ((select count(*) from B_D_OPS_001_STD_0.R_DIGT_SCHE_FRWK_PROS  n 
INNER JOIN  U_D_DSV_001_DCD_0.R_DIGT_SCHE_FRWK_PROS o
on n.TARG_TABL_M=o.TARG_TABL_M and n.TARG_DB_M=o.TARG_DB_M and n.EXPY_S='9999-12-31 23:59:59')>=1
)
(
ABORT 'Already an active record for the same table is present in Framework R_DIGT_SCHE_FRWK_PROS table. Please Use  R_DIGT_SCHE_FRWK_CHEK_IN to update and insert new record';
);

--
/* <sc-trigger> PDEVNT_H.EVNT_TRG_UPD </sc-trigger> */
CREATE TRIGGER PDEVNT_H.EVNT_TRG_UPD
 AFTER UPDATE ON PDEVNT_H.EVNT_ETL_DATE 
 FOR EACH STATEMENT
 INSERT INTO DBCMNGR.AlertRequest 
 VALUES (DATE, CAST(SUBSTR(CAST(CURRENT_TIME AS CHAR(5)),1,2)||SUBSTR(CAST(CURRENT_TIME AS CHAR(5)),
 4,2)AS INTEGER),'PDEVNT_H','PDEVNT_H.EVNT_ETL_DATE - Update detected',
 NULL,'E',0,'ESITDSITSMOBusinessIntelligenceOperations.BIGDW@cba.com.au','0A'XC ||'0A'XC ||'Hi Team, '||'0A'XC ||'0A'XC ||'PDEVNT_H.EVNT_ETL_DATE has been changed '|| '0A'XC ||'Please verfiy if PDEVNT_H archival process is running and sync up the relevant PDEVNT_H tables in PROD4. '|| '0A'XC || '0A'XC || '*** PLEASE DO NOT REPLY TO THIS EMAIL ***');

--
/* <sc-trigger> UDGDWOPSWRK.TU_Sandpit_Archive_Status </sc-trigger> */
REPLACE TRIGGER UDGDWOPSWRK.TU_Sandpit_Archive_Status
AFTER UPDATE ON UDGDWOPSWRK.Sandpit_Archive_Status
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO UDGDWOPSWRK.Sandpit_Archive_Status_Log (
    Batch_Id
    ,Batch_Run_Date
    ,Batch_State
    ,Start_Time
    ,End_Time
    ,Session_Id
    ,Last_Update_By
    ,Last_Update_At
    ,Log_User
    ,Log_Ts
  ) VALUES (
    o.Batch_Id
    ,o.Batch_Run_Date
    ,o.Batch_State
    ,o.Start_Time
    ,o.End_Time
    ,o.Session_Id
    ,o.Last_Update_By
    ,o.Last_Update_At
    ,USER
    ,CURRENT_TIMESTAMP(6)
  );
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Corporate_Entity </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Corporate_Entity
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity_Log (
      CE_Id
    , Corporate_Entity_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Corporate_Entity_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_D00_D_TCF_001_LKP_0.BKEY_TI_Key_Set </sc-trigger> */


REPLACE TRIGGER D_D00_D_TCF_001_LKP_0.BKEY_TI_Key_Set
AFTER INSERT ON D_D00_D_TCF_001_LKP_0.BKEY_T_Key_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_LKP_0.BKEY_T_Key_Set_Log (
      Key_Set_Id
    , Description
    , Key_Table_Name
    , Key_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Description
    , n.Key_Table_Name
    , n.Key_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_Hist
  (
     Stream_Name
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Stream_Name
    ,o.Db_Name
    ,o.Obj_Name
    ,o.Obj_Type
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_REML_PRJC_MSTR_HIST_UPD </sc-trigger> */


/*CREATE UPD TRIGGER FOR  REML_PRJC_MSTR_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_REML_PRJC_MSTR_HIST_UPD
AFTER UPDATE ON U_D_DSV_001_REM_1.REML_PRJC_MSTR_HIST
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.REML_PRJC_MSTR_HIST_LOG  SELECT A.*,USER,'UPD', CURRENT_TIMESTAMP  FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFWX_TU_Xfm_Process </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFWX_TU_Xfm_Process
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process
REFERENCING NEW ROW AS NR
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process_Log (
      Process_Name,           
      Busn_Dt ,         
      Xfm_Run_Dt ,                        
      Xfm_Rows_Inserted ,                       
      Xfm_Stg_Ld_Status ,           
      Xfm_Stg_Ld_Start_Time ,
      Xfm_Stg_Ld_End_Time ,   
      Xfm_Stg_Val_Start_Time ,
      Xfm_Stg_Val_End_Time ,  
      Msur_Mdl_Pub_Run_Dt ,         
      Msur_Mdl_Pub_Process_Id ,           
      Msur_Mdl_Pub_Start_Time ,           
      Msur_Mdl_Pub_End_Time ,
      Msur_Mdl_Pub_Status ,   
      Msur_Mdl_Pub_Priority , 
      Pub_Stream_Key ,
      Update_User ,           
      Update_Ts 
  ) VALUES (
      NR.Process_Name,        
      NR.Busn_Dt ,            
      NR.Xfm_Run_Dt ,                     
      NR.Xfm_Rows_Inserted ,                    
      NR.Xfm_Stg_Ld_Status ,        
      NR.Xfm_Stg_Ld_Start_Time ,
      NR.Xfm_Stg_Ld_End_Time ,
      NR.Xfm_Stg_Val_Start_Time ,
      NR.Xfm_Stg_Val_End_Time ,     
      NR.Msur_Mdl_Pub_Run_Dt ,            
      NR.Msur_Mdl_Pub_Process_Id ,        
      NR.Msur_Mdl_Pub_Start_Time ,        
      NR.Msur_Mdl_Pub_End_Time ,
      NR.Msur_Mdl_Pub_Status ,      
      NR.Msur_Mdl_Pub_Priority ,    
      NR.Pub_Stream_Key ,
      NR.Update_User ,        
      NR.Update_Ts 
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Stream_Id </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Stream_Id
AFTER UPDATE ON PDGENTCF.CTLFW_T_Stream_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Stream_Id_Log (
      Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDBIDM.TI_STRM_PARAM_CNTL </sc-trigger> */
/*########################################################################
 BICD Data Reconciliation Log Maintenance Trigger for Control Table: STRM_PARAM_CNTL  
 Database:       PDBIDM                                                
########################################################################*/
REPLACE TRIGGER PDBIDM.TI_STRM_PARAM_CNTL
AFTER INSERT ON PDBIDM.STRM_PARAM_CNTL
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDBIDM.STRM_PARAM_CNTL_LOG (
     STREAM_KEY
     ,STREAM_ID
     ,PARAM_DATE_NAME_1
     ,PARAM_DATE_VALU_1
     ,PARAM_DATE_NAME_2
     ,PARAM_DATE_VALU_2
     ,PARAM_DATE_NAME_3
     ,PARAM_DATE_VALU_3
     ,PARAM_DATE_NAME_4
     ,PARAM_DATE_VALU_4
     ,PARAM_DATE_NAME_5
     ,PARAM_DATE_VALU_5
     ,PARAM_DTTS_NAME_1
     ,PARAM_DTTS_VALU_1
     ,PARAM_DTTS_NAME_2
     ,PARAM_DTTS_VALU_2
     ,PARAM_DTTS_NAME_3
     ,PARAM_DTTS_VALU_3
     ,PARAM_DTTS_NAME_4
     ,PARAM_DTTS_VALU_4
     ,PARAM_DTTS_NAME_5
     ,PARAM_DTTS_VALU_5
     ,PARAM_VARC_NAME_1
     ,PARAM_VARC_VALU_1
     ,PARAM_VARC_NAME_2
     ,PARAM_VARC_VALU_2
     ,PARAM_VARC_NAME_3
     ,PARAM_VARC_VALU_3
     ,PARAM_VARC_NAME_4
     ,PARAM_VARC_VALU_4
     ,PARAM_VARC_NAME_5
     ,PARAM_VARC_VALU_5
     ,PARAM_ITGR_NAME_1
     ,PARAM_ITGR_VALU_1
     ,PARAM_ITGR_NAME_2
     ,PARAM_ITGR_VALU_2
     ,PARAM_ITGR_NAME_3
     ,PARAM_ITGR_VALU_3
     ,PARAM_ITGR_NAME_4
     ,PARAM_ITGR_VALU_4
     ,PARAM_ITGR_NAME_5
     ,PARAM_ITGR_VALU_5
     ,PARAM_DCML_NAME_1
     ,PARAM_DCML_VALU_1
     ,PARAM_DCML_NAME_2
     ,PARAM_DCML_VALU_2
     ,PARAM_DCML_NAME_3
     ,PARAM_DCML_VALU_3
     ,PARAM_DCML_NAME_4
     ,PARAM_DCML_VALU_4
     ,PARAM_DCML_NAME_5
     ,PARAM_DCML_VALU_5
     ,LOG_DATE
     ,LOG_TIME
     ,LOG_DTTS
     ,LOG_USER
  ) 
 SELECT 
     n.STREAM_KEY
	 ,STRMID.STREAM_ID
     ,n.PARAM_DATE_NAME_1
     ,n.PARAM_DATE_VALU_1
     ,n.PARAM_DATE_NAME_2
     ,n.PARAM_DATE_VALU_2
     ,n.PARAM_DATE_NAME_3
     ,n.PARAM_DATE_VALU_3
     ,n.PARAM_DATE_NAME_4
     ,n.PARAM_DATE_VALU_4
     ,n.PARAM_DATE_NAME_5
     ,n.PARAM_DATE_VALU_5
     ,n.PARAM_DTTS_NAME_1
     ,n.PARAM_DTTS_VALU_1
     ,n.PARAM_DTTS_NAME_2
     ,n.PARAM_DTTS_VALU_2
     ,n.PARAM_DTTS_NAME_3
     ,n.PARAM_DTTS_VALU_3
     ,n.PARAM_DTTS_NAME_4
     ,n.PARAM_DTTS_VALU_4
     ,n.PARAM_DTTS_NAME_5
     ,n.PARAM_DTTS_VALU_5
     ,n.PARAM_VARC_NAME_1
     ,n.PARAM_VARC_VALU_1
     ,n.PARAM_VARC_NAME_2
     ,n.PARAM_VARC_VALU_2
     ,n.PARAM_VARC_NAME_3
     ,n.PARAM_VARC_VALU_3
     ,n.PARAM_VARC_NAME_4
     ,n.PARAM_VARC_VALU_4
     ,n.PARAM_VARC_NAME_5
     ,n.PARAM_VARC_VALU_5
     ,n.PARAM_ITGR_NAME_1
     ,n.PARAM_ITGR_VALU_1
     ,n.PARAM_ITGR_NAME_2
     ,n.PARAM_ITGR_VALU_2
     ,n.PARAM_ITGR_NAME_3
     ,n.PARAM_ITGR_VALU_3
     ,n.PARAM_ITGR_NAME_4
     ,n.PARAM_ITGR_VALU_4
     ,n.PARAM_ITGR_NAME_5
     ,n.PARAM_ITGR_VALU_5
     ,n.PARAM_DCML_NAME_1
     ,n.PARAM_DCML_VALU_1
     ,n.PARAM_DCML_NAME_2
     ,n.PARAM_DCML_VALU_2
     ,n.PARAM_DCML_NAME_3
     ,n.PARAM_DCML_VALU_3
     ,n.PARAM_DCML_NAME_4
     ,n.PARAM_DCML_VALU_4
     ,n.PARAM_DCML_NAME_5
     ,n.PARAM_DCML_VALU_5
	 ,DATE
	 ,TIME
	 ,CURRENT_TIMESTAMP(0)
	 ,USER
FROM PVGENTCF.CTLFW_Stream_Id STRMID
WHERE STRMID.STREAM_KEY = n.STREAM_KEY
;
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Cd_AUT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Cd_AUT
  AFTER UPDATE ON PDGDWPIIOBF.Dtf_Job_Cd
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Cd_Hst
  (
     Job_Cd
    ,Tgt_Databasename
 ,Databasename
    ,ObjectName
    ,ObjectType
    ,CopyStat
 ,CopyData
    ,CompareDDL
 ,TruncateTgtTable
    ,WhereClause
    ,KeyColumn
    ,Query_Band
    ,Job_Dscrp
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Tgt_Databasename
 ,n.Databasename
    ,n.ObjectName
    ,n.ObjectType
    ,n.CopyStat
 ,n.CopyData
    ,n.CompareDDL
 ,n.TruncateTgtTable
    ,n.WhereClause
    ,n.KeyColumn
    ,n.Query_Band
    ,n.Job_Dscrp
    ,'U'
   );

--
/* <sc-trigger> PDGEN_REF.BMAP_TI_Valid_Language </sc-trigger> */
REPLACE TRIGGER PDGEN_REF.BMAP_TI_Valid_Language
AFTER INSERT ON PDGEN_REF.BMAP_T_Valid_Language
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGEN_REF.BMAP_T_Valid_Language_Log (
      Language_Id
    , Language_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Language_Id
    , n.Language_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_S00_D_TCF_001_LKP_0.BMAP_TU_Valid_Language </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_LKP_0.BMAP_TU_Valid_Language
AFTER UPDATE ON D_S00_D_TCF_001_LKP_0.BMAP_T_Valid_Language
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_LKP_0.BMAP_T_Valid_Language_Log (
      Language_Id
    , Language_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Language_Id
    , n.Language_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_OPS_001_STD_0.TD_Job_Param </sc-trigger> */
REPLACE TRIGGER P_D_OPS_001_STD_0.TD_Job_Param
AFTER DELETE ON P_D_OPS_001_STD_0.Job_Param
REFERENCING OLD ROW AS O  
FOR EACH ROW  (
INSERT INTO P_D_OPS_001_STD_0.Job_Param_Log (
   Param_File_Name
 , Param_Group
 , Param_Name
 , Param_Value
 , Param_Desc
  ,Param_Std_Code
 , CR_No
 , Update_Action_Code
 , Update_CR_No
 , Update_Session_ID
 , Update_Date
 , Update_User
 , Update_Time
 , Update_Ts
  ) VALUES (
   o.Param_File_Name
 , o.Param_Group
 , o.Param_Name
 , o.Param_Value
 , o.Param_Desc
  ,o.Param_Std_Code
 , o.CR_No
 , 'D'
 , NULL
 , SESSION
 , DATE
 , USER
 , TIME
 , CURRENT_TIMESTAMP(6)
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL_TRIG_D </sc-trigger> */




REPLACE TRIGGER U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL_TRIG_D
AFTER DELETE ON  U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL_AUDT
(
BSB                           
,ACCT_I                        
,AMT                           
,CUST_I                        
,MNTR_ACCT_N                   
,STMT_X                        
,SRCE                          
,BENE_IMPCT_D                  
,NOTE                          
,BENE_N                        
,STUS                          
,STUS_N                        
,STUS_X                        
,STUS_UPDT_DT                  
,IMPT_FILE_NAME                
,EXPT_FILE_NAME                
,FINL_ALT_BSB_1                
,FINL_ALT_ACCT_I_1             
,FINL_ALT_BSB_2                
,FINL_ALT_ACCT_I_2             
,FINL_ALT_BSB_3                
,FINL_ALT_ACCT_I_3             
,FINL_ALT_ACCT_UPDT_DT         
,UPDT_D                        
,RECD_I                                          
,RECON_TRAN_I
,EVNT_I
,RECON_TRAN_D
,RECON_ACCT_I
,AMT_TYPE                      
,REFN_I                        
,REFN_INTL                     
,REFN_EXTL    
,MODF_BY     
,MODF_DTTS                     
,OPER_TYPE                                            

)
VALUES
(
 n.BSB                           
,n.ACCT_I                        
,n.AMT                           
,n.CUST_I                        
,n.MNTR_ACCT_N                   
,n.STMT_X                        
,n.SRCE                          
,n.BENE_IMPCT_D                  
,n.NOTE                          
,n.BENE_N                        
,n.STUS                          
,n.STUS_N                        
,n.STUS_X                        
,n.STUS_UPDT_DT                  
,n.IMPT_FILE_NAME                
,n.EXPT_FILE_NAME                
,n.FINL_ALT_BSB_1                
,n.FINL_ALT_ACCT_I_1             
,n.FINL_ALT_BSB_2                
,n.FINL_ALT_ACCT_I_2             
,n.FINL_ALT_BSB_3                
,n.FINL_ALT_ACCT_I_3             
,n.FINL_ALT_ACCT_UPDT_DT         
,n.UPDT_D                        
,n.RECD_I    
,n.RECON_TRAN_I
,n.EVNT_I
,n.RECON_TRAN_D
,n.RECON_ACCT_I
,n.AMT_TYPE                      
,n.REFN_I                        
,n.REFN_INTL                     
,n.REFN_EXTL 
,USER
,Current_Timestamp(6)
,'DELETE'
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_Hist
  (
     DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DB_Name
    ,n.Obj_Name
    ,n.Col_1_Name
    ,n.Col_2_Name
    ,n.Join_Col_1_Name
    ,n.Join_Col_2_Name
    ,n.Predicate_Clause_Text
    ,n.Join_Type
    ,n.Rltd_DB_Name
    ,n.Rltd_Obj_Name
    ,n.Rltd_Col_1_Name
    ,n.Rltd_Col_2_Name
    ,n.Rltd_Join_Col_1_Name
    ,n.Rltd_Join_Col_2_Name
    ,n.Rltd_Predicate_Clause_Text
    ,n.Is_Enabled
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Process_Id </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Process_Id
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Id_Log (
      Process_Name
    , Process_Id
    , Process_State
    , Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Process_Id
    , n.Process_State
    , n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> UDGDWOPS.TD_Sandpit_Archive_Status </sc-trigger> */
REPLACE TRIGGER UDGDWOPS.TD_Sandpit_Archive_Status
AFTER DELETE ON UDGDWOPS.Sandpit_Archive_Status
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO UDGDWOPS.Sandpit_Archive_Status_Log (
    Batch_Id
    ,Batch_Run_Date
    ,Batch_State
    ,Start_Time
    ,End_Time
    ,Session_Id
    ,Last_Update_By
    ,Last_Update_At
    ,Log_User
    ,Log_Ts
  ) VALUES (
    o.Batch_Id
    ,o.Batch_Run_Date
    ,o.Batch_State
    ,o.Start_Time
    ,o.End_Time
    ,o.Session_Id
    ,o.Last_Update_By
    ,o.Last_Update_At
    ,USER
    ,CURRENT_TIMESTAMP(6)
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */




-- Bankwest Tigger Delete
--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_BWA_DR_TRIG_D
REPLACE TRIGGER U_D_DSV_001_NFT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.NOST_BWA_DR_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS o
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_BWA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'DELETE'  
,o.unique_identifier             
,o.match_type                    
,o.Nostro_acct                   
,o.CURR_NOSTRO                   
,o.Group_area                    
,o.Acct_Name                     
,o.external_party_bic            
,o.value_date                    
,o.AMOUNT                        
,o.Message_Type                  
,o.message_type_desc             
,o.tag_61_stmt_line              
,o.internal_ref                  
,o.external_ref                  
,o.description                   
,o.department                    
,o.Investigation_Flag            
,o.WS_flag                       
,o.deal_number                   
,o.cust_number                   
,o.broker_name                   
,o.product_cat                   
,o.Department_Responsible        
,o.Reason                        
,o.Other_Comment                 
,o.Escalate                      
,o.IFTI_Reportable               
,o.Payor_Bank                    
,o.Payor_Details                 
,o.Payee_Bank                    
,o.Payee_Details                 
,o.Investigation_Status          
,o.MatchID                       
,o.deal_num_list                 
,o.Beneficiary                   
,o.Remark                        
,o.Settle                        
,o.Category_Correct              
,o.Updated                       
,o.msg_id                        
,o.MSG_TAG_SEQ                   
,o.CBA_BIC                       
,o.currency_tag62                
,o.date_tag_created              
,o.amount_matched                
,o.Buckets_f                     
,o.Bucket                        
,o.WS_GDW_source                 
,o.cust_code                     
,o.cust_type                     
,o.ext_ref_nr                    
,o.portfolio_no                  
,o.Acc                           
,o.Additional_Field              
,o.Pay_Meth                      
,o.swift_50                      
,o.Remittance_info               
,o.Enough_Data_For_IFTI          
,o.swf_File_Ref                  
,o.compliance_exclusion
);

--
/* <sc-trigger> PDDQRM.TRG_DQRM_DATA_QUALITY_RULE_AD </sc-trigger> */
REPLACE TRIGGER PDDQRM.TRG_DQRM_DATA_QUALITY_RULE_AD 
AFTER DELETE ON PDDQRM.DQRM_DATA_QUALITY_RULE 
REFERENCING OLD ROW AS O FOR EACH ROW 
INSERT INTO PDDQRM.DQ_RULE_HIST                                                                                                                           
(RULE_IDNN
,RULE_M
,RULE_X
,RULE_N
,RULE_RVSD
,RULE_STRT_D
,RULE_END_D
,QUAL_TSHD
,QUAL_GOAL_STAT
,FNCL_IMPA_A
,FNCL_IMPA_UNIT
,BUSN_IMPA_NOTE
,RULE_RVSD_NOTE
,DEVT_COND_CLAU
,FROM_CLAU
,WHER_CLAU
,MTRC_SQL_LOCK_F
,RUN_PRE_F
,RUN_POST_F
,MTRC_SQL
,PRE_PROS_SQL
,GENR_PRE_PROS_SQL
,POST_PROS_SQL
,GENR_POST_PROS_SQL
,STOR_DETL_DEVT_F
,DELT_PREV_DEVT_F
,DEVT_SQL_LOCK_F
,HIST_DAY
,HIST_ROW
,DEVT_SQL
,DQ_RULE_PARN_IDNN
,APPT_ITEM_IDNN
,STUS_ITEM_IDNN
,FREQ_ITEM_IDNN
,TYPE_ITEM_IDNN
,SBTY_ITEM_IDNN
,DATA_CSTI_ITEM_IDNN
,WGHT_ITEM_IDNN
,VERS
,MODF_D
,MODF_BY
,STOR_DETL_CNT_F
,DELT_PREV_CNT_F
,CNT_SQL_LOCK_F
,CNT_HIST_DAY
,CNT_HIST_ROW
,CNT_SQL   
,ACTV_BY_I
,TRIG_S
,TRIG_ACTV_X  
,METRIC_USE_PROCESS_ID_FLAG
,DEVIATION_USE_PROCESS_ID_FLAG                                                                          
)
values 
(   
 o.RULE_ID  
,o.RULE_NAME
,o.RULE_DESC
,o.RULE_NUMBER
,o.RULE_REVISION
,o.RULE_START_DATE
,o.RULE_END_DATE
,o.QLTY_THRESHOLD
,o.QLTY_GOAL_STATE
,o.FIN_IMPACT_AMT
,o.FIN_IMPACT_UNIT
,o.BUS_IMPACT_NOTES
,o.RULE_REVISION_NOTES
,o.DEVIATION_COND_CLAUSE
,o.FROM_CLAUSE
,o.WHERE_CLAUSE
,o.METRIC_SQL_LOCK_FLAG
,o.RUN_PRE_FLAG
,o.RUN_POST_FLAG
,o.METRIC_SQL
,o.PRE_PROC_SQL
,o.GENERATED_PRE_PROC_SQL
,o.POST_PROC_SQL
,o.GENERATED_POST_PROC_SQL
,o.STORE_DETAIL_DEV_FLAG
,o.DELETE_PREV_DEV_FLAG
,o.DEV_SQL_LOCK_FLAG
,o.HISTORY_DAYS
,o.HISTORY_ROWS
,o.DEVIATION_SQL
,o.DQR_PARENT_ID
,o.APPLICATION_ITEM_ID
,o.STATUS_ITEM_ID
,o.FREQUENCY_ITEM_ID
,o.TYPE_ITEM_ID
,o.SUBTYPE_ITEM_ID
,o.DATA_STEWARD_ITEM_ID
,o.WEIGHTING_ITEM_ID
,o.VERSION
,o.MODIFIED_DATE
,o.MODIFIED_BY
,o.STORE_DETAIL_COUNTS_FLAG
,o.DELETE_PREV_COUNTS_FLAG
,o.COUNTS_SQL_LOCK_FLAG
,o.COUNTS_HISTORY_DAYS
,o.COUNTS_HISTORY_ROWS
,o.COUNTS_SQL    
,USER   
,CURRENT_TIMESTAMP(0)   
,'DELETE'  
,o.METRIC_USE_PROCESS_ID_FLAG
,o.DEVIATION_USE_PROCESS_ID_FLAG                                                                        
);

--
/* <sc-trigger> Which_System.Repl_Matrix_ADT </sc-trigger> */
REPLACE TRIGGER Which_System.Repl_Matrix_ADT
  AFTER DELETE ON Which_System.Repl_Matrix
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO Which_System.Repl_Matrix_Hist
  (
Platform_Name
,Description
,Platform_Role
,Platform_Stat
,Platform_Function
,Platform_Repl_Prfl
,TDS_Env
,Create_User
,Create_Date
,Create_Ts
,Update_User
,Update_Date
,Update_Ts
,Modify_Action

   ) VALUES (
o.Platform_Name
,o.Description
,o.Platform_Role
,o.Platform_Stat
,o.Platform_Function
,o.Platform_Repl_Prfl
,o.TDS_Env
,o.Create_User
,o.Create_Date
,o.Create_Ts
,o.Update_User
,o.Update_Date
,o.Update_Ts
,'D'
  );

--
/* <sc-trigger> PDTRMEXTD.GENID_COLLATERAL </sc-trigger> */
--------------------------------------------------------------------------------

REPLACE TRIGGER PDTRMEXTD.GENID_COLLATERAL
AFTER INSERT ON PDTRMCORE.EX_COLLATERAL
	REFERENCING NEW AS NEWROW 
	FOR EACH ROW
(

LOCKING TABLE PDTRMCORE.EX_COLLATERAL FOR WRITE
/* update sequence number */
UPDATE A 
FROM PDTRMEXTD.TRM_SEQ A
SET 	SEQ_NO = A.SEQ_NO + 1
WHERE	A.SEQ_TYPE_CD = 'COLLATERAL'
; 

/* set Script_Id */
UPDATE A
FROM PDTRMCORE.EX_COLLATERAL AS A
, (   SELECT 
        SEQ_NO
	FROM PDTRMEXTD.TRM_SEQ
	WHERE 
      SEQ_TYPE_CD = 'COLLATERAL'
  )   AS B  
SET 	Script_Id = B.SEQ_NO   
WHERE 	A.COLLATERAL_ID = NEWROW.COLLATERAL_ID;
   
);

--
/* <sc-trigger> PDSECURITY.TU_GDW_USER_ATTR </sc-trigger> */


REPLACE TRIGGER PDSECURITY.TU_GDW_USER_ATTR
AFTER UPDATE ON PDSECURITY.GDW_USER_ATTR
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO PDSECURITY.GDW_USER_ATTR_LOG (
      USERNAME
      ,USER_TYPE
      ,EMPL_I
      ,SAR_NO
      ,LAST_UPDATE_BY
      ,LAST_UPDATE_AT
      ,LOG_ACTION
      ,LOG_BY
      ,LOG_AT
  ) VALUES (
      O.USERNAME
      ,O.USER_TYPE
      ,O.EMPL_I
      ,O.SAR_NO
      ,O.LAST_UPDATE_BY
      ,O.LAST_UPDATE_AT
      ,'UPDATE'
      ,USER
      ,Current_Timestamp(6)
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_LKP_0.BKEY_TU_Key_Set </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER P_P00_D_TCF_001_LKP_0.BKEY_TU_Key_Set
AFTER UPDATE ON P_P00_D_TCF_001_LKP_0.BKEY_T_Key_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_LKP_0.BKEY_T_Key_Set_Log (
      Key_Set_Id
    , Description
    , Key_Table_Name
    , Key_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Description
    , n.Key_Table_Name
    , n.Key_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_P01_D_OPS_001_STD_0.TI_Job_Param </sc-trigger> */
REPLACE TRIGGER P_P01_D_OPS_001_STD_0.TI_Job_Param
AFTER INSERT ON P_P01_D_OPS_001_STD_0.Job_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P01_D_OPS_001_STD_0.Job_Param_Log (
   Param_File_Name
 , Param_Group
 , Param_Name
 , Param_Value
 , Param_Desc
 , Param_Std_Code
 , CR_No
 , Update_Action_Code
 , Update_CR_No
 , Update_Session_ID
 , Update_Date
 , Update_User
 , Update_Time
 , Update_Ts
  ) VALUES (
   n.Param_File_Name
 , n.Param_Group
 , n.Param_Name
 , n.Param_Value
 , n.Param_Desc
  ,n.Param_Std_Code
 , n.CR_No
 , 'I'
 , n.CR_No
 , SESSION
 , DATE
 , USER
 , TIME
 , CURRENT_TIMESTAMP(6)
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Process_Param </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Process_Param
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Param_Log (
      Process_Name
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */



--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_BWA_CR_TRIG_U
REPLACE TRIGGER U_D_DSV_001_NIT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.VOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated
)
VALUES
(
 'VOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'UPDATE'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> P_D_OAC_001_STD_0.OAC_TDPID_Map_AUT </sc-trigger> */
REPLACE TRIGGER P_D_OAC_001_STD_0.OAC_TDPID_Map_AUT
  AFTER UPDATE ON P_D_OAC_001_STD_0.OAC_TDPID_Map
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_OAC_001_STD_0.OAC_TDPID_Map_Hist
  (
  TDPID_TDS
 ,TDPID_BAR
 ,First_Type_Cd
 ,First_Id
 ,Create_User
 ,Create_Date
 ,Create_Ts
 ,Update_User
 ,Update_Date
 ,Update_Ts
 ,Modify_Action
   ) VALUES (
  n.TDPID_TDS
 ,n.TDPID_BAR
 ,n.First_Type_Cd
 ,n.First_Id
 ,n.Create_User
 ,n.Create_Date
 ,n.Create_Ts
 ,CURRENT_USER
 ,CURRENT_DATE
 ,CURRENT_TIMESTAMP(6)
 ,'U'
   );

--
/* <sc-trigger> B_D_OPS_001_STD_0.R_DIGT_SCHE_FRWK_CHEK_IN </sc-trigger> */
REPLACE TRIGGER B_D_OPS_001_STD_0.R_DIGT_SCHE_FRWK_CHEK_IN
AFTER UPDATE  ON  B_D_OPS_001_STD_0.R_DIGT_SCHE_FRWK_PROS
REFERENCING  OLD_TABLE  as o
 NEW_TABLE  as  n 
FOR EACH STATEMENT
when (0=(select count(o.TARG_TABL_M) from o, n where o.TARG_TABL_M=n.TARG_TABL_M AND o.TARG_DB_M=n.TARG_DB_M))
(
ABORT 'CHECK IN failed as the table provided does not exist in framework PROS table';
);

--
/* <sc-trigger> PDEVNT_H.EVNT_TRG_DEL </sc-trigger> */
CREATE TRIGGER PDEVNT_H.EVNT_TRG_DEL
 AFTER DELETE ON PDEVNT_H.EVNT_ETL_DATE 
 FOR EACH STATEMENT
 INSERT INTO DBCMNGR.AlertRequest 
 VALUES (DATE, CAST(SUBSTR(CAST(CURRENT_TIME AS CHAR(5)),1,2)||SUBSTR(CAST(CURRENT_TIME AS CHAR(5)),
 4,2)AS INTEGER),'PDEVNT_H','PDEVNT_H.EVNT_ETL_DATE - Delete detected',
 NULL,'E',0,'ESITDSITSMOBusinessIntelligenceOperations.BIGDW@cba.com.au','0A'XC ||'0A'XC ||'Hi Team, '||'0A'XC ||'0A'XC ||'PDEVNT_H.EVNT_ETL_DATE has been changed '|| '0A'XC ||'Please verfiy if PDEVNT_H archival process is running and sync up the relevant PDEVNT_H tables in PROD4. '|| '0A'XC || '0A'XC || '*** PLEASE DO NOT REPLY TO THIS EMAIL ***');

--
/* <sc-trigger> UDGDWOPSWRK.TU_Sandpit_Archive_Param </sc-trigger> */
REPLACE TRIGGER UDGDWOPSWRK.TU_Sandpit_Archive_Param
AFTER UPDATE ON UDGDWOPSWRK.Sandpit_Archive_Param
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO UDGDWOPSWRK.Sandpit_Archive_Param_Log (
    Param_Group
    ,Param_Name
    ,Param_Value
    ,Param_Desc
    ,Efft_Date
    ,Expy_Date
    ,Last_Update_By
    ,Last_Update_At
    ,Log_User
    ,Log_Ts
  ) VALUES (
    o.Param_Group
    ,o.Param_Name
    ,o.Param_Value
    ,o.Param_Desc
    ,o.Efft_Date
    ,o.Expy_Date
    ,o.Last_Update_By
    ,o.Last_Update_At
    ,USER
    ,CURRENT_TIMESTAMP(6)
  );
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Process_Type_Param </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Process_Type_Param
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param_Log (
      Process_Type
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> D_D00_D_TCF_001_LKP_0.BMAP_TI_Code_Set </sc-trigger> */


REPLACE TRIGGER D_D00_D_TCF_001_LKP_0.BMAP_TI_Code_Set
AFTER INSERT ON D_D00_D_TCF_001_LKP_0.BMAP_T_Code_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_LKP_0.BMAP_T_Code_Set_Log (
      Code_Set_Id
    , Description
    , Map_Table_Name
    , Map_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts

  ) VALUES (
      n.Code_Set_Id
    , n.Description
    , n.Map_Table_Name
    , n.Map_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_Hist
  (
     Cand_Obj_Stus_Type_Cd
    ,Cand_Obj_Stus_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Stus_Type_Cd
    ,n.Cand_Obj_Stus_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_REML_UPDT_PARN_LIST_HIST_UPD </sc-trigger> */


/*CREATE UPD TRIGGER FOR  REML_UPDT_PARN_LIST_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_REML_UPDT_PARN_LIST_HIST_UPD
AFTER UPDATE ON U_D_DSV_001_REM_1.REML_UPDT_PARN_LIST_HIST
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.REML_UPDT_PARN_LIST_HIST_LOG  SELECT A.*,USER,'UPD', CURRENT_TIMESTAMP  FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_System_File </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_System_File
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_System_File
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_System_File_Log (
      Ctl_Id
    , File_Id
    , Data_File_Name
    , Ctl_File_Name
    , Queue_Name
    , Description
    , Data_File_Suffix
    , Ctl_File_Suffix
    , Data_File_Type
    , ET_Tolerance
    , UV_Tolerance
    , Sessions
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Queue_Name
    , n.Description
    , n.Data_File_Suffix
    , n.Ctl_File_Suffix
    , n.Data_File_Type
    , n.ET_Tolerance
    , n.UV_Tolerance
    , n.Sessions
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Process </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Process
AFTER INSERT ON PDGENTCF.CTLFW_T_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Process_Log (
      Process_Name
    , Description
    , Process_Type
    , Ctl_Id
    , Stream_Key
    , In_DB_Name
    , In_Object_Name
    , Out_DB_Name
    , Out_Object_Name
    , Target_TableDatabaseName
    , Target_TableName
    , Temp_DatabaseName
    , Key_Set_Id
    , Domain_Id
    , Code_Set_Id
    , Collect_Stats
    , Truncate_Target
    , Verification_Flag
    , File_Qualifier_Reset_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Description
    , n.Process_Type
    , n.Ctl_Id
    , n.Stream_Key
    , n.In_DB_Name
    , n.In_Object_Name
    , n.Out_DB_Name
    , n.Out_Object_Name
    , n.Target_TableDatabaseName
    , n.Target_TableName
    , n.Temp_DatabaseName
    , n.Key_Set_Id
    , n.Domain_Id
    , n.Code_Set_Id
    , n.Collect_Stats
    , n.Truncate_Target
    , n.Verification_Flag
    , n.File_Qualifier_Reset_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr_ADT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr_ADT
  AFTER DELETE ON PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr_Hst
  (
     RfshGroupNm
 ,TgtDatabaseNm
 ,SrcDatabaseNm
 ,SrcTableNm
 ,SrcTableCurrPerm
 ,SrcTablePredicateInfo
 ,SrcKeyColumn
 ,SelectionCriteriaRuleNbr
 ,SuggestedUtility
 ,TruncateTgtTable
 ,RfshGroupEntryTimestamp
 ,RfshGroupEntryStatus
 ,Modify_Action
   ) VALUES (
     o.RfshGroupNm
 ,o.TgtDatabaseNm
 ,o.SrcDatabaseNm
 ,o.SrcTableNm
 ,o.SrcTableCurrPerm
 ,o.SrcTablePredicateInfo
 ,o.SrcKeyColumn
 ,o.SelectionCriteriaRuleNbr
 ,o.SuggestedUtility
 ,o.TruncateTgtTable
 ,o.RfshGroupEntryTimestamp
 ,o.RfshGroupEntryStatus
    ,'D'
   );

--
/* <sc-trigger> PDGEN_REF.BMAP_TI_Code_Set </sc-trigger> */
REPLACE TRIGGER PDGEN_REF.BMAP_TI_Code_Set
AFTER INSERT ON PDGEN_REF.BMAP_T_Code_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGEN_REF.BMAP_T_Code_Set_Log (
      Code_Set_Id
    , Description
    , Map_Table_Name
    , Map_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts

  ) VALUES (
      n.Code_Set_Id
    , n.Description
    , n.Map_Table_Name
    , n.Map_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_S00_D_TCF_001_LKP_0.BMAP_TU_Code_Set </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_LKP_0.BMAP_TU_Code_Set
AFTER UPDATE ON D_S00_D_TCF_001_LKP_0.BMAP_T_Code_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_LKP_0.BMAP_T_Code_Set_Log (
      Code_Set_Id
    , Description
    , Map_Table_Name
    , Map_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Description
    , n.Map_Table_Name
    , n.Map_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_OPS_001_STD_0.TU_Job_Param </sc-trigger> */
REPLACE TRIGGER P_D_OPS_001_STD_0.TU_Job_Param
AFTER UPDATE ON P_D_OPS_001_STD_0.Job_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_D_OPS_001_STD_0.Job_Param_Log (
   Param_File_Name
 , Param_Group
 , Param_Name
 , Param_Value
 , Param_Desc
  ,Param_Std_Code
 , CR_No
 , Update_Action_Code
 , Update_CR_No
 , Update_Session_ID
 , Update_Date
 , Update_User
 , Update_Time
 , Update_Ts
  ) VALUES (
   n.Param_File_Name
 , n.Param_Group
 , n.Param_Name
 , n.Param_Value
 , n.Param_Desc
  ,n.Param_Std_Code
 , n.CR_No
 , 'U'
 , n.CR_No
 , SESSION
 , DATE
 , USER
 , TIME
 , CURRENT_TIMESTAMP(6)
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL_TRIG_U </sc-trigger> */


REPLACE TRIGGER U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL_TRIG_U
AFTER UPDATE ON  U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_ICP_1.CUST_MANL_FFILL_FINL_AUDT
(
BSB                           
,ACCT_I                        
,AMT                           
,CUST_I                        
,MNTR_ACCT_N                   
,STMT_X                        
,SRCE                          
,BENE_IMPCT_D                  
,NOTE                          
,BENE_N                        
,STUS                          
,STUS_N                        
,STUS_X                        
,STUS_UPDT_DT                  
,IMPT_FILE_NAME                
,EXPT_FILE_NAME                
,FINL_ALT_BSB_1                
,FINL_ALT_ACCT_I_1             
,FINL_ALT_BSB_2                
,FINL_ALT_ACCT_I_2             
,FINL_ALT_BSB_3                
,FINL_ALT_ACCT_I_3             
,FINL_ALT_ACCT_UPDT_DT         
,UPDT_D                        
,RECD_I                                          
,RECON_TRAN_I
,EVNT_I
,RECON_TRAN_D
,RECON_ACCT_I
,AMT_TYPE                      
,REFN_I                        
,REFN_INTL                     
,REFN_EXTL    
,MODF_BY     
,MODF_DTTS                     
,OPER_TYPE                                            

)
VALUES
(
 n.BSB                           
,n.ACCT_I                        
,n.AMT                           
,n.CUST_I                        
,n.MNTR_ACCT_N                   
,n.STMT_X                        
,n.SRCE                          
,n.BENE_IMPCT_D                  
,n.NOTE                          
,n.BENE_N                        
,n.STUS                          
,n.STUS_N                        
,n.STUS_X                        
,n.STUS_UPDT_DT                  
,n.IMPT_FILE_NAME                
,n.EXPT_FILE_NAME                
,n.FINL_ALT_BSB_1                
,n.FINL_ALT_ACCT_I_1             
,n.FINL_ALT_BSB_2                
,n.FINL_ALT_ACCT_I_2             
,n.FINL_ALT_BSB_3                
,n.FINL_ALT_ACCT_I_3             
,n.FINL_ALT_ACCT_UPDT_DT         
,n.UPDT_D                        
,n.RECD_I    
,n.RECON_TRAN_I
,n.EVNT_I
,n.RECON_TRAN_D
,n.RECON_ACCT_I
,n.AMT_TYPE                      
,n.REFN_I                        
,n.REFN_INTL                     
,n.REFN_EXTL 
,USER
,Current_Timestamp(6)
,'UPDATE'
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_Hist
  (
     Stream_Name
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Name
    ,n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Process_Type </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Process_Type
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Type
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Log (
      Process_Type
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> UDGDWOPS.TU_Sandpit_Archive_Status </sc-trigger> */
REPLACE TRIGGER UDGDWOPS.TU_Sandpit_Archive_Status
AFTER UPDATE ON UDGDWOPS.Sandpit_Archive_Status
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO UDGDWOPS.Sandpit_Archive_Status_Log (
    Batch_Id
    ,Batch_Run_Date
    ,Batch_State
    ,Start_Time
    ,End_Time
    ,Session_Id
    ,Last_Update_By
    ,Last_Update_At
    ,Log_User
    ,Log_Ts
  ) VALUES (
    o.Batch_Id
    ,o.Batch_Run_Date
    ,o.Batch_State
    ,o.Start_Time
    ,o.End_Time
    ,o.Session_Id
    ,o.Last_Update_By
    ,o.Last_Update_At
    ,USER
    ,CURRENT_TIMESTAMP(6)
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */
-- Bankwest  Credit Update Trigger
--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_BWA_CR_TRIG_U
replace trigger U_D_DSV_001_NFT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_U
after update on U_D_DSV_001_NFT_1.NOST_BWA_CR_EXC_RPRT_ACCESS
referencing new row as n
for each row
insert into U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
values
(
 'NOST_BWA_CR_EXC_RPRT_ACCESS'
,user
,current_timestamp(6)
,'UPDATE'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> Which_System.Repl_Matrix_AIT </sc-trigger> */
REPLACE TRIGGER Which_System.Repl_Matrix_AIT
  AFTER INSERT ON Which_System.Repl_Matrix
  REFERENCING NEW ROW AS o FOR EACH ROW
  INSERT INTO Which_System.Repl_Matrix_Hist
  (
     Platform_Name
,Description
,Platform_Role
,Platform_Stat
,Platform_Function
,Platform_Repl_Prfl
,TDS_Env
,Create_User
,Create_Date
,Create_Ts
,Update_User
,Update_Date
,Update_Ts
,Modify_Action

   ) VALUES (
o.Platform_Name
,o.Description
,o.Platform_Role
,o.Platform_Stat
,o.Platform_Function
,o.Platform_Repl_Prfl
,o.TDS_Env
,o.Create_User
,o.Create_Date
,o.Create_Ts
,o.Update_User
,o.Update_Date
,o.Update_Ts
,'I'
   );

--
/* <sc-trigger> PDTRMEXTD.GENID_RESPONSE </sc-trigger> */
--------------------------------------------------------------------------------

REPLACE TRIGGER PDTRMEXTD.GENID_RESPONSE
AFTER INSERT ON PDTRMCORE.EX_RESPONSE
	REFERENCING NEW AS NEWROW 
	FOR EACH ROW
(

LOCKING TABLE PDTRMCORE.EX_RESPONSE FOR WRITE
/* update sequence number */
UPDATE A 
FROM PDTRMEXTD.TRM_SEQ A
SET 	SEQ_NO = A.SEQ_NO + 1
WHERE	A.SEQ_TYPE_CD = 'RESPONSE'
; 

/* set CM3_Response_Id */
UPDATE A
FROM PDTRMCORE.EX_RESPONSE AS A
, (   SELECT 
        SEQ_NO
	FROM PDTRMEXTD.TRM_SEQ
	WHERE 
      SEQ_TYPE_CD = 'RESPONSE'
  )   AS B  
SET 	CM3_Response_Id = B.SEQ_NO   
WHERE 	A.RESPONSE_ID = NEWROW.RESPONSE_ID;
   
);

--
/* <sc-trigger> PDSECURITY.TD_GDW_USER_TYPE </sc-trigger> */


REPLACE TRIGGER PDSECURITY.TD_GDW_USER_TYPE
AFTER DELETE ON PDSECURITY.GDW_USER_TYPE
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO PDSECURITY.GDW_USER_TYPE_LOG (
      USER_TYPE
      ,USER_TYPE_DESC
      ,USER_TYPE_CLASS
      ,LAST_UPDATE_BY
      ,LAST_UPDATE_AT
      ,LOG_ACTION
      ,LOG_BY
      ,LOG_AT
  ) VALUES (
      O.USER_TYPE
      ,O.USER_TYPE_DESC
      ,O.USER_TYPE_CLASS
      ,O.LAST_UPDATE_BY
      ,O.LAST_UPDATE_AT
      ,'DELETE'
      ,USER
      ,Current_Timestamp(6)
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_LKP_0.BMAP_TU_Valid_Language </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER P_P00_D_TCF_001_LKP_0.BMAP_TU_Valid_Language
AFTER UPDATE ON P_P00_D_TCF_001_LKP_0.BMAP_T_Valid_Language
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_LKP_0.BMAP_T_Valid_Language_Log (
      Language_Id
    , Language_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Language_Id
    , n.Language_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Process_Type </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Process_Type
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Type
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Log (
      Process_Type
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.MT202_EXC_RPRT_AUDT
(
REPORT                        
,SWIFT_MESSAGE_TEXT            
,UNIQUE_IDENTIFIER             
,MSG_ID                        
,MSG_SEQ                       
,MSG_TYPE                      
,DB_DATE                       
,SOURCE                        
,BRAND                         
,CBA_BIC                       
,EXTERNAL_PARTY_BIC            
,INP_OUT_IDENTIFIER            
,AMOUNT                        
,CURRENCY                      
,VAL_DATE                      
,REMITTER                      
,BENEFICIARY                   
,FIELD_20                      
,FIELD_21                      
,CREATED_DATE                  
,CDT_ID                        
,CDT_NAME                      
,DBT_ID                        
,DBT_NAME                      
,TAG_52                        
,TAG_58                        
,TAG_72                        
,SWF_FILE_REF                  
,TRN_PROC_PMS                  
,BUSN_DAYS                     
,IS_REVIEWED                   
,TEAM_RESP                     
,INVESTIGATION_STUS            
,TRAN_CATG                     
,PRODUCT                       
,OTHR_CMMT                     
,VERF_BY_IFTI_MNGR             
,IFTI_RPRT                     
,SUBM_FILE_NAME                
,SUBM_DATE                     
,SUBM_RCPT_NO                  
,INSERTION_DATE                
,EXC_OFCR                      
,EXC_OFCR_DTTM                 
,VERF_OFCR_CMMT                
,VERF_OFCR                     
,VERF_OFCR_DTTM                
,QA_OFCR_CMMT                  
,QA_OFCR                       
,QA_OFCR_DTTM                  
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(
n.REPORT                        
,n.SWIFT_MESSAGE_TEXT            
,n.UNIQUE_IDENTIFIER             
,n.MSG_ID                        
,n.MSG_SEQ                       
,n.MSG_TYPE                      
,n.DB_DATE                       
,n.SOURCE                        
,n.BRAND                         
,n.CBA_BIC                       
,n.EXTERNAL_PARTY_BIC            
,n.INP_OUT_IDENTIFIER            
,n.AMOUNT                        
,n.CURRENCY                      
,n.VAL_DATE                      
,n.REMITTER                      
,n.BENEFICIARY                   
,n.FIELD_20                      
,n.FIELD_21                      
,n.CREATED_DATE                  
,n.CDT_ID                        
,n.CDT_NAME                      
,n.DBT_ID                        
,n.DBT_NAME                      
,n.TAG_52                        
,n.TAG_58                        
,n.TAG_72                        
,n.SWF_FILE_REF                  
,n.TRN_PROC_PMS                  
,n.BUSN_DAYS                     
,n.IS_REVIEWED                   
,n.TEAM_RESP                     
,n.INVESTIGATION_STUS            
,n.TRAN_CATG                     
,n.PRODUCT                       
,n.OTHR_CMMT                     
,n.VERF_BY_IFTI_MNGR             
,n.IFTI_RPRT                     
,n.SUBM_FILE_NAME                
,n.SUBM_DATE                     
,n.SUBM_RCPT_NO                  
,n.INSERTION_DATE                
,n.EXC_OFCR                      
,n.EXC_OFCR_DTTM                 
,n.VERF_OFCR_CMMT                
,n.VERF_OFCR                     
,n.VERF_OFCR_DTTM                
,n.QA_OFCR_CMMT                  
,n.QA_OFCR                       
,n.QA_OFCR_DTTM                  
,USER
,Current_Timestamp(6)
,'INSERT' 
);

--
/* <sc-trigger> P_D_OAC_001_STD_0.OAC_Alrt_Rqst_AIT </sc-trigger> */
REPLACE TRIGGER P_D_OAC_001_STD_0.OAC_Alrt_Rqst_AIT
AFTER INSERT ON P_D_OAC_001_STD_0.OAC_Alrt_Rqst
REFERENCING NEW ROW AS n
FOR EACH ROW  (
 INSERT INTO DBCMNGR.AlertRequest (
   ReqDate
  ,ReqTime
  ,JobName
  ,Description
  ,EventValue
  ,ActionCode
  ,RepeatPeriod
  ,Destination
  ,Message
 ) VALUES (
   n.ReqDate
  ,n.ReqTime
  ,n.JobName
  ,n.Description
  ,n.EventValue
  ,n.ActionCode
  ,n.RepeatPeriod
  ,n.Destination
  ,TRIM(n.Destination) || ': ' || TRIM(n.Message)
 );
);

--
/* <sc-trigger> UDGDWOPSWRK.TD_Sandpit_Archive_Status </sc-trigger> */
REPLACE TRIGGER UDGDWOPSWRK.TD_Sandpit_Archive_Status
AFTER DELETE ON UDGDWOPSWRK.Sandpit_Archive_Status
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO UDGDWOPSWRK.Sandpit_Archive_Status_Log (
    Batch_Id
    ,Batch_Run_Date
    ,Batch_State
    ,Start_Time
    ,End_Time
    ,Session_Id
    ,Last_Update_By
    ,Last_Update_At
    ,Log_User
    ,Log_Ts
  ) VALUES (
    o.Batch_Id
    ,o.Batch_Run_Date
    ,o.Batch_State
    ,o.Start_Time
    ,o.End_Time
    ,o.Session_Id
    ,o.Last_Update_By
    ,o.Last_Update_At
    ,USER
    ,CURRENT_TIMESTAMP(6)
  );
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Transform_SurrKeyCol </sc-trigger> */
--------------------------------------------------------------------------------


/*******************************************************************************
* CREATE CTLFW_TU_Transform_SurrKeyCol UPDATE TRIGGER
*******************************************************************************/
REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Transform_SurrKeyCol
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Transform_SurrKeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Transform_SKCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,SurrKey_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.SurrKey_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> D_D00_D_TCF_001_LKP_0.BMAP_TI_Valid_Language </sc-trigger> */


REPLACE TRIGGER D_D00_D_TCF_001_LKP_0.BMAP_TI_Valid_Language
AFTER INSERT ON D_D00_D_TCF_001_LKP_0.BMAP_T_Valid_Language
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_LKP_0.BMAP_T_Valid_Language_Log (
      Language_Id
    , Language_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Language_Id
    , n.Language_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Rule_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Rule_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Obj_Rule
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Rule_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Enabled_Flag
    ,Repl_Type_Cd
    ,Predicate_Clause_Text
    ,Force_Flag
    ,Key_Column
    ,Obj_Size_Type_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Enabled_Flag
    ,n.Repl_Type_Cd
    ,n.Predicate_Clause_Text
    ,n.Force_Flag
    ,n.Key_Column
    ,n.Obj_Size_Type_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_TTN_REML_PAYT_DETL_HIST_DEL </sc-trigger> */

/*CREATE DEL TRIGGER FOR  TTN_REML_PAYT_DETL_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_TTN_REML_PAYT_DETL_HIST_DEL 
AFTER DELETE ON U_D_DSV_001_REM_1.TTN_REML_PAYT_DETL_HIST 
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.TTN_REML_PAYT_DETL_HIST_LOG  SELECT A.*,USER,'DEL', CURRENT_TIMESTAMP FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Transform_KeyCol </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Transform_KeyCol
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,Key_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.Key_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_System </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_System
AFTER INSERT ON PDGENTCF.CTLFW_T_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_System_Log (
      Ctl_Id
    , System_Name
    , Path_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.System_Name
    , n.Path_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.TI_MaskingMetadataRequest </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.TI_MaskingMetadataRequest
AFTER INSERT ON PDGDWPIIOBF.Masking_Metadata_Request
REFERENCING NEW TABLE AS n
FOR EACH STATEMENT  (
INSERT INTO PDGDWPIIOBF.Masking_Metadata_Request_Log 
    (
        DatabaseName
        ,TableName
        ,ColumnName
        ,ColumnExpression
        ,MaskingExpression
        ,Comments
        ,LoggingType
        ,LoggingUser
        ,LoggingDate
        ,LoggingTimestamp
    )
SELECT
        n.DatabaseName
        ,n.TableName
        ,n.ColumnName
        ,n.ColumnExpression
        ,n.MaskingExpression        
        ,n.Comments
        ,'INSERT'
        ,CURRENT_USER
        ,CURRENT_DATE
        ,CURRENT_TIMESTAMP(6)
        
FROM n
;
);

--
/* <sc-trigger> PDGEN_REF.BMAP_TU_Valid_Language </sc-trigger> */
REPLACE TRIGGER PDGEN_REF.BMAP_TU_Valid_Language
AFTER UPDATE ON PDGEN_REF.BMAP_T_Valid_Language
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGEN_REF.BMAP_T_Valid_Language_Log (
      Language_Id
    , Language_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Language_Id
    , n.Language_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_S00_D_TCF_001_LKP_0.BMAP_TI_Code_Set </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_LKP_0.BMAP_TI_Code_Set
AFTER INSERT ON D_S00_D_TCF_001_LKP_0.BMAP_T_Code_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_LKP_0.BMAP_T_Code_Set_Log (
      Code_Set_Id
    , Description
    , Map_Table_Name
    , Map_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts

  ) VALUES (
      n.Code_Set_Id
    , n.Description
    , n.Map_Table_Name
    , n.Map_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_Hist
  (
     Log_Id
    ,Prog_Host
    ,Prog_Name
    ,Func_Name
    ,Proc_Id
    ,Mesg_Type
    ,Mesg_Body
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Prog_Mode
    ,Prog_Vers
    ,Send_Alert_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Log_Id
    ,n.Prog_Host
    ,n.Prog_Name
    ,n.Func_Name
    ,n.Proc_Id
    ,n.Mesg_Type
    ,n.Mesg_Body
    ,n.Prog_Stat_Cd
    ,n.Prog_Stat_Desc
    ,n.Prog_Mode
    ,n.Prog_Vers
    ,n.Send_Alert_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Process </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Process
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Log (
      Process_Name
    , Description
    , Process_Type
    , Ctl_Id
    , Stream_Key
    , In_DB_Name
    , In_Object_Name
    , Out_DB_Name
    , Out_Object_Name
    , Target_TableDatabaseName
    , Target_TableName
    , Temp_DatabaseName
    , Key_Set_Id
    , Domain_Id
    , Code_Set_Id
    , Collect_Stats
    , Truncate_Target
    , Verification_Flag
    , File_Qualifier_Reset_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Description
    , n.Process_Type
    , n.Ctl_Id
    , n.Stream_Key
    , n.In_DB_Name
    , n.In_Object_Name
    , n.Out_DB_Name
    , n.Out_Object_Name
    , n.Target_TableDatabaseName
    , n.Target_TableName
    , n.Temp_DatabaseName
    , n.Key_Set_Id
    , n.Domain_Id
    , n.Code_Set_Id
    , n.Collect_Stats
    , n.Truncate_Target
    , n.Verification_Flag
    , n.File_Qualifier_Reset_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> UDGDWOPS.TD_Sandpit_Archive_Param </sc-trigger> */
REPLACE TRIGGER UDGDWOPS.TD_Sandpit_Archive_Param
AFTER DELETE ON UDGDWOPS.Sandpit_Archive_Param
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO UDGDWOPS.Sandpit_Archive_Param_Log (
    Param_Group
    ,Param_Name
    ,Param_Value
    ,Param_Desc
    ,Efft_Date
    ,Expy_Date
    ,Last_Update_By
    ,Last_Update_At
    ,Log_User
    ,Log_Ts
  ) VALUES (
    o.Param_Group
    ,o.Param_Name
    ,o.Param_Value
    ,o.Param_Desc
    ,o.Efft_Date
    ,o.Expy_Date
    ,o.Last_Update_By
    ,o.Last_Update_At
    ,USER
    ,CURRENT_TIMESTAMP(6)
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.MT202_EXC_RPRT_AUDT
(
REPORT                        
,SWIFT_MESSAGE_TEXT            
,UNIQUE_IDENTIFIER             
,MSG_ID                        
,MSG_SEQ                       
,MSG_TYPE                      
,DB_DATE                       
,SOURCE                        
,BRAND                         
,CBA_BIC                       
,EXTERNAL_PARTY_BIC            
,INP_OUT_IDENTIFIER            
,AMOUNT                        
,CURRENCY                      
,VAL_DATE                      
,REMITTER                      
,BENEFICIARY                   
,FIELD_20                      
,FIELD_21                      
,CREATED_DATE                  
,CDT_ID                        
,CDT_NAME                      
,DBT_ID                        
,DBT_NAME                      
,TAG_52                        
,TAG_58                        
,TAG_72                        
,SWF_FILE_REF                  
,TRN_PROC_PMS                  
,BUSN_DAYS                     
,IS_REVIEWED                   
,TEAM_RESP                     
,INVESTIGATION_STUS            
,TRAN_CATG                     
,PRODUCT                       
,OTHR_CMMT                     
,VERF_BY_IFTI_MNGR             
,IFTI_RPRT                     
,SUBM_FILE_NAME                
,SUBM_DATE                     
,SUBM_RCPT_NO                  
,INSERTION_DATE                
,EXC_OFCR                      
,EXC_OFCR_DTTM                 
,VERF_OFCR_CMMT                
,VERF_OFCR                     
,VERF_OFCR_DTTM                
,QA_OFCR_CMMT                  
,QA_OFCR                       
,QA_OFCR_DTTM                  
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(
n.REPORT                        
,n.SWIFT_MESSAGE_TEXT            
,n.UNIQUE_IDENTIFIER             
,n.MSG_ID                        
,n.MSG_SEQ                       
,n.MSG_TYPE                      
,n.DB_DATE                       
,n.SOURCE                        
,n.BRAND                         
,n.CBA_BIC                       
,n.EXTERNAL_PARTY_BIC            
,n.INP_OUT_IDENTIFIER            
,n.AMOUNT                        
,n.CURRENCY                      
,n.VAL_DATE                      
,n.REMITTER                      
,n.BENEFICIARY                   
,n.FIELD_20                      
,n.FIELD_21                      
,n.CREATED_DATE                  
,n.CDT_ID                        
,n.CDT_NAME                      
,n.DBT_ID                        
,n.DBT_NAME                      
,n.TAG_52                        
,n.TAG_58                        
,n.TAG_72                        
,n.SWF_FILE_REF                  
,n.TRN_PROC_PMS                  
,n.BUSN_DAYS                     
,n.IS_REVIEWED                   
,n.TEAM_RESP                     
,n.INVESTIGATION_STUS            
,n.TRAN_CATG                     
,n.PRODUCT                       
,n.OTHR_CMMT                     
,n.VERF_BY_IFTI_MNGR             
,n.IFTI_RPRT                     
,n.SUBM_FILE_NAME                
,n.SUBM_DATE                     
,n.SUBM_RCPT_NO                  
,n.INSERTION_DATE                
,n.EXC_OFCR                      
,n.EXC_OFCR_DTTM                 
,n.VERF_OFCR_CMMT                
,n.VERF_OFCR                     
,n.VERF_OFCR_DTTM                
,n.QA_OFCR_CMMT                  
,n.QA_OFCR                       
,n.QA_OFCR_DTTM                  
,USER
,Current_Timestamp(6)
,'UPDATE' 
);

--
/* <sc-trigger> PDTRMEXTD.GENID_COMM_CLASS </sc-trigger> */
--------------------------------------------------------------------------------

REPLACE TRIGGER PDTRMEXTD.GENID_COMM_CLASS
AFTER INSERT ON PDTRMCORE.EX_COMM_CLASS
 REFERENCING NEW AS NEWROW
 FOR EACH ROW
(

LOCKING TABLE PDTRMCORE.EX_COMM_CLASS FOR WRITE
/* update sequence number */
UPDATE A
FROM PDTRMEXTD.TRM_SEQ A
SET  SEQ_NO = A.SEQ_NO + 1
WHERE A.SEQ_TYPE_CD = 'COMM_CLASS'
;

/* set COMM_CLASS */
UPDATE A
FROM PDTRMCORE.EX_COMM_CLASS AS A
, (   SELECT
        SEQ_NO
 FROM PDTRMEXTD.TRM_SEQ
 WHERE
      SEQ_TYPE_CD = 'COMM_CLASS'
  )   AS B
SET  VECTOR_CLASS = B.SEQ_NO
WHERE  A.COMM_CLASS_ID = NEWROW.COMM_CLASS_ID;

);

--
/* <sc-trigger> PDSECURITY.TD_GDW_USER_ATTR </sc-trigger> */
REPLACE TRIGGER PDSECURITY.TD_GDW_USER_ATTR
AFTER DELETE ON PDSECURITY.GDW_USER_ATTR
REFERENCING OLD ROW AS O
FOR EACH ROW  (
INSERT INTO PDSECURITY.GDW_USER_ATTR_LOG (
      USERNAME
      ,USER_TYPE
      ,EMPL_I
      ,SAR_NO
      ,LAST_UPDATE_BY
      ,LAST_UPDATE_AT
      ,LOG_ACTION
      ,LOG_BY
      ,LOG_AT
  ) VALUES (
      O.USERNAME
      ,O.USER_TYPE
      ,O.EMPL_I
      ,O.SAR_NO
      ,O.LAST_UPDATE_BY
      ,O.LAST_UPDATE_AT
      ,'DELETE'
      ,USER
      ,Current_Timestamp(6)
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_LKP_0.BKEY_TI_Domain </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER P_P00_D_TCF_001_LKP_0.BKEY_TI_Domain
AFTER INSERT ON P_P00_D_TCF_001_LKP_0.BKEY_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_LKP_0.BKEY_T_Domain_Log (
      Key_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_System_File_Extract </sc-trigger> */



REPLACE  TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_System_File_Extract
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_System_File_Extract
REFERENCING NEW ROW AS n
FOR EACH ROW (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_SFE_Log (
      Ctl_Id
    , File_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , File_Qualifier
    , Data_File_Name
    , Ctl_File_Name
    , Extraction_Date
    , Ext_Start_Time
    , Ext_End_Time
    , TD_Session_Id
    , Tool_Session_Id
    , Load_Status
    , Count_Source
    , Count_Target
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.File_Qualifier
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Extraction_Date
    , n.Ext_Start_Time
    , n.Ext_End_Time
    , n.TD_Session_Id
    , n.Tool_Session_Id
    , n.Load_Status
    , n.Count_Source
    , n.Count_Target
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR_TRIG_D </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendersCorrespondentCode1,
sendersCorrespondentName1,
sendersCorrespondentCity1,
sendersCorrespondentCountry1,
sendersCorrespondentCode2,
sendersCorrespondentName2,
sendersCorrespondentCity2,
sendersCorrespondentCountry2,
sendersCorrespondentCode3,
sendersCorrespondentName3,
sendersCorrespondentCity3,
sendersCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_CORR',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendersCorrespondentCode1,
n.sendersCorrespondentName1,
n.sendersCorrespondentCity1,
n.sendersCorrespondentCountry1,
n.sendersCorrespondentCode2,
n.sendersCorrespondentName2,
n.sendersCorrespondentCity2,
n.sendersCorrespondentCountry2,
n.sendersCorrespondentCode3,
n.sendersCorrespondentName3,
n.sendersCorrespondentCity3,
n.sendersCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_File_Process </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_File_Process
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_File_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_File_Process_Log (
      Process_Name
    , Ctl_Id
    , File_Id
    , Check_Load_Status
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Ctl_Id
    , n.File_Id
    , n.Check_Load_Status
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_D00_D_TCF_001_LKP_0.BMAP_TU_Domain </sc-trigger> */


REPLACE TRIGGER D_D00_D_TCF_001_LKP_0.BMAP_TU_Domain
AFTER UPDATE ON D_D00_D_TCF_001_LKP_0.BMAP_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_LKP_0.BMAP_T_Domain_Log (
      Code_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_Hist
  (
     Stream_Name
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Name
    ,n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_REML_CUST_AGGR_SMRY_HIST_DEL </sc-trigger> */

/*CREATE DEL TRIGGER FOR  REML_CUST_AGGR_SMRY_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_REML_CUST_AGGR_SMRY_HIST_DEL 
AFTER DELETE ON U_D_DSV_001_REM_1.REML_CUST_AGGR_SMRY_HIST 
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.REML_CUST_AGGR_SMRY_HIST_LOG  SELECT A.*,USER,'DEL', CURRENT_TIMESTAMP FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Process_Type </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Process_Type
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Type
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Log (
      Process_Type
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Transform_SurrKeyCol </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Transform_SurrKeyCol
AFTER UPDATE ON PDGENTCF.CTLFW_T_Transform_SurrKeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Transform_SKCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,SurrKey_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.SurrKey_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.TI_BKey_Domain </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.TI_BKey_Domain
AFTER INSERT ON PDGDWPIIOBF.BKey_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGDWPIIOBF.BKey_Domain_Log (
      Key_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGEN_REF.BKEY_TI_Domain </sc-trigger> */
REPLACE TRIGGER PDGEN_REF.BKEY_TI_Domain
AFTER INSERT ON PDGEN_REF.BKEY_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGEN_REF.BKEY_T_Domain_Log (
      Key_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_S00_D_TCF_001_LKP_0.BKEY_TI_Key_Set </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_LKP_0.BKEY_TI_Key_Set
AFTER INSERT ON D_S00_D_TCF_001_LKP_0.BKEY_T_Key_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_LKP_0.BKEY_T_Key_Set_Log (
      Key_Set_Id
    , Description
    , Key_Table_Name
    , Key_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Description
    , n.Key_Table_Name
    , n.Key_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_SQLState_Class_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_SQLState_Class_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_SQLState_Class
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_SQLState_Class_Hist
  (
     ClassCode
    ,ClassDefinition
    ,ClassNote
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     n.ClassCode
    ,n.ClassDefinition
    ,n.ClassNote
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_TS
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_TS
    ,'I'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_System </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_System
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_System_Log (
      Ctl_Id
    , System_Name
    , Path_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.System_Name
    , n.Path_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */
Replace TRIGGER U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_AUDT
(stmt_line_sk
,unique_id
,rprt_name
,acct_no
,acct_name
,acct_cncy
,acct_baln_pool
,date_tag_created
,value_date
,amt
,txn_type
,internal_ref
,external_ref
,description
,swf_file_ref
,mtch_type
,team_resp
,investigation_stus
,tran_catg
,is_reviewed
,othr_cmmt
,busn_days
,ifti_rprt
,subm_date
,subm_file_name
,subm_rcpt_no
,imt_mtch_grup_id
,imt_refn_1
,imt_refn_2
,imt_refn_3
,pms_ref_no
,pms_dbt_name
,pms_sbk_ref_no
,wss_srce
,wss_portfolio
,wss_audt_note
,wss_net_info
,wss_area
,itc_case_no
,wss_extl_deal_numb
,action_date
,EXC_OFCR
,EXC_OFCR_DTTM
,VERF_OFCR_CMMT
,VERF_OFCR
,VERF_OFCR_DTTM
,QA_OFCR_CMMT
,QA_OFCR
,QA_OFCR_DTTM
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(n.stmt_line_sk
,n.unique_id
,n.rprt_name
,n.acct_no
,n.acct_name
,n.acct_cncy
,n.acct_baln_pool
,n.date_tag_created
,n.value_date
,n.amt
,n.txn_type
,n.internal_ref
,n.external_ref
,n.description
,n.swf_file_ref
,n.mtch_type
,n.team_resp
,n.investigation_stus
,n.tran_catg
,n.is_reviewed
,n.othr_cmmt
,n.busn_days
,n.ifti_rprt
,n.subm_date
,n.subm_file_name
,n.subm_rcpt_no
,n.imt_mtch_grup_id
,n.imt_refn_1
,n.imt_refn_2
,n.imt_refn_3
,n.pms_ref_no
,n.pms_dbt_name
,n.pms_sbk_ref_no
,n.wss_srce
,n.wss_portfolio
,n.wss_audt_note
,n.wss_net_info
,n.wss_area
,n.itc_case_no
,n.wss_extl_deal_numb
,n.action_date
,n.EXC_OFCR
,n.EXC_OFCR_DTTM
,n.VERF_OFCR_CMMT
,n.VERF_OFCR
,n.VERF_OFCR_DTTM
,n.QA_OFCR_CMMT
,n.QA_OFCR
,n.QA_OFCR_DTTM
,USER
,Current_Timestamp(6)
,'INSERT' 
);

--
/* <sc-trigger> PDTRMEXTD.GENID_COMM_SUBCLASS </sc-trigger> */
--------------------------------------------------------------------------------

REPLACE TRIGGER PDTRMEXTD.GENID_COMM_SUBCLASS
AFTER INSERT ON PDTRMCORE.EX_COMM_SUBCLASS
REFERENCING NEW AS NEWROW
FOR EACH ROW
(

LOCKING TABLE PDTRMCORE.EX_COMM_SUBCLASS FOR WRITE

/* update sequence number */
UPDATE A
FROM PDTRMEXTD.TRM_SEQ A
SET  SEQ_NO = A.SEQ_NO + 1
WHERE A.SEQ_TYPE_CD = 'COMM_SUBCLASS'
;

/* set COMM_SUBCLASS */
UPDATE A
FROM PDTRMCORE.EX_COMM_SUBCLASS AS A
, (SELECT SEQ_NO
   FROM  PDTRMEXTD.TRM_SEQ
   WHERE SEQ_TYPE_CD = 'COMM_SUBCLASS'
  )   AS B
SET  VECTOR_SUB_CLASS = B.SEQ_NO
WHERE  A.COMM_SUBCLASS_ID = NEWROW.COMM_SUBCLASS_ID;
)
;

--
/* <sc-trigger> P_P00_D_TCF_001_LKP_0.BMAP_TU_Code_Set </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER P_P00_D_TCF_001_LKP_0.BMAP_TU_Code_Set
AFTER UPDATE ON P_P00_D_TCF_001_LKP_0.BMAP_T_Code_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_LKP_0.BMAP_T_Code_Set_Log (
      Code_Set_Id
    , Description
    , Map_Table_Name
    , Map_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Description
    , n.Map_Table_Name
    , n.Map_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Transform_SurrKeyCol </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Transform_SurrKeyCol
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Transform_SurrKeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Transform_SKCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,SurrKey_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.SurrKey_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST_TRIG_D </sc-trigger> */
-- DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receivingInstnCode,
receivingInstnName,
receivingInstnCity,
receivingInstnCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_INST',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receivingInstnCode,
n.receivingInstnName,
n.receivingInstnCity,
n.receivingInstnCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Process_Param </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Process_Param
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Param_Log (
      Process_Name
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> D_D00_D_TCF_001_LKP_0.BKEY_TU_Domain </sc-trigger> */


REPLACE TRIGGER D_D00_D_TCF_001_LKP_0.BKEY_TU_Domain
AFTER UPDATE ON D_D00_D_TCF_001_LKP_0.BKEY_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_LKP_0.BKEY_T_Domain_Log (
      Key_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Cand_Obj
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Hist
  (
     Cand_Id
    ,Trtmnt_Cd
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,TDM_Obj_Type
    ,TDM_Copy_Stat
    ,TDM_Compare_DDL
    ,TDM_Where_Clause
    ,TDM_Key_Column
    ,TDM_Query_Band_Str
    ,TDM_Job_Desc
    ,TDM_Staging_Database_Nm
    ,TDM_Job_Cd
    ,Force_Flag
    ,Tbl_Size
    ,Job_Cd
    ,UOWId
    ,num_Tbl
    ,running_flag
    ,failed_flag
    ,exists_flag
    ,Tbl_flag
    ,changed_flag
    ,Rule_No
    ,Limit_Desc
    ,dedupe_cd
    ,Enabled_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Id
    ,o.Trtmnt_Cd
    ,o.TDM_Database_Nm
    ,o.TDM_Obj_Nm
    ,o.TDM_Obj_Type
    ,o.TDM_Copy_Stat
    ,o.TDM_Compare_DDL
    ,o.TDM_Where_Clause
    ,o.TDM_Key_Column
    ,o.TDM_Query_Band_Str
    ,o.TDM_Job_Desc
    ,o.TDM_Staging_Database_Nm
    ,o.TDM_Job_Cd
    ,o.Force_Flag
    ,o.Tbl_Size
    ,o.Job_Cd
    ,o.UOWId
    ,o.num_Tbl
    ,o.running_flag
    ,o.failed_flag
    ,o.exists_flag
    ,o.Tbl_flag
    ,o.changed_flag
    ,o.Rule_No
    ,o.Limit_Desc
    ,o.dedupe_cd
    ,o.Enabled_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_TTN_REML_CUST_AGGR_SMRY_HIST_DEL </sc-trigger> */

/*CREATE DEL TRIGGER FOR  TTN_REML_CUST_AGGR_SMRY_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_TTN_REML_CUST_AGGR_SMRY_HIST_DEL 
AFTER DELETE ON U_D_DSV_001_REM_1.TTN_REML_CUST_AGGR_SMRY_HIST 
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.TTN_REML_CUST_AGGR_SMRY_HIST_LOG  SELECT A.*,USER,'DEL', CURRENT_TIMESTAMP FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Corporate_Entity </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Corporate_Entity
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity_Log (
      CE_Id
    , Corporate_Entity_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Corporate_Entity_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Transform_KeyCol </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Transform_KeyCol
AFTER INSERT ON PDGENTCF.CTLFW_T_Transform_KeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Transform_KeyCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,Key_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.Key_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Step_Parm_ADT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Step_Parm_ADT
  AFTER DELETE ON PDGDWPIIOBF.Dtf_Job_Step_Parm
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Step_Parm_Hst
  (
     Job_Cd
    ,Job_Step_No
    ,Job_Step_Seq_No
    ,Job_Step_Parm
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.Job_Step_No
    ,o.Job_Step_Seq_No
    ,o.Job_Step_Parm
    ,'D'
   );

--
/* <sc-trigger> PDGEN_REF.BMAP_TU_Domain </sc-trigger> */
REPLACE TRIGGER PDGEN_REF.BMAP_TU_Domain
AFTER UPDATE ON PDGEN_REF.BMAP_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGEN_REF.BMAP_T_Domain_Log (
      Code_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_S00_D_TCF_001_LKP_0.BKEY_TU_Key_Set </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_LKP_0.BKEY_TU_Key_Set
AFTER UPDATE ON D_S00_D_TCF_001_LKP_0.BKEY_T_Key_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_LKP_0.BKEY_T_Key_Set_Log (
      Key_Set_Id
    , Description
    , Key_Table_Name
    , Key_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Description
    , n.Key_Table_Name
    , n.Key_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_Stat_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_Stat_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_JA_Stat
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_Stat_Hist
  (
     Submit_TS
    ,No_Cands
    ,No_Obj
    ,No_MB
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Submit_TS
    ,n.No_Cands
    ,n.No_Obj
    ,n.No_MB
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Load </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Load
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Load
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Load_Log 
 (
  Ctl_Id
  ,File_Id
  ,Business_Date
  ,Business_Date_Cycle_Num
  ,Business_Date_Cycle_Start_Ts
  ,File_Qualifier
  ,Process_Name
  ,Process_Id
  ,TD_Session_Id
  ,Tool_Session_Id
  ,S_Tbl_Name
  ,S_DB_Name
  ,Rows_Input
  ,Rows_Considered
  ,Rows_Not_Considered
  ,Rows_Rejected
  ,Rows_Inserted
  ,Rows_Updated
  ,Rows_Deleted
  ,Rows_ET
  ,Rows_UV
  ,Run_Date
  ,Start_Time
  ,End_Time
  ,Load_Status
  ,Update_Date
  ,Update_User
  ,Update_Time
  ,Update_Ts
   ) 
  VALUES 
   (
  n.Ctl_Id
  ,n.File_Id
  ,n.Business_Date
  ,n.Business_Date_Cycle_Num
  ,n.Business_Date_Cycle_Start_Ts
  ,n.File_Qualifier
  ,n.Process_Name
  ,n.Process_Id
  ,n.TD_Session_Id
  ,n.Tool_Session_Id
  ,n.S_Tbl_Name
  ,n.S_DB_Name
  ,n.Rows_Input
  ,n.Rows_Considered
  ,n.Rows_Not_Considered
  ,n.Rows_Rejected
  ,n.Rows_Inserted
  ,n.Rows_Updated
  ,n.Rows_Deleted
  ,n.Rows_ET
  ,n.Rows_UV
  ,n.Run_Date
  ,n.Start_Time
  ,n.End_Time
  ,n.Load_Status
  ,n.Update_Date
  ,n.Update_User
  ,n.Update_Time
  ,n.Update_Ts
   );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receivingInstnCode,
receivingInstnName,
receivingInstnCity,
receivingInstnCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_INST',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receivingInstnCode,
n.receivingInstnName,
n.receivingInstnCity,
n.receivingInstnCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> PDTRMEXTD.GENID_CHANNEL_INSTANCE </sc-trigger> */
--------------------------------------------------------------------------------

REPLACE TRIGGER PDTRMEXTD.GENID_CHANNEL_INSTANCE
AFTER INSERT ON PDTRMCORE.EX_CHANNEL_INSTANCE
	REFERENCING NEW AS NEWROW 
	FOR EACH ROW
(

LOCKING TABLE PDTRMCORE.EX_CHANNEL_INSTANCE FOR WRITE

/* set Channel_Id */
UPDATE A 
FROM PDTRMCORE.EX_CHANNEL_INSTANCE AS A
, ( 
	SELECT	DT1.Channel_Class_Id, EXCI.Channel_Instance_Id, New_Channel_Id
	FROM	PDTRMCORE.EX_CHANNEL_INSTANCE EXCI
	INNER JOIN
		PDTRMCORE.CM_CHANNEL_INSTANCE CMCI
	ON	EXCI.Channel_Instance_Id = CMCI.Channel_Instance_Id
	INNER JOIN
		(
		SELECT	Channel_Class_Id, MAX(COALESCE(Channel_Id, 0)) + 1 AS New_Channel_Id
		FROM	PDTRMCORE.CM_CHANNEL_INSTANCE CMCI1
		INNER JOIN
			PDTRMCORE.EX_CHANNEL_INSTANCE EXCI1
		ON	CMCI1.Channel_Instance_Id = EXCI1.Channel_Instance_Id
		GROUP BY 1
		) DT1 (Channel_Class_Id, New_Channel_Id)

	ON CMCI.Channel_Class_Id = DT1.Channel_Class_Id
	WHERE EXCI.Channel_Id IS NULL
) B
SET	Channel_Id  = B.New_Channel_Id
WHERE	A.Channel_Instance_Id = NEWROW.Channel_Instance_Id
AND	B.Channel_Instance_Id = NEWROW.Channel_Instance_Id
 ;   
);

--
/* <sc-trigger> P_P00_D_TCF_001_LKP_0.BMAP_TI_Domain </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER P_P00_D_TCF_001_LKP_0.BMAP_TI_Domain
AFTER INSERT ON P_P00_D_TCF_001_LKP_0.BMAP_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_LKP_0.BMAP_T_Domain_Log (
      Code_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Stream_BusDate </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Stream_BusDate
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Business_Date
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Business_Date
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR_TRIG_I </sc-trigger> */
-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR
 --------------------------------------------------------------------- 
 --DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendersCorrespondentCode1,
sendersCorrespondentName1,
sendersCorrespondentCity1,
sendersCorrespondentCountry1,
sendersCorrespondentCode2,
sendersCorrespondentName2,
sendersCorrespondentCity2,
sendersCorrespondentCountry2,
sendersCorrespondentCode3,
sendersCorrespondentName3,
sendersCorrespondentCity3,
sendersCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_CORR',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendersCorrespondentCode1,
n.sendersCorrespondentName1,
n.sendersCorrespondentCity1,
n.sendersCorrespondentCountry1,
n.sendersCorrespondentCode2,
n.sendersCorrespondentName2,
n.sendersCorrespondentCity2,
n.sendersCorrespondentCountry2,
n.sendersCorrespondentCode3,
n.sendersCorrespondentName3,
n.sendersCorrespondentCity3,
n.sendersCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Transform_SurrKeyCol </sc-trigger> */
--------------------------------------------------------------------------------


/*******************************************************************************
* CREATE CTLFW_TI_Transform_SurrKeyCol INSERT TRIGGER
*******************************************************************************/
REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Transform_SurrKeyCol
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Transform_SurrKeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Transform_SKCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,SurrKey_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.SurrKey_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> D_D00_D_TCF_001_LKP_0.BKEY_TI_Domain </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_LKP_0.BKEY_TI_Domain
AFTER INSERT ON D_D00_D_TCF_001_LKP_0.BKEY_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_LKP_0.BKEY_T_Domain_Log (
      Key_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_Hist
  (
     Cand_Id
    ,Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Job_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_REML_PAYT_DETL_HIST_UPD </sc-trigger> */


/*CREATE UPD TRIGGER FOR  REML_PAYT_DETL_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_REML_PAYT_DETL_HIST_UPD
AFTER UPDATE ON U_D_DSV_001_REM_1.REML_PAYT_DETL_HIST
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.REML_PAYT_DETL_HIST_LOG  SELECT A.*,USER,'UPD', CURRENT_TIMESTAMP  FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Corporate_Entity </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Corporate_Entity
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity_Log (
      CE_Id
    , Corporate_Entity_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Corporate_Entity_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Stream_Param </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Stream_Param
AFTER INSERT ON PDGENTCF.CTLFW_T_Stream_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Stream_Param_Log (
      Stream_Key
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> PDGDWPIIOBF.TD_PIIDataScanningCheckType </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.TD_PIIDataScanningCheckType
AFTER DELETE ON PDGDWPIIOBF.PIIDataScanningCheckType
REFERENCING OLD ROW AS o
FOR EACH ROW  (
INSERT INTO PDGDWPIIOBF.PIIDataScanningCheckTypeLog (
      CheckType
    , CheckTypeRule
    , CheckTypeDesc
    , PIIDataType
    , UpdateUser
    , UpdateTs
  ) VALUES (
      o.CheckType
    , o.CheckTypeRule
    , o.CheckTypeDesc
    , o.PIIDataType
    , o.UpdateUser
    , o.UpdateTs
  );
);

--
/* <sc-trigger> PDGEN_REF.BKEY_TU_Key_Set </sc-trigger> */
REPLACE TRIGGER PDGEN_REF.BKEY_TU_Key_Set
AFTER UPDATE ON PDGEN_REF.BKEY_T_Key_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGEN_REF.BKEY_T_Key_Set_Log (
      Key_Set_Id
    , Description
    , Key_Table_Name
    , Key_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Description
    , n.Key_Table_Name
    , n.Key_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_S00_D_TCF_001_LKP_0.BMAP_TI_Valid_Language </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_LKP_0.BMAP_TI_Valid_Language
AFTER INSERT ON D_S00_D_TCF_001_LKP_0.BMAP_T_Valid_Language
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_LKP_0.BMAP_T_Valid_Language_Log (
      Language_Id
    , Language_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Language_Id
    , n.Language_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Alrt_Action_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Alrt_Action_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Alrt_Action
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Alrt_Action_Hist
  (
     ActionCode
    ,ActionDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.ActionCode
    ,n.ActionDesc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Process_Param </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Process_Param
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Param_Log (
      Process_Name
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST_TRIG_D </sc-trigger> */

-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receivingInstnCode,
receivingInstnName,
receivingInstnCity,
receivingInstnCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_INST',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receivingInstnCode,
n.receivingInstnName,
n.receivingInstnCity,
n.receivingInstnCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> PDTRMEXTD.GENID_CAMPAIGN_COLLECTION </sc-trigger> */
REPLACE TRIGGER PDTRMEXTD.GENID_CAMPAIGN_COLLECTION
AFTER INSERT ON PDTRMCORE.EX_CAMPAIGN_COLLECTION
	REFERENCING NEW AS NEWROW 
	FOR EACH ROW
(

LOCKING TABLE PDTRMCORE.EX_CAMPAIGN_COLLECTION FOR WRITE
/* update sequence number */
UPDATE A 
FROM PDTRMEXTD.TRM_SEQ A
SET 	SEQ_NO = A.SEQ_NO + 1
WHERE	A.SEQ_TYPE_CD = 'CAMPAIGN_COLLECTION'
; 

/* set Campaign_Class_Id */
UPDATE A
FROM PDTRMCORE.EX_CAMPAIGN_COLLECTION AS A
, (   SELECT 
        SEQ_NO
	FROM PDTRMEXTD.TRM_SEQ
	WHERE 
      SEQ_TYPE_CD = 'CAMPAIGN_COLLECTION'
  )   AS B  
SET 	Campaign_Class_Id = B.SEQ_NO   
WHERE 	A.CAMPAIGN_COLLECTION_ID = NEWROW.CAMPAIGN_COLLECTION_ID;
   
);

--
/* <sc-trigger> P_P00_D_TCF_001_LKP_0.BMAP_TI_Code_Set </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER P_P00_D_TCF_001_LKP_0.BMAP_TI_Code_Set
AFTER INSERT ON P_P00_D_TCF_001_LKP_0.BMAP_T_Code_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_LKP_0.BMAP_T_Code_Set_Log (
      Code_Set_Id
    , Description
    , Map_Table_Name
    , Map_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts

  ) VALUES (
      n.Code_Set_Id
    , n.Description
    , n.Map_Table_Name
    , n.Map_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_File_Process </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_File_Process
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_File_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_File_Process_Log (
      Process_Name
    , Ctl_Id
    , File_Id
    , Check_Load_Status
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Ctl_Id
    , n.File_Id
    , n.Check_Load_Status
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */



--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_BWA_DR_TRIG_U
REPLACE TRIGGER U_D_DSV_001_NIT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.NOST_BWA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_BWA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'UPDATE'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Stream_BusDate </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Stream_BusDate
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Business_Date
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Business_Date
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_D00_D_TCF_001_LKP_0.BMAP_TU_Valid_Language </sc-trigger> */


REPLACE TRIGGER D_D00_D_TCF_001_LKP_0.BMAP_TU_Valid_Language
AFTER UPDATE ON D_D00_D_TCF_001_LKP_0.BMAP_T_Valid_Language
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_LKP_0.BMAP_T_Valid_Language_Log (
      Language_Id
    , Language_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Language_Id
    , n.Language_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Step_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Step_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Cand_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Step_Hist
  (
     Cand_Id
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Exec_Param_KVP
    ,Executable_User
    ,Executable_TDPID
    ,Executable_BT_ET_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Step_No
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Exec_Param_KVP
    ,n.Executable_User
    ,n.Executable_TDPID
    ,n.Executable_BT_ET_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_TTN_REML_PAYT_DETL_HIST_UPD </sc-trigger> */


/*CREATE UPD TRIGGER FOR  TTN_REML_PAYT_DETL_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_TTN_REML_PAYT_DETL_HIST_UPD
AFTER UPDATE ON U_D_DSV_001_REM_1.TTN_REML_PAYT_DETL_HIST
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.TTN_REML_PAYT_DETL_HIST_LOG  SELECT A.*,USER,'UPD', CURRENT_TIMESTAMP  FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_System_File_Extract </sc-trigger> */


REPLACE  TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_System_File_Extract
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_System_File_Extract
REFERENCING NEW ROW AS n
FOR EACH ROW (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_SFE_Log (
      Ctl_Id
    , File_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , File_Qualifier
    , Data_File_Name
    , Ctl_File_Name
    , Extraction_Date
    , Ext_Start_Time
    , Ext_End_Time
    , TD_Session_Id
    , Tool_Session_Id
    , Load_Status
    , Count_Source
    , Count_Target
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.File_Qualifier
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Extraction_Date
    , n.Ext_Start_Time
    , n.Ext_End_Time
    , n.TD_Session_Id
    , n.Tool_Session_Id
    , n.Load_Status
    , n.Count_Source
    , n.Count_Target
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Process_Type </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Process_Type
AFTER UPDATE ON PDGENTCF.CTLFW_T_Process_Type
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Process_Type_Log (
      Process_Type
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Detl_AIT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Detl_AIT
  AFTER INSERT ON PDGDWPIIOBF.Dtf_Job_Detl
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Detl_Hst
  (
     Job_Cd
    ,Job_Priority
    ,Source_Tdpid
    ,Target_Tdpid
    ,Source_User
    ,Source_Passwd
    ,Target_User
    ,Target_Passwd
    ,Max_Agents_Per_Task
    ,Data_Streams
    ,Source_Sessions
    ,Target_Sessions
    ,Overwrite_Existing_Objects
    ,Force_Utility
    ,Foreign_Server
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Staging_Database
    ,Staging_Database_For_Table
    ,Object_Include_Flag
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Job_Priority
    ,n.Source_Tdpid
    ,n.Target_Tdpid
    ,n.Source_User
    ,n.Source_Passwd
    ,n.Target_User
    ,n.Target_Passwd
    ,n.Max_Agents_Per_Task
    ,n.Data_Streams
    ,n.Source_Sessions
    ,n.Target_Sessions
    ,n.Overwrite_Existing_Objects
    ,n.Force_Utility
    ,n.Foreign_Server
    ,n.Log_Level
    ,n.Online_Archive
    ,n.Freeze_Job_Steps
    ,n.Staging_Database
    ,n.Staging_Database_For_Table
    ,n.Object_Include_Flag
    ,'I'
   );

--
/* <sc-trigger> PDGEN_REF.BKEY_TI_Key_Set </sc-trigger> */
REPLACE TRIGGER PDGEN_REF.BKEY_TI_Key_Set
AFTER INSERT ON PDGEN_REF.BKEY_T_Key_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGEN_REF.BKEY_T_Key_Set_Log (
      Key_Set_Id
    , Description
    , Key_Table_Name
    , Key_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Description
    , n.Key_Table_Name
    , n.Key_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_S00_D_TCF_001_LKP_0.BKEY_TI_Domain </sc-trigger> */
REPLACE TRIGGER D_S00_D_TCF_001_LKP_0.BKEY_TI_Domain
AFTER INSERT ON D_S00_D_TCF_001_LKP_0.BKEY_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_LKP_0.BKEY_T_Domain_Log (
      Key_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_Hist
  (
     Daemon_Name
    ,Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Enabled_Flag
    ,Loop_Delay_Secs
    ,Send_Alert_On_Error_Flag
    ,Autosys_User
    ,Autosys_Host
    ,Autosys_Host_IP
    ,Autosys_Stus_Restrict_Flag
    ,Autosys_Stus_Allowed_Set
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Daemon_Name
    ,n.Prog_Name
    ,n.Prog_Mode
    ,n.Prog_User
    ,n.Prog_Host
    ,n.Enabled_Flag
    ,n.Loop_Delay_Secs
    ,n.Send_Alert_On_Error_Flag
    ,n.Autosys_User
    ,n.Autosys_Host
    ,n.Autosys_Host_IP
    ,n.Autosys_Stus_Restrict_Flag
    ,n.Autosys_Stus_Allowed_Set
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFWX_TU_Xfm_Process </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFWX_TU_Xfm_Process
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process
REFERENCING NEW ROW AS NR
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process_Log (
      Process_Name,           
      Busn_Dt ,         
      Xfm_Run_Dt ,                        
      Xfm_Rows_Inserted ,                       
      Xfm_Stg_Ld_Status ,           
      Xfm_Stg_Ld_Start_Time ,
      Xfm_Stg_Ld_End_Time ,   
      Xfm_Stg_Val_Start_Time ,
      Xfm_Stg_Val_End_Time ,  
      Msur_Mdl_Pub_Run_Dt ,         
      Msur_Mdl_Pub_Process_Id ,           
      Msur_Mdl_Pub_Start_Time ,           
      Msur_Mdl_Pub_End_Time ,
      Msur_Mdl_Pub_Status ,   
      Msur_Mdl_Pub_Priority , 
      Pub_Stream_Key ,
      Update_User ,           
      Update_Ts 
  ) VALUES (
      NR.Process_Name,        
      NR.Busn_Dt ,            
      NR.Xfm_Run_Dt ,                     
      NR.Xfm_Rows_Inserted ,                    
      NR.Xfm_Stg_Ld_Status ,        
      NR.Xfm_Stg_Ld_Start_Time ,
      NR.Xfm_Stg_Ld_End_Time ,
      NR.Xfm_Stg_Val_Start_Time ,
      NR.Xfm_Stg_Val_End_Time ,     
      NR.Msur_Mdl_Pub_Run_Dt ,            
      NR.Msur_Mdl_Pub_Process_Id ,        
      NR.Msur_Mdl_Pub_Start_Time ,        
      NR.Msur_Mdl_Pub_End_Time ,
      NR.Msur_Mdl_Pub_Status ,      
      NR.Msur_Mdl_Pub_Priority ,    
      NR.Pub_Stream_Key ,
      NR.Update_User ,        
      NR.Update_Ts 
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receiversCorrespondentCode1,
receiversCorrespondentName1,
receiversCorrespondentCity1,
receiversCorrespondentCountry1,
receiversCorrespondentCode2,
receiversCorrespondentName2,
receiversCorrespondentCity2,
receiversCorrespondentCountry2,
receiversCorrespondentCode3,
receiversCorrespondentName3,
receiversCorrespondentCity3,
receiversCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_CORR',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receiversCorrespondentCode1,
n.receiversCorrespondentName1,
n.receiversCorrespondentCity1,
n.receiversCorrespondentCountry1,
n.receiversCorrespondentCode2,
n.receiversCorrespondentName2,
n.receiversCorrespondentCity2,
n.receiversCorrespondentCountry2,
n.receiversCorrespondentCode3,
n.receiversCorrespondentName3,
n.receiversCorrespondentCity3,
n.receiversCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> PDTRMEXTD.GENID_EX_COMMUNICATION </sc-trigger> */
REPLACE TRIGGER PDTRMEXTD.GENID_EX_COMMUNICATION
AFTER INSERT ON PDTRMCORE.EX_COMMUNICATION
 REFERENCING NEW AS NEWROW
 FOR EACH ROW
(
 -- Trigger a write lock on CM_COMMUNICATION to prevent a row from being insert while we're running
 UPDATE  PDTRMCORE.CM_COMMUNICATION
 SET     Communication_Id = Communication_Id

 -- Update EX_COMMUNICATION with a new / updated version id assuming there's a CM_COMMUNICATION row to work with
 ;EXEC PDTRMEXTD.M_GENID_COMMUNICATION(NEWROW.Communication_Id);
);

--
/* <sc-trigger> P_P00_D_TCF_001_LKP_0.BKEY_TU_Domain </sc-trigger> */
REPLACE TRIGGER P_P00_D_TCF_001_LKP_0.BKEY_TU_Domain
AFTER UPDATE ON P_P00_D_TCF_001_LKP_0.BKEY_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_LKP_0.BKEY_T_Domain_Log (
      Key_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Stream_Param </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Stream_Param
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
      Stream_Key
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.TRI_tbl_Hugo_HIST </sc-trigger> */
Create TRIGGER U_D_DSV_001_NIT_1.TRI_tbl_Hugo_HIST
AFTER INSERT ON U_D_DSV_001_NIT_1.tbl_Hugo
	REFERENCING NEW AS NewRow
	FOR EACH ROW
		INSERT U_D_DSV_001_NIT_1.tbl_Hugo_HIST
			VALUES
			(
			NewRow.int_col,
			NewRow.col1||  x'0A',
			NewRow.col2||  x'0A'
			);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Transform_KeyCol </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Transform_KeyCol
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,Key_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.Key_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> D_D00_D_TCF_001_LKP_0.BKEY_TU_Key_Set </sc-trigger> */


REPLACE TRIGGER D_D00_D_TCF_001_LKP_0.BKEY_TU_Key_Set
AFTER UPDATE ON D_D00_D_TCF_001_LKP_0.BKEY_T_Key_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_LKP_0.BKEY_T_Key_Set_Log (
      Key_Set_Id
    , Description
    , Key_Table_Name
    , Key_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Description
    , n.Key_Table_Name
    , n.Key_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_Hist
  (
     Obj
    ,Is_Current_Flag
    ,Is_Enabled_Flag
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Key_Type
    ,Key_Name
    ,Key_Sql
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Obj
    ,n.Is_Current_Flag
    ,n.Is_Enabled_Flag
    ,n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Key_Type
    ,n.Key_Name
    ,n.Key_Sql
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_TTN_REML_ACCT_AGGR_HIST_DEL </sc-trigger> */

/*CREATE DEL TRIGGER FOR  TTN_REML_ACCT_AGGR_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_TTN_REML_ACCT_AGGR_HIST_DEL 
AFTER DELETE ON U_D_DSV_001_REM_1.TTN_REML_ACCT_AGGR_HIST 
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.TTN_REML_ACCT_AGGR_HIST_LOG  SELECT A.*,USER,'DEL', CURRENT_TIMESTAMP FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_File_Process </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_File_Process
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_File_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_File_Process_Log (
      Process_Name
    , Ctl_Id
    , File_Id
    , Check_Load_Status
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Ctl_Id
    , n.File_Id
    , n.Check_Load_Status
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Transform_SurrKeyCol </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Transform_SurrKeyCol
AFTER INSERT ON PDGENTCF.CTLFW_T_Transform_SurrKeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Transform_SKCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,SurrKey_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.SurrKey_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Cd_ADT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Cd_ADT
  AFTER DELETE ON PDGDWPIIOBF.Dtf_Job_Cd
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Cd_Hst
  (
     Job_Cd
    ,Tgt_Databasename
 ,Databasename
    ,ObjectName
    ,ObjectType
    ,CopyStat
 ,CopyData
    ,CompareDDL
 ,TruncateTgtTable
    ,WhereClause
    ,KeyColumn
    ,Query_Band
    ,Job_Dscrp
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.Tgt_Databasename
 ,o.Databasename
    ,o.ObjectName
    ,o.ObjectType
    ,o.CopyStat
 ,o.CopyData
    ,o.CompareDDL
 ,o.TruncateTgtTable
    ,o.WhereClause
    ,o.KeyColumn
    ,o.Query_Band
    ,o.Job_Dscrp
    ,'D'
   );

--
/* <sc-trigger> PDGEN_REF.BMAP_TI_Domain </sc-trigger> */
REPLACE TRIGGER PDGEN_REF.BMAP_TI_Domain
AFTER INSERT ON PDGEN_REF.BMAP_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGEN_REF.BMAP_T_Domain_Log (
      Code_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_S00_D_TCF_001_LKP_0.BMAP_TI_Domain </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_LKP_0.BMAP_TI_Domain
AFTER INSERT ON D_S00_D_TCF_001_LKP_0.BMAP_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_LKP_0.BMAP_T_Domain_Log (
      Code_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I_Src
    ,Max_Pros_Key_I_Tgt
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Nm
    ,n.Db_Nm
    ,n.Tbl_Nm
    ,n.User_ID
    ,n.Max_Pros_Key_I_Src
    ,n.Max_Pros_Key_I_Tgt
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TD_Stream_Param </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TD_Stream_Param
AFTER DELETE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING OLD ROW AS O  
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
    Stream_Key
    , Param_Group
    , Param_Name
    , Description
    , Param_Value
    , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
    o.Stream_Key
    , o.Param_Group
    , o.Param_Name
    , o.Description
    , o.Param_Value
    , o.Param_Cast
    , DATE
    , USER
    , TIME
    , CURRENT_TIMESTAMP(6) -  INTERVAL '0.000001' second /*Change Delete Trigger and change update_ts - 0.000001 second*/
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST_TRIG_D </sc-trigger> */

--DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
beneficiaryInstnCode,
beneficiaryInstnName,
beneficiaryInstnCity,
beneficiaryInstnCountry,
beneficiaryInstnBranchBSB,
beneficiaryInstnBranchId,
beneficiaryInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_BENE_INST',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.beneficiaryInstnCode,
n.beneficiaryInstnName,
n.beneficiaryInstnCity,
n.beneficiaryInstnCountry,
n.beneficiaryInstnBranchBSB,
n.beneficiaryInstnBranchId,
n.beneficiaryInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> PDTRMEXTD.GENID_CHANNEL_CLASS </sc-trigger> */
--------------------------------------------------------------------------------

REPLACE TRIGGER PDTRMEXTD.GENID_CHANNEL_CLASS
AFTER INSERT ON PDTRMCORE.EX_CHANNEL_CLASS
	REFERENCING NEW AS NEWROW 
	FOR EACH ROW
(

LOCKING TABLE PDTRMCORE.EX_CHANNEL_CLASS FOR WRITE
/* update sequence number */
UPDATE A 
FROM PDTRMEXTD.TRM_SEQ A
SET 	SEQ_NO = A.SEQ_NO + 1
WHERE	A.SEQ_TYPE_CD = 'CHANNEL_CLASS'
; 

/* set Channel_Class */
UPDATE A
FROM PDTRMCORE.EX_CHANNEL_CLASS AS A
, (   SELECT 
        SEQ_NO
	FROM PDTRMEXTD.TRM_SEQ
	WHERE 
      SEQ_TYPE_CD = 'CHANNEL_CLASS'
  )   AS B  
SET 	Channel_Class = B.SEQ_NO   
WHERE 	A.CHANNEL_CLASS_ID = NEWROW.CHANNEL_CLASS_ID;
   
);

--
/* <sc-trigger> P_P00_D_TCF_001_LKP_0.BMAP_TU_Domain </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER P_P00_D_TCF_001_LKP_0.BMAP_TU_Domain
AFTER UPDATE ON P_P00_D_TCF_001_LKP_0.BMAP_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_LKP_0.BMAP_T_Domain_Log (
      Code_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_System </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_System
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_System_Log (
      Ctl_Id
    , System_Name
    , Path_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.System_Name
    , n.Path_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */

Replace TRIGGER U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_AUDT
(stmt_line_sk
,unique_id
,rprt_name
,acct_no
,acct_name
,acct_cncy
,acct_baln_pool
,date_tag_created
,value_date
,amt
,txn_type
,internal_ref
,external_ref
,description
,swf_file_ref
,mtch_type
,team_resp
,investigation_stus
,tran_catg
,is_reviewed
,othr_cmmt
,busn_days
,ifti_rprt
,subm_date
,subm_file_name
,subm_rcpt_no
,imt_mtch_grup_id
,imt_refn_1
,imt_refn_2
,imt_refn_3
,pms_ref_no
,pms_dbt_name
,pms_sbk_ref_no
,wss_srce
,wss_portfolio
,wss_audt_note
,wss_net_info
,wss_area
,itc_case_no
,wss_extl_deal_numb
,action_date
,EXC_OFCR
,EXC_OFCR_DTTM
,VERF_OFCR_CMMT
,VERF_OFCR
,VERF_OFCR_DTTM
,QA_OFCR_CMMT
,QA_OFCR
,QA_OFCR_DTTM
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(n.stmt_line_sk
,n.unique_id
,n.rprt_name
,n.acct_no
,n.acct_name
,n.acct_cncy
,n.acct_baln_pool
,n.date_tag_created
,n.value_date
,n.amt
,n.txn_type
,n.internal_ref
,n.external_ref
,n.description
,n.swf_file_ref
,n.mtch_type
,n.team_resp
,n.investigation_stus
,n.tran_catg
,n.is_reviewed
,n.othr_cmmt
,n.busn_days
,n.ifti_rprt
,n.subm_date
,n.subm_file_name
,n.subm_rcpt_no
,n.imt_mtch_grup_id
,n.imt_refn_1
,n.imt_refn_2
,n.imt_refn_3
,n.pms_ref_no
,n.pms_dbt_name
,n.pms_sbk_ref_no
,n.wss_srce
,n.wss_portfolio
,n.wss_audt_note
,n.wss_net_info
,n.wss_area
,n.itc_case_no
,n.wss_extl_deal_numb
,n.action_date
,n.EXC_OFCR
,n.EXC_OFCR_DTTM
,n.VERF_OFCR_CMMT
,n.VERF_OFCR
,n.VERF_OFCR_DTTM
,n.QA_OFCR_CMMT
,n.QA_OFCR
,n.QA_OFCR_DTTM
,USER
,Current_Timestamp(6)
,'INSERT' 
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFWX_TU_Xfm_Process </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFWX_TU_Xfm_Process
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process
REFERENCING NEW ROW AS NR
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process_Log (
      Process_Name,           
      Busn_Dt ,         
      Xfm_Run_Dt ,                        
      Xfm_Rows_Inserted ,                       
      Xfm_Stg_Ld_Status ,           
      Xfm_Stg_Ld_Start_Time ,
      Xfm_Stg_Ld_End_Time ,   
      Xfm_Stg_Val_Start_Time ,
      Xfm_Stg_Val_End_Time ,  
      Msur_Mdl_Pub_Run_Dt ,         
      Msur_Mdl_Pub_Process_Id ,           
      Msur_Mdl_Pub_Start_Time ,           
      Msur_Mdl_Pub_End_Time ,
      Msur_Mdl_Pub_Status ,   
      Msur_Mdl_Pub_Priority , 
      Pub_Stream_Key ,
      Update_User ,           
      Update_Ts 
  ) VALUES (
      NR.Process_Name,        
      NR.Busn_Dt ,            
      NR.Xfm_Run_Dt ,                     
      NR.Xfm_Rows_Inserted ,                    
      NR.Xfm_Stg_Ld_Status ,        
      NR.Xfm_Stg_Ld_Start_Time ,
      NR.Xfm_Stg_Ld_End_Time ,
      NR.Xfm_Stg_Val_Start_Time ,
      NR.Xfm_Stg_Val_End_Time ,     
      NR.Msur_Mdl_Pub_Run_Dt ,            
      NR.Msur_Mdl_Pub_Process_Id ,        
      NR.Msur_Mdl_Pub_Start_Time ,        
      NR.Msur_Mdl_Pub_End_Time ,
      NR.Msur_Mdl_Pub_Status ,      
      NR.Msur_Mdl_Pub_Priority ,    
      NR.Pub_Stream_Key ,
      NR.Update_User ,        
      NR.Update_Ts 
  );
);

--
/* <sc-trigger> D_D00_D_TCF_001_LKP_0.BMAP_TI_Domain </sc-trigger> */


REPLACE TRIGGER D_D00_D_TCF_001_LKP_0.BMAP_TI_Domain
AFTER INSERT ON D_D00_D_TCF_001_LKP_0.BMAP_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_LKP_0.BMAP_T_Domain_Log (
      Code_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_Hist
  (
     Cand_Obj_Id
    ,Related_Cand_Obj_Id
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Id
    ,n.Related_Cand_Obj_Id
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_TTN_REML_ACCT_AGGR_HIST_UPD </sc-trigger> */


/*CREATE UPD TRIGGER FOR  TTN_REML_ACCT_AGGR_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_TTN_REML_ACCT_AGGR_HIST_UPD
AFTER UPDATE ON U_D_DSV_001_REM_1.TTN_REML_ACCT_AGGR_HIST
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.TTN_REML_ACCT_AGGR_HIST_LOG  SELECT A.*,USER,'UPD', CURRENT_TIMESTAMP  FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Load </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Load
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Load
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Load_Log 
 (
  Ctl_Id
  ,File_Id
  ,Business_Date
  ,Business_Date_Cycle_Num
  ,Business_Date_Cycle_Start_Ts
  ,File_Qualifier
  ,Process_Name
  ,Process_Id
  ,TD_Session_Id
  ,Tool_Session_Id
  ,S_Tbl_Name
  ,S_DB_Name
  ,Rows_Input
  ,Rows_Considered
  ,Rows_Not_Considered
  ,Rows_Rejected
  ,Rows_Inserted
  ,Rows_Updated
  ,Rows_Deleted
  ,Rows_ET
  ,Rows_UV
  ,Run_Date
  ,Start_Time
  ,End_Time
  ,Load_Status
  ,Update_Date
  ,Update_User
  ,Update_Time
  ,Update_Ts
   ) 
  VALUES 
   (
  n.Ctl_Id
  ,n.File_Id
  ,n.Business_Date
  ,n.Business_Date_Cycle_Num
  ,n.Business_Date_Cycle_Start_Ts
  ,n.File_Qualifier
  ,n.Process_Name
  ,n.Process_Id
  ,n.TD_Session_Id
  ,n.Tool_Session_Id
  ,n.S_Tbl_Name
  ,n.S_DB_Name
  ,n.Rows_Input
  ,n.Rows_Considered
  ,n.Rows_Not_Considered
  ,n.Rows_Rejected
  ,n.Rows_Inserted
  ,n.Rows_Updated
  ,n.Rows_Deleted
  ,n.Rows_ET
  ,n.Rows_UV
  ,n.Run_Date
  ,n.Start_Time
  ,n.End_Time
  ,n.Load_Status
  ,n.Update_Date
  ,n.Update_User
  ,n.Update_Time
  ,n.Update_Ts
   );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_System_File_Extract </sc-trigger> */
REPLACE  TRIGGER PDGENTCF.CTLFW_TU_System_File_Extract
AFTER UPDATE ON PDGENTCF.CTLFW_T_System_File_Extract
REFERENCING NEW ROW AS n
FOR EACH ROW (
INSERT INTO PDGENTCF.CTLFW_T_SFE_Log (
      Ctl_Id
    , File_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , File_Qualifier
    , Data_File_Name
    , Ctl_File_Name
    , Extraction_Date
    , Ext_Start_Time
    , Ext_End_Time
    , TD_Session_Id
    , Tool_Session_Id
    , Load_Status
    , Count_Source
    , Count_Target
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.File_Qualifier
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Extraction_Date
    , n.Ext_Start_Time
    , n.Ext_End_Time
    , n.TD_Session_Id
    , n.Tool_Session_Id
    , n.Load_Status
    , n.Count_Source
    , n.Count_Target
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Step_Parm_AUT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Step_Parm_AUT
  AFTER UPDATE ON PDGDWPIIOBF.Dtf_Job_Step_Parm
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Step_Parm_Hst
  (
     Job_Cd
    ,Job_Step_No
    ,Job_Step_Seq_No
    ,Job_Step_Parm
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Job_Step_No
    ,n.Job_Step_Seq_No
    ,n.Job_Step_Parm
    ,'U'
   );

--
/* <sc-trigger> PDGEN_REF.BKEY_TU_Domain </sc-trigger> */
REPLACE TRIGGER PDGEN_REF.BKEY_TU_Domain
AFTER UPDATE ON PDGEN_REF.BKEY_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGEN_REF.BKEY_T_Domain_Log (
      Key_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> D_S00_D_TCF_001_LKP_0.BMAP_TU_Domain </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_LKP_0.BMAP_TU_Domain
AFTER UPDATE ON D_S00_D_TCF_001_LKP_0.BMAP_T_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_LKP_0.BMAP_T_Domain_Log (
      Code_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Code_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Rule_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Rule_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Obj_Rule
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Rule_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Enabled_Flag
    ,Repl_Type_Cd
    ,Predicate_Clause_Text
    ,Force_Flag
    ,Key_Column
    ,Obj_Size_Type_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Db_Name
    ,o.Obj_Name
    ,o.Obj_Type
    ,o.Enabled_Flag
    ,o.Repl_Type_Cd
    ,o.Predicate_Clause_Text
    ,o.Force_Flag
    ,o.Key_Column
    ,o.Obj_Size_Type_Cd
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Stream_Param </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Stream_Param
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
      Stream_Key
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST_TRIG_U </sc-trigger> */


--update
--DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
beneficiaryInstnCode,
beneficiaryInstnName,
beneficiaryInstnCity,
beneficiaryInstnCountry,
beneficiaryInstnBranchBSB,
beneficiaryInstnBranchId,
beneficiaryInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_BENE_INST',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.beneficiaryInstnCode,
n.beneficiaryInstnName,
n.beneficiaryInstnCity,
n.beneficiaryInstnCountry,
n.beneficiaryInstnBranchBSB,
n.beneficiaryInstnBranchId,
n.beneficiaryInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> PDTRMEXTD.GENID_CM_COMMUNICATION </sc-trigger> */
REPLACE TRIGGER PDTRMEXTD.GENID_CM_COMMUNICATION
AFTER INSERT ON PDTRMCORE.CM_COMMUNICATION
 REFERENCING NEW AS NEWROW
 FOR EACH ROW
(
 -- Update EX_COMMUNICATION with a new / updated version id assuming there's an EX_COMMUNICATION row to work with
 EXEC PDTRMEXTD.M_GENID_COMMUNICATION(NEWROW.Communication_Id);
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Load </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Load
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Load
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Load_Log 
 (
  Ctl_Id
  ,File_Id
  ,Business_Date
  ,Business_Date_Cycle_Num
  ,Business_Date_Cycle_Start_Ts
  ,File_Qualifier
  ,Process_Name
  ,Process_Id
  ,TD_Session_Id
  ,Tool_Session_Id
  ,S_Tbl_Name
  ,S_DB_Name
  ,Rows_Input
  ,Rows_Considered
  ,Rows_Not_Considered
  ,Rows_Rejected
  ,Rows_Inserted
  ,Rows_Updated
  ,Rows_Deleted
  ,Rows_ET
  ,Rows_UV
  ,Run_Date
  ,Start_Time
  ,End_Time
  ,Load_Status
  ,Update_Date
  ,Update_User
  ,Update_Time
  ,Update_Ts
   ) 
  VALUES 
   (
  n.Ctl_Id
  ,n.File_Id
  ,n.Business_Date
  ,n.Business_Date_Cycle_Num
  ,n.Business_Date_Cycle_Start_Ts
  ,n.File_Qualifier
  ,n.Process_Name
  ,n.Process_Id
  ,n.TD_Session_Id
  ,n.Tool_Session_Id
  ,n.S_Tbl_Name
  ,n.S_DB_Name
  ,n.Rows_Input
  ,n.Rows_Considered
  ,n.Rows_Not_Considered
  ,n.Rows_Rejected
  ,n.Rows_Inserted
  ,n.Rows_Updated
  ,n.Rows_Deleted
  ,n.Rows_ET
  ,n.Rows_UV
  ,n.Run_Date
  ,n.Start_Time
  ,n.End_Time
  ,n.Load_Status
  ,n.Update_Date
  ,n.Update_User
  ,n.Update_Time
  ,n.Update_Ts
   );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYE_TRIG_D </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYE_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYE
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payeeSameAsSwiftBenCust,
payeeFullName,
payeeAddr,
payeeSuburb,
payeeState,
payeePostcode,
payeeCountry,
payeeAccountBSB1,
payeeAccountNumber1,
payeeAccountBSB2,
payeeAccountNumber2,
payeeAccountBSB3,
payeeAccountNumber3,
payeeIdentificationType1,
payeeIdentificationTypeOther1,
payeeIdentificationNumber1,
payeeIdentificationIssuer1,
payeeIdentificationCountry1,
payeeIdentificationType2,
payeeIdentificationTypeOther2,
payeeIdentificationNumber2,
payeeIdentificationIssuer2,
payeeIdentificationCountry2,
payeeIdentificationType3,
payeeIdentificationTypeOther3,
payeeIdentificationNumber3,
payeeIdentificationIssuer3,
payeeIdentificationCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_PAYE',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payeeSameAsSwiftBenCust,
n.payeeFullName,
n.payeeAddr,
n.payeeSuburb,
n.payeeState,
n.payeePostcode,
n.payeeCountry,
n.payeeAccountBSB1,
n.payeeAccountNumber1,
n.payeeAccountBSB2,
n.payeeAccountNumber2,
n.payeeAccountBSB3,
n.payeeAccountNumber3,
n.payeeIdentificationType1,
n.payeeIdentificationTypeOther1,
n.payeeIdentificationNumber1,
n.payeeIdentificationIssuer1,
n.payeeIdentificationCountry1,
n.payeeIdentificationType2,
n.payeeIdentificationTypeOther2,
n.payeeIdentificationNumber2,
n.payeeIdentificationIssuer2,
n.payeeIdentificationCountry2,
n.payeeIdentificationType3,
n.payeeIdentificationTypeOther3,
n.payeeIdentificationNumber3,
n.payeeIdentificationIssuer3,
n.payeeIdentificationCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_System_File_Extract </sc-trigger> */
--------------------------------------------------------------------------------


CREATE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_System_File_Extract
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_System_File_Extract
REFERENCING NEW ROW AS n
FOR EACH ROW (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_SFE_Log (
      Ctl_Id
    , File_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , File_Qualifier
    , Data_File_Name
    , Ctl_File_Name
    , Extraction_Date
    , Ext_Start_Time
    , Ext_End_Time
    , TD_Session_Id
    , Tool_Session_Id
    , Load_Status
    , Count_Source
    , Count_Target
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.File_Qualifier
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Extraction_Date
    , n.Ext_Start_Time
    , n.Ext_End_Time
    , n.TD_Session_Id
    , n.Tool_Session_Id
    , n.Load_Status
    , n.Count_Source
    , n.Count_Target
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_Hist
  (
     Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Prog_Vers
    ,Proc_Id
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Enabled_Flag
    ,Prog_Beg_Ts
    ,Prog_End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Prog_Name
    ,n.Prog_Mode
    ,n.Prog_User
    ,n.Prog_Host
    ,n.Prog_Vers
    ,n.Proc_Id
    ,n.Prog_Stat_Cd
    ,n.Prog_Stat_Desc
    ,n.Enabled_Flag
    ,n.Prog_Beg_Ts
    ,n.Prog_End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_TTN_REML_UPDT_PARN_LIST_HIST_UPD </sc-trigger> */


/*CREATE UPD TRIGGER FOR  TTN_REML_UPDT_PARN_LIST_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_TTN_REML_UPDT_PARN_LIST_HIST_UPD
AFTER UPDATE ON U_D_DSV_001_REM_1.TTN_REML_UPDT_PARN_LIST_HIST
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.TTN_REML_UPDT_PARN_LIST_HIST_LOG  SELECT A.*,USER,'UPD', CURRENT_TIMESTAMP  FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFWX_TI_Xfm_Process </sc-trigger> */
REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFWX_TI_Xfm_Process
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process
REFERENCING NEW ROW AS NR
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process_Log (
      Process_Name,           
      Busn_Dt ,         
      Xfm_Run_Dt ,                        
      Xfm_Rows_Inserted ,                       
      Xfm_Stg_Ld_Status ,           
      Xfm_Stg_Ld_Start_Time ,
      Xfm_Stg_Ld_End_Time ,         
      Xfm_Stg_Val_Start_Time ,
      Xfm_Stg_Val_End_Time ,
      Msur_Mdl_Pub_Run_Dt ,         
      Msur_Mdl_Pub_Process_Id ,           
      Msur_Mdl_Pub_Start_Time ,           
      Msur_Mdl_Pub_End_Time ,
      Msur_Mdl_Pub_Status ,   
      Msur_Mdl_Pub_Priority , 
      Pub_Stream_Key ,
      Update_User ,           
      Update_Ts 
  ) VALUES (
      NR.Process_Name,        
      NR.Busn_Dt ,            
      NR.Xfm_Run_Dt ,                     
      NR.Xfm_Rows_Inserted ,                    
      NR.Xfm_Stg_Ld_Status ,        
      NR.Xfm_Stg_Ld_Start_Time ,
      NR.Xfm_Stg_Ld_End_Time ,      
      NR.Xfm_Stg_Val_Start_Time ,
      NR.Xfm_Stg_Val_End_Time ,           
      NR.Msur_Mdl_Pub_Run_Dt ,            
      NR.Msur_Mdl_Pub_Process_Id ,        
      NR.Msur_Mdl_Pub_Start_Time ,        
      NR.Msur_Mdl_Pub_End_Time ,
      NR.Msur_Mdl_Pub_Status ,      
      NR.Msur_Mdl_Pub_Priority ,    
      NR.Pub_Stream_Key ,
      NR.Update_User ,        
      NR.Update_Ts 
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Process_Param </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Process_Param
AFTER UPDATE ON PDGENTCF.CTLFW_T_Process_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Process_Param_Log (
      Process_Name
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_AIT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_AIT
  AFTER INSERT ON PDGDWPIIOBF.Dtf_Job
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Hst
  (
     Job_Id
    ,Job_Strt_Dt
    ,Job_Strt_Tm
    ,Job_End_Dt
    ,Job_End_Tm
    ,Job_Cd
    ,Stus_Cd
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Job_Strt_Dt
    ,n.Job_Strt_Tm
    ,n.Job_End_Dt
    ,n.Job_End_Tm
    ,n.Job_Cd
    ,n.Stus_Cd
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_Hist
  (
     Obj
    ,Is_Current_Flag
    ,Is_Enabled_Flag
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Key_Type
    ,Key_Name
    ,Key_Sql
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Obj
    ,o.Is_Current_Flag
    ,o.Is_Enabled_Flag
    ,o.Db_Name
    ,o.Obj_Name
    ,o.Obj_Type
    ,o.Key_Type
    ,o.Key_Name
    ,o.Key_Sql
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Load </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Load
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Load
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Load_Log 
 (
  Ctl_Id
  ,File_Id
  ,Business_Date
  ,Business_Date_Cycle_Num
  ,Business_Date_Cycle_Start_Ts
  ,File_Qualifier
  ,Process_Name
  ,Process_Id
  ,TD_Session_Id
  ,Tool_Session_Id
  ,S_Tbl_Name
  ,S_DB_Name
  ,Rows_Input
  ,Rows_Considered
  ,Rows_Not_Considered
  ,Rows_Rejected
  ,Rows_Inserted
  ,Rows_Updated
  ,Rows_Deleted
  ,Rows_ET
  ,Rows_UV
  ,Run_Date
  ,Start_Time
  ,End_Time
  ,Load_Status
  ,Update_Date
  ,Update_User
  ,Update_Time
  ,Update_Ts
  ) 
VALUES 
 (
  n.Ctl_Id
  ,n.File_Id
  ,n.Business_Date
  ,n.Business_Date_Cycle_Num
  ,n.Business_Date_Cycle_Start_Ts
  ,n.File_Qualifier
  ,n.Process_Name
  ,n.Process_Id
  ,n.TD_Session_Id
  ,n.Tool_Session_Id
  ,n.S_Tbl_Name
  ,n.S_DB_Name
  ,n.Rows_Input
  ,n.Rows_Considered
  ,n.Rows_Not_Considered
  ,n.Rows_Rejected
  ,n.Rows_Inserted
  ,n.Rows_Updated
  ,n.Rows_Deleted
  ,n.Rows_ET
  ,n.Rows_UV
  ,n.Run_Date
  ,n.Start_Time
  ,n.End_Time
  ,n.Load_Status
  ,n.Update_Date
  ,n.Update_User
  ,n.Update_Time
  ,n.Update_Ts
 );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE_TRIG_I </sc-trigger> */


-----------------------------------------------------------------------------------------------
 
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE
 --------------------------------------------------------------------- PAYR
 --DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payeeSameAsSwiftBenCust,
payeeFullName,
payeeAddr,
payeeSuburb,
payeeState,
payeePostcode,
payeeCountry,
payeeAccountBSB1,
payeeAccountNumber1,
payeeAccountBSB2,
payeeAccountNumber2,
payeeAccountBSB3,
payeeAccountNumber3,
payeeIdentificationType1,
payeeIdentificationTypeOther1,
payeeIdentificationNumber1,
payeeIdentificationIssuer1,
payeeIdentificationCountry1,
payeeIdentificationType2,
payeeIdentificationTypeOther2,
payeeIdentificationNumber2,
payeeIdentificationIssuer2,
payeeIdentificationCountry2,
payeeIdentificationType3,
payeeIdentificationTypeOther3,
payeeIdentificationNumber3,
payeeIdentificationIssuer3,
payeeIdentificationCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_PAYE',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payeeSameAsSwiftBenCust,
n.payeeFullName,
n.payeeAddr,
n.payeeSuburb,
n.payeeState,
n.payeePostcode,
n.payeeCountry,
n.payeeAccountBSB1,
n.payeeAccountNumber1,
n.payeeAccountBSB2,
n.payeeAccountNumber2,
n.payeeAccountBSB3,
n.payeeAccountNumber3,
n.payeeIdentificationType1,
n.payeeIdentificationTypeOther1,
n.payeeIdentificationNumber1,
n.payeeIdentificationIssuer1,
n.payeeIdentificationCountry1,
n.payeeIdentificationType2,
n.payeeIdentificationTypeOther2,
n.payeeIdentificationNumber2,
n.payeeIdentificationIssuer2,
n.payeeIdentificationCountry2,
n.payeeIdentificationType3,
n.payeeIdentificationTypeOther3,
n.payeeIdentificationNumber3,
n.payeeIdentificationIssuer3,
n.payeeIdentificationCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Stream_BusDate </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Stream_BusDate
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Business_Date
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Business_Date
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */




REPLACE TRIGGER U_D_DSV_001_NIT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.VOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated
)
VALUES
(
 'VOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'DELETE'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Process_Param </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Process_Param
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Param_Log (
      Process_Name
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_DM_Stus_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_DM_Stus_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDM_DM_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_DM_Stus_Hist
  (
     ResourceId
    ,UOWId
    ,Job_CD
    ,Job_Stus_Cd
    ,Start_TS
    ,End_TS
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.ResourceId
    ,n.UOWId
    ,n.Job_CD
    ,n.Job_Stus_Cd
    ,n.Start_TS
    ,n.End_TS
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_REML_PAYT_DETL_HIST_DEL </sc-trigger> */

/*CREATE DEL TRIGGER FOR  REML_PAYT_DETL_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_REML_PAYT_DETL_HIST_DEL 
AFTER DELETE ON U_D_DSV_001_REM_1.REML_PAYT_DETL_HIST 
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.REML_PAYT_DETL_HIST_LOG  SELECT A.*,USER,'DEL', CURRENT_TIMESTAMP FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Process_Type_Param </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Process_Type_Param
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param_Log (
      Process_Type
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Process_Id </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Process_Id
AFTER INSERT ON PDGENTCF.CTLFW_T_Process_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Process_Id_Log (
      Process_Name
    , Process_Id
    , Process_State
    , Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Process_Id
    , n.Process_State
    , n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Cd_AIT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Cd_AIT
  AFTER INSERT ON PDGDWPIIOBF.Dtf_Job_Cd
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Cd_Hst
  (
     Job_Cd
    ,Tgt_Databasename
 ,Databasename
    ,ObjectName
    ,ObjectType
    ,CopyStat
 ,CopyData
    ,CompareDDL
 ,TruncateTgtTable
    ,WhereClause
    ,KeyColumn
    ,Query_Band
    ,Job_Dscrp
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Tgt_Databasename
 ,n.Databasename
    ,n.ObjectName
    ,n.ObjectType
    ,n.CopyStat
 ,n.CopyData
    ,n.CompareDDL
 ,n.TruncateTgtTable
    ,n.WhereClause
    ,n.KeyColumn
    ,n.Query_Band
    ,n.Job_Dscrp
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Sys_Name
    ,Update_Tally
    ,Elapsed_Secs
    ,Key_Type1
    ,Key_Type2
    ,Key_Type3
    ,Key_Value
    ,Key_Where_Flag
    ,Key_Where_Val
    ,Key_Where_Cond
    ,Key_Partition_Flag
    ,Key_Partition_Val
    ,Key_Partition_Col
    ,Prev_Create_Date
    ,Prev_Create_Ts
    ,Is_Current_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Sys_Name
    ,n.Update_Tally
    ,n.Elapsed_Secs
    ,n.Key_Type1
    ,n.Key_Type2
    ,n.Key_Type3
    ,n.Key_Value
    ,n.Key_Where_Flag
    ,n.Key_Where_Val
    ,n.Key_Where_Cond
    ,n.Key_Partition_Flag
    ,n.Key_Partition_Val
    ,n.Key_Partition_Col
    ,n.Prev_Create_Date
    ,n.Prev_Create_Ts
    ,n.Is_Current_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_System_File </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_System_File
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_System_File
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_System_File_Log (
      Ctl_Id
    , File_Id
    , Data_File_Name
    , Ctl_File_Name
    , Queue_Name
    , Description
    , Data_File_Suffix
    , Ctl_File_Suffix
    , Data_File_Type
    , ET_Tolerance
    , UV_Tolerance
    , Sessions
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Queue_Name
    , n.Description
    , n.Data_File_Suffix
    , n.Ctl_File_Suffix
    , n.Data_File_Type
    , n.ET_Tolerance
    , n.UV_Tolerance
    , n.Sessions
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE_TRIG_D </sc-trigger> */

--DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE
REFERENCING OLD ROW AS n
FOR EACH ROW
WHEN (n.UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
filename,
txnRefNo ,
interceptFlag ,
isSwiftOrStructured,
swiftMsg ,
transferDate,
direction,
currency ,
currencyAmount,
valueDate ,    
RPRT_STUS ,
REVW_MODF_BY,
REVW_DTTM ,
SUPV_MODF_BY,
SUPV_DTTM ,
CRET_BY,
CRET_DTTM ,
INDA_DRFT ,
INDA_REDY,
INDA_APRD,
INDA_RWRK,
INDA_NOT_RPRT,
EXC_REAS ,
ENUS_CMMT ,
SDS_MSG_ID ,
SDS_MSG_SEQ,
base_transferDate,
base_direction,
base_currency ,
base_currencyAmount,
base_valueDate , 
INDA_PAYR,
INDA_PAYE,
INDA_ORDR_INST,
INDA_SEND_INST,
INDA_SEND_CORR,
INDA_BENE_INST,
INDA_RECE_INST,
INDA_RECE_CORR,
INDA_ADDN_DETL,
LINK_KEY       
)
VALUES
(
'IFTI_XML_REVW_BASE',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.filename,
n.txnRefNo ,
n.interceptFlag ,
n.isSwiftOrStructured,
n.swiftMsg ,
n.transferDate,
n.direction,
n.currency ,
n.currencyAmount,
n.valueDate ,    
n.RPRT_STUS ,
n.REVW_MODF_BY,
n.REVW_DTTM ,
n.SUPV_MODF_BY,
n.SUPV_DTTM ,
n.CRET_BY,
n.CRET_DTTM ,
n.INDA_DRFT ,
n.INDA_REDY,
n.INDA_APRD,
n.INDA_RWRK,
n.INDA_NOT_RPRT,
n.EXC_REAS ,
n.ENUS_CMMT ,
n.SDS_MSG_ID ,
n.SDS_MSG_SEQ,
n.base_transferDate,
n.base_direction,
n.base_currency ,
n.base_currencyAmount,
n.base_valueDate , 
n.INDA_PAYR,
n.INDA_PAYE,
n.INDA_ORDR_INST,
n.INDA_SEND_INST,
n.INDA_SEND_CORR,
n.INDA_BENE_INST,
n.INDA_RECE_INST,
n.INDA_RECE_CORR,
n.INDA_ADDN_DETL,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_System_File </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_System_File
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_System_File
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_System_File_Log (
      Ctl_Id
    , File_Id
    , Data_File_Name
    , Ctl_File_Name
    , Queue_Name
    , Description
    , Data_File_Suffix
    , Ctl_File_Suffix
    , Data_File_Type
    , ET_Tolerance
    , UV_Tolerance
    , Sessions
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Queue_Name
    , n.Description
    , n.Data_File_Suffix
    , n.Ctl_File_Suffix
    , n.Data_File_Type
    , n.ET_Tolerance
    , n.UV_Tolerance
    , n.Sessions
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_BENE_INST_TRIG_D </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_BENE_INST_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_BENE_INST
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
beneficiaryInstnCode,
beneficiaryInstnName,
beneficiaryInstnCity,
beneficiaryInstnCountry,
beneficiaryInstnBranchBSB,
beneficiaryInstnBranchId,
beneficiaryInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_BENE_INST',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.beneficiaryInstnCode,
n.beneficiaryInstnName,
n.beneficiaryInstnCity,
n.beneficiaryInstnCountry,
n.beneficiaryInstnBranchBSB,
n.beneficiaryInstnBranchId,
n.beneficiaryInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Stream </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Stream
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Stream
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Stream_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Stream_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Data_Purge_Log_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Data_Purge_Log_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Data_Purge_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Data_Purge_Log_Hist
  (
     Purge_Db
    ,Purge_Tbl
    ,Log_Date
    ,Start_Time
    ,Stop_Time
    ,Rows_Returned
    ,Status
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Purge_Db
    ,n.Purge_Tbl
    ,n.Log_Date
    ,n.Start_Time
    ,n.Stop_Time
    ,n.Rows_Returned
    ,n.Status
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_TTN_REML_CUST_AGGR_SMRY_HIST_UPD </sc-trigger> */


/*CREATE UPD TRIGGER FOR  TTN_REML_CUST_AGGR_SMRY_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_TTN_REML_CUST_AGGR_SMRY_HIST_UPD
AFTER UPDATE ON U_D_DSV_001_REM_1.TTN_REML_CUST_AGGR_SMRY_HIST
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.TTN_REML_CUST_AGGR_SMRY_HIST_LOG  SELECT A.*,USER,'UPD', CURRENT_TIMESTAMP  FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Transform_KeyCol </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Transform_KeyCol
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,Key_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.Key_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Transform </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Transform
AFTER UPDATE ON PDGENTCF.CTLFW_T_Transform
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Transform_Log 
 (
 Process_Name
 ,Business_Date
 ,Business_Date_Cycle_Num
 ,Business_Date_Cycle_Start_Ts
 ,Process_Id
 ,TD_Session_Id
 ,Tool_Session_Id
 ,In_DB_Name
 ,In_Object_Name
 ,Out_DB_Name
 ,Out_Object_Name
 ,Rows_Inserted
 ,Rows_Updated
 ,Rows_Deleted
 ,Run_Date
 ,Start_Time
 ,End_Time
 ,Update_Date
 ,Update_User
 ,Update_Time
 ,Update_Ts
   ) 
  VALUES 
   (
 n.Process_Name
 ,n.Business_Date
 ,n.Business_Date_Cycle_Num
 ,n.Business_Date_Cycle_Start_Ts
 ,n.Process_Id
 ,n.TD_Session_Id
 ,n.Tool_Session_Id
 ,n.In_DB_Name
 ,n.In_Object_Name
 ,n.Out_DB_Name
 ,n.Out_Object_Name
 ,n.Rows_Inserted
 ,n.Rows_Updated
 ,n.Rows_Deleted
 ,n.Run_Date
 ,n.Start_Time
 ,n.End_Time
 ,n.Update_Date
 ,n.Update_User
 ,n.Update_Time
 ,n.Update_Ts
   );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr_AUT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr_AUT
  AFTER UPDATE ON PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr_Hst
  (
     RfshGroupNm
 ,TgtDatabaseNm
 ,SrcDatabaseNm
 ,SrcTableNm
 ,SrcTableCurrPerm
 ,SrcTablePredicateInfo
 ,SrcKeyColumn
 ,SelectionCriteriaRuleNbr
 ,SuggestedUtility
 ,TruncateTgtTable
 ,RfshGroupEntryTimestamp
 ,RfshGroupEntryStatus
    ,Modify_Action
   ) VALUES (
     n.RfshGroupNm
 ,n.TgtDatabaseNm
 ,n.SrcDatabaseNm
 ,n.SrcTableNm
 ,n.SrcTableCurrPerm
 ,n.SrcTablePredicateInfo
 ,n.SrcKeyColumn
 ,n.SelectionCriteriaRuleNbr
 ,n.SuggestedUtility
 ,n.TruncateTgtTable
 ,n.RfshGroupEntryTimestamp
 ,n.RfshGroupEntryStatus
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDM_Job
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Hist
  (
     Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.UOW_Id
    ,o.Job_Priority
    ,o.Job_Type
    ,o.Job_Source
    ,o.Job_Description
    ,o.Source_TDPID
    ,o.Source_User
    ,o.Source_Passwd_Encrypted
    ,o.Source_Logon_Mechanism
    ,o.Source_Logon_Mechanism_Data
    ,o.Source_Userid_Pool
    ,o.Source_Account_Id
    ,o.Source_Sessions
    ,o.Target_TDPID
    ,o.Target_User
    ,o.Target_Passwd_Encrypted
    ,o.Target_Logon_Mechanism
    ,o.Target_Logon_Mechanism_Data
    ,o.Target_Userid_Pool
    ,o.Target_Account_Id
    ,o.Target_Sessions
    ,o.Data_Streams
    ,o.Response_Timeout
    ,o.Overwrite_Existing_Obj
    ,o.Force_Utility
    ,o.Log_Level
    ,o.Online_Archive
    ,o.Freeze_Job_Steps
    ,o.Max_Agents_Per_Task
    ,o.Additional_ARC_Parameters
    ,o.Query_Band_Str
    ,o.Sync_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Transform </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Transform
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Transform
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Transform_Log 
 (
 Process_Name
 ,Business_Date
 ,Business_Date_Cycle_Num
 ,Business_Date_Cycle_Start_Ts
 ,Process_Id
 ,TD_Session_Id
 ,Tool_Session_Id
 ,In_DB_Name
 ,In_Object_Name
 ,Out_DB_Name
 ,Out_Object_Name
 ,Rows_Inserted
 ,Rows_Updated
 ,Rows_Deleted
 ,Run_Date
 ,Start_Time
 ,End_Time
 ,Update_Date
 ,Update_User
 ,Update_Time
 ,Update_Ts
   ) 
  VALUES 
   (
 n.Process_Name
 ,n.Business_Date
 ,n.Business_Date_Cycle_Num
 ,n.Business_Date_Cycle_Start_Ts
 ,n.Process_Id
 ,n.TD_Session_Id
 ,n.Tool_Session_Id
 ,n.In_DB_Name
 ,n.In_Object_Name
 ,n.Out_DB_Name
 ,n.Out_Object_Name
 ,n.Rows_Inserted
 ,n.Rows_Updated
 ,n.Rows_Deleted
 ,n.Run_Date
 ,n.Start_Time
 ,n.End_Time
 ,n.Update_Date
 ,n.Update_User
 ,n.Update_Time
 ,n.Update_Ts
   );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */






-- Bankwest  Credit Insert Trigger
--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_BWA_CR_TRIG_I
replace trigger U_D_DSV_001_NFT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_I
after insert on U_D_DSV_001_NFT_1.NOST_BWA_CR_EXC_RPRT_ACCESS
referencing new row as n
for each row
insert into U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
values
(
 'NOST_BWA_CR_EXC_RPRT_ACCESS'
,user
,current_timestamp(6)
,'INSERT'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TD_Stream_Param </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TD_Stream_Param
AFTER DELETE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING OLD ROW AS O  
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
    Stream_Key
    , Param_Group
    , Param_Name
    , Description
    , Param_Value
    , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
    o.Stream_Key
    , o.Param_Group
    , o.Param_Name
    , o.Description
    , o.Param_Value
    , o.Param_Cast
    , DATE
    , USER
    , TIME
    , CURRENT_TIMESTAMP(6) -  INTERVAL '0.000001' second /*Change Delete Trigger and change update_ts - 0.000001 second*/
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_BENE_INST_TRIG_U </sc-trigger> */


--update
--DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_BENE_INST_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_BENE_INST_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_BENE_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
beneficiaryInstnCode,
beneficiaryInstnName,
beneficiaryInstnCity,
beneficiaryInstnCountry,
beneficiaryInstnBranchBSB,
beneficiaryInstnBranchId,
beneficiaryInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_BENE_INST',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.beneficiaryInstnCode,
n.beneficiaryInstnName,
n.beneficiaryInstnCity,
n.beneficiaryInstnCountry,
n.beneficiaryInstnBranchBSB,
n.beneficiaryInstnBranchId,
n.beneficiaryInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFWX_TI_Xfm_Process </sc-trigger> */
REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFWX_TI_Xfm_Process
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process
REFERENCING NEW ROW AS NR
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process_Log (
      Process_Name,           
      Busn_Dt ,         
      Xfm_Run_Dt ,                        
      Xfm_Rows_Inserted ,                       
      Xfm_Stg_Ld_Status ,           
      Xfm_Stg_Ld_Start_Time ,
      Xfm_Stg_Ld_End_Time ,         
      Xfm_Stg_Val_Start_Time ,
      Xfm_Stg_Val_End_Time ,
      Msur_Mdl_Pub_Run_Dt ,         
      Msur_Mdl_Pub_Process_Id ,           
      Msur_Mdl_Pub_Start_Time ,           
      Msur_Mdl_Pub_End_Time ,
      Msur_Mdl_Pub_Status ,   
      Msur_Mdl_Pub_Priority , 
      Pub_Stream_Key ,
      Update_User ,           
      Update_Ts 
  ) VALUES (
      NR.Process_Name,        
      NR.Busn_Dt ,            
      NR.Xfm_Run_Dt ,                     
      NR.Xfm_Rows_Inserted ,                    
      NR.Xfm_Stg_Ld_Status ,        
      NR.Xfm_Stg_Ld_Start_Time ,
      NR.Xfm_Stg_Ld_End_Time ,      
      NR.Xfm_Stg_Val_Start_Time ,
      NR.Xfm_Stg_Val_End_Time ,           
      NR.Msur_Mdl_Pub_Run_Dt ,            
      NR.Msur_Mdl_Pub_Process_Id ,        
      NR.Msur_Mdl_Pub_Start_Time ,        
      NR.Msur_Mdl_Pub_End_Time ,
      NR.Msur_Mdl_Pub_Status ,      
      NR.Msur_Mdl_Pub_Priority ,    
      NR.Pub_Stream_Key ,
      NR.Update_User ,        
      NR.Update_Ts 
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Alrt_Action_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Alrt_Action_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Alrt_Action
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Alrt_Action_Hist
  (
     ActionCode
    ,ActionDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.ActionCode
    ,n.ActionDesc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_REML_ACCT_AGGR_HIST_DEL </sc-trigger> */

/*CREATE DEL TRIGGER FOR  REML_ACCT_AGGR_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_REML_ACCT_AGGR_HIST_DEL 
AFTER DELETE ON U_D_DSV_001_REM_1.REML_ACCT_AGGR_HIST 
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.REML_ACCT_AGGR_HIST_LOG  SELECT A.*,USER,'DEL', CURRENT_TIMESTAMP FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Process_Param </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Process_Param
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Param_Log (
      Process_Name
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Load </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Load
AFTER INSERT ON PDGENTCF.CTLFW_T_Load
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Load_Log 
 (
  Ctl_Id
  ,File_Id
  ,Business_Date
  ,Business_Date_Cycle_Num
  ,Business_Date_Cycle_Start_Ts
  ,File_Qualifier
  ,Process_Name
  ,Process_Id
  ,TD_Session_Id
  ,Tool_Session_Id
  ,S_Tbl_Name
  ,S_DB_Name
  ,Rows_Input
  ,Rows_Considered
  ,Rows_Not_Considered
  ,Rows_Rejected
  ,Rows_Inserted
  ,Rows_Updated
  ,Rows_Deleted
  ,Rows_ET
  ,Rows_UV
  ,Run_Date
  ,Start_Time
  ,End_Time
  ,Load_Status
  ,Update_Date
  ,Update_User
  ,Update_Time
  ,Update_Ts
  ) 
VALUES 
 (
  n.Ctl_Id
  ,n.File_Id
  ,n.Business_Date
  ,n.Business_Date_Cycle_Num
  ,n.Business_Date_Cycle_Start_Ts
  ,n.File_Qualifier
  ,n.Process_Name
  ,n.Process_Id
  ,n.TD_Session_Id
  ,n.Tool_Session_Id
  ,n.S_Tbl_Name
  ,n.S_DB_Name
  ,n.Rows_Input
  ,n.Rows_Considered
  ,n.Rows_Not_Considered
  ,n.Rows_Rejected
  ,n.Rows_Inserted
  ,n.Rows_Updated
  ,n.Rows_Deleted
  ,n.Rows_ET
  ,n.Rows_UV
  ,n.Run_Date
  ,n.Start_Time
  ,n.End_Time
  ,n.Load_Status
  ,n.Update_Date
  ,n.Update_User
  ,n.Update_Time
  ,n.Update_Ts
 );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Stus_AIT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Stus_AIT
  AFTER INSERT ON PDGDWPIIOBF.Dtf_Job_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Stus_Hst
  (
     Stus_Cd
    ,Stus_Desn
    ,Modify_Action
   ) VALUES (
     n.Stus_Cd
    ,n.Stus_Desn
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Alrt_Rqst_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Alrt_Rqst_AIT
AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Alrt_Rqst
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO DBCMNGR.AlertRequest (
     ReqDate
    ,ReqTime
    ,JobName
    ,Description
    ,EventValue
    ,ActionCode
    ,RepeatPeriod
    ,Destination
    ,Message
  ) VALUES (
     n.ReqDate
    ,n.ReqTime
    ,n.JobName
    ,n.Description
    ,n.EventValue
    ,n.ActionCode
    ,n.RepeatPeriod
    ,n.Destination
    ,trim(n.Destination) || '_' || trim(n.Message)
  );
);

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Param_Group </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Param_Group
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Param_Group
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Param_Group_Log (
      Param_Group
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Param_Group
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST_TRIG_I </sc-trigger> */

-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST
 
 --------------------------------------------------------------------- 
 --DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receivingInstnCode,
receivingInstnName,
receivingInstnCity,
receivingInstnCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_INST',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receivingInstnCode,
n.receivingInstnName,
n.receivingInstnCity,
n.receivingInstnCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_CE_System </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_CE_System
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_CE_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_CE_System_Log (
      CE_Id
    , Ctl_Id
    , CE_Ctl_Rltnshp_Type
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Ctl_Id
    , n.CE_Ctl_Rltnshp_Type
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.tri_tbl_HS_Update_col02 </sc-trigger> */
Create Trigger U_D_DSV_001_NIT_1.tri_tbl_HS_Update_col02
AFTER UPDATE OF Col_02 on U_D_DSV_001_NIT_1.tbl_HS
REFERENCING OLD AS OldRow NEW AS NewRow
For Each Row
Insert U_D_DSV_001_NIT_1.tbl_HS_HIST (Col_Id, HIST_02) values (NewRow.Col_Id, NewRow.Col_02);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_System_File_Param </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_System_File_Param
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_System_File_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_System_File_Param_Log (
      Ctl_Id
 , File_Id
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
 , n.File_Id
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Repl_Type_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Repl_Type_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Repl_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Repl_Type_Hist
  (
     Repl_Type_Cd
    ,Repl_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Repl_Type_Cd
    ,n.Repl_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_TTN_REML_PRJC_MSTR_HIST_UPD </sc-trigger> */


/*CREATE UPD TRIGGER FOR  TTN_REML_PRJC_MSTR_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_TTN_REML_PRJC_MSTR_HIST_UPD
AFTER UPDATE ON U_D_DSV_001_REM_1.TTN_REML_PRJC_MSTR_HIST
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.TTN_REML_PRJC_MSTR_HIST_LOG  SELECT A.*,USER,'UPD', CURRENT_TIMESTAMP  FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Transform_SurrKeyCol </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Transform_SurrKeyCol
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Transform_SurrKeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Transform_SKCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,SurrKey_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.SurrKey_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Process </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Process
AFTER UPDATE ON PDGENTCF.CTLFW_T_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Process_Log (
      Process_Name
    , Description
    , Process_Type
    , Ctl_Id
    , Stream_Key
    , In_DB_Name
    , In_Object_Name
    , Out_DB_Name
    , Out_Object_Name
    , Target_TableDatabaseName
    , Target_TableName
    , Temp_DatabaseName
    , Key_Set_Id
    , Domain_Id
    , Code_Set_Id
    , Collect_Stats
    , Truncate_Target
    , Verification_Flag
    , File_Qualifier_Reset_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Description
    , n.Process_Type
    , n.Ctl_Id
    , n.Stream_Key
    , n.In_DB_Name
    , n.In_Object_Name
    , n.Out_DB_Name
    , n.Out_Object_Name
    , n.Target_TableDatabaseName
    , n.Target_TableName
    , n.Temp_DatabaseName
    , n.Key_Set_Id
    , n.Domain_Id
    , n.Code_Set_Id
    , n.Collect_Stats
    , n.Truncate_Target
    , n.Verification_Flag
    , n.File_Qualifier_Reset_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Stus_AUT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Stus_AUT
  AFTER UPDATE ON PDGDWPIIOBF.Dtf_Job_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Stus_Hst
  (
     Stus_Cd
    ,Stus_Desn
    ,Modify_Action
   ) VALUES (
     n.Stus_Cd
    ,n.Stus_Desn
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_SQLState_Class_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_SQLState_Class_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_SQLState_Class
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_SQLState_Class_Hist
  (
     ClassCode
    ,ClassDefinition
    ,ClassNote
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     o.ClassCode
    ,o.ClassDefinition
    ,o.ClassNote
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_TS
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Transform </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Transform
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Transform
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Transform_Log 
 (
 Process_Name
 ,Business_Date
 ,Business_Date_Cycle_Num
 ,Business_Date_Cycle_Start_Ts
 ,Process_Id
 ,TD_Session_Id
 ,Tool_Session_Id
 ,In_DB_Name
 ,In_Object_Name
 ,Out_DB_Name
 ,Out_Object_Name
 ,Rows_Inserted
 ,Rows_Updated
 ,Rows_Deleted
 ,Run_Date
 ,Start_Time
 ,End_Time
 ,Update_Date
 ,Update_User
 ,Update_Time
 ,Update_Ts
 ) 
VALUES 
 (
 n.Process_Name
 ,n.Business_Date
 ,n.Business_Date_Cycle_Num
 ,n.Business_Date_Cycle_Start_Ts
 ,n.Process_Id
 ,n.TD_Session_Id
 ,n.Tool_Session_Id
 ,n.In_DB_Name
 ,n.In_Object_Name
 ,n.Out_DB_Name
 ,n.Out_Object_Name
 ,n.Rows_Inserted
 ,n.Rows_Updated
 ,n.Rows_Deleted
 ,n.Run_Date
 ,n.Start_Time
 ,n.End_Time
 ,n.Update_Date
 ,n.Update_User
 ,n.Update_Time
 ,n.Update_Ts
 );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST_TRIG_D </sc-trigger> */

-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
orderingInstnCode,
orderingInstnName,
orderingInstnCity,
orderingInstnCountry,
orderingInstnBranchBSB,
orderingInstnBranchId,
orderingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ORDR_INST',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.orderingInstnCode,
n.orderingInstnName,
n.orderingInstnCity,
n.orderingInstnCountry,
n.orderingInstnBranchBSB,
n.orderingInstnBranchId,
n.orderingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Process </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Process
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Log (
      Process_Name
    , Description
    , Process_Type
    , Ctl_Id
    , Stream_Key
    , In_DB_Name
    , In_Object_Name
    , Out_DB_Name
    , Out_Object_Name
    , Target_TableDatabaseName
    , Target_TableName
    , Temp_DatabaseName
    , Key_Set_Id
    , Domain_Id
    , Code_Set_Id
    , Collect_Stats
    , Truncate_Target
    , Verification_Flag
    , File_Qualifier_Reset_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Description
    , n.Process_Type
    , n.Ctl_Id
    , n.Stream_Key
    , n.In_DB_Name
    , n.In_Object_Name
    , n.Out_DB_Name
    , n.Out_Object_Name
    , n.Target_TableDatabaseName
    , n.Target_TableName
    , n.Temp_DatabaseName
    , n.Key_Set_Id
    , n.Domain_Id
    , n.Code_Set_Id
    , n.Collect_Stats
    , n.Truncate_Target
    , n.Verification_Flag
    , n.File_Qualifier_Reset_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */
-- cba  debit Insert Trigger
--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_CBA_DR_TRIG_I
REPLACE TRIGGER U_D_DSV_001_NIT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.NOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'INSERT'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_System_File_Extract </sc-trigger> */
--------------------------------------------------------------------------------


CREATE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_System_File_Extract
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_System_File_Extract
REFERENCING NEW ROW AS n
FOR EACH ROW (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_SFE_Log (
      Ctl_Id
    , File_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , File_Qualifier
    , Data_File_Name
    , Ctl_File_Name
    , Extraction_Date
    , Ext_Start_Time
    , Ext_End_Time
    , TD_Session_Id
    , Tool_Session_Id
    , Load_Status
    , Count_Source
    , Count_Target
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.File_Qualifier
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Extraction_Date
    , n.Ext_Start_Time
    , n.Ext_End_Time
    , n.TD_Session_Id
    , n.Tool_Session_Id
    , n.Load_Status
    , n.Count_Source
    , n.Count_Target
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I_Src
    ,Max_Pros_Key_I_Tgt
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Nm
    ,n.Db_Nm
    ,n.Tbl_Nm
    ,n.User_ID
    ,n.Max_Pros_Key_I_Src
    ,n.Max_Pros_Key_I_Tgt
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_REML_UPDT_PARN_LIST_HIST_DEL </sc-trigger> */

/*CREATE DEL TRIGGER FOR  REML_UPDT_PARN_LIST_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_REML_UPDT_PARN_LIST_HIST_DEL 
AFTER DELETE ON U_D_DSV_001_REM_1.REML_UPDT_PARN_LIST_HIST 
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.REML_UPDT_PARN_LIST_HIST_LOG  SELECT A.*,USER,'DEL', CURRENT_TIMESTAMP FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_System_File </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_System_File
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_System_File
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_System_File_Log (
      Ctl_Id
    , File_Id
    , Data_File_Name
    , Ctl_File_Name
    , Queue_Name
    , Description
    , Data_File_Suffix
    , Ctl_File_Suffix
    , Data_File_Type
    , ET_Tolerance
    , UV_Tolerance
    , Sessions
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Queue_Name
    , n.Description
    , n.Data_File_Suffix
    , n.Ctl_File_Suffix
    , n.Data_File_Type
    , n.ET_Tolerance
    , n.UV_Tolerance
    , n.Sessions
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_File_Process </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_File_Process
AFTER UPDATE ON PDGENTCF.CTLFW_T_File_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_File_Process_Log (
      Process_Name
    , Ctl_Id
    , File_Id
    , Check_Load_Status
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Ctl_Id
    , n.File_Id
    , n.Check_Load_Status
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Step_AUT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Step_AUT
  AFTER UPDATE ON PDGDWPIIOBF.Dtf_Job_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Step_Hst
  (
     Job_Id
    ,Job_Step_No
    ,Job_Step_Seq_No
    ,Job_Step_Strt_Ts
    ,Job_Step_End_Ts
    ,Job_Step_Sts_Cd
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Job_Step_No
    ,n.Job_Step_Seq_No
    ,n.Job_Step_Strt_Ts
    ,n.Job_Step_End_Ts
    ,n.Job_Step_Sts_Cd
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_Hist
  (
     ExtJob_Name
    ,ExtJob_RunNo
    ,ExtJob_Type
    ,ExtJob_Stat
    ,ExtJob_Beg_Ts
    ,ExtJob_End_Ts
    ,ExtJob_Pri
    ,Prev_ExtJob_End_Ts
    ,ExtJob_ErrNo
    ,ExtJob_ErrMsg
    ,Prg_Stat_Cd
    ,Prg_Stat_Desc
    ,ExtSys_Name
    ,ExtSys_Mode
    ,ExtSys_Capture_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.ExtJob_Name
    ,o.ExtJob_RunNo
    ,o.ExtJob_Type
    ,o.ExtJob_Stat
    ,o.ExtJob_Beg_Ts
    ,o.ExtJob_End_Ts
    ,o.ExtJob_Pri
    ,o.Prev_ExtJob_End_Ts
    ,o.ExtJob_ErrNo
    ,o.ExtJob_ErrMsg
    ,o.Prg_Stat_Cd
    ,o.Prg_Stat_Desc
    ,o.ExtSys_Name
    ,o.ExtSys_Mode
    ,o.ExtSys_Capture_Ts
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Character_Set </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Character_Set
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Character_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Character_Set_Log (
      Character_Set_Name
    , Description
    , Max_Character_Length
    , Min_Character_Length
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Character_Set_Name
    , n.Description
    , n.Max_Character_Length
    , n.Min_Character_Length
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
orderingInstnCode,
orderingInstnName,
orderingInstnCity,
orderingInstnCountry,
orderingInstnBranchBSB,
orderingInstnBranchId,
orderingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ORDR_INST',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.orderingInstnCode,
n.orderingInstnName,
n.orderingInstnCity,
n.orderingInstnCountry,
n.orderingInstnBranchBSB,
n.orderingInstnBranchId,
n.orderingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_System_File_Param </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_System_File_Param
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_System_File_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_System_File_Param_Log (
      Ctl_Id
 , File_Id
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
 , n.File_Id
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYR_TRIG_D </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYR_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYR
REFERENCING OLD ROW AS n
FOR EACH ROW
WHEN (n.UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payerSameAsSwiftOrdCust,
PayerFullName,
payerAddr,
payerSuburb,
payerState,
payerPostcode,
payerCountry,
payerAccountBSB1,
payerAccountNumber1,
payerAccountBSB2,
payerAccountNumber2,
payerAccountBSB3,
payerAccountNumber3,
payerAbn,
payerAcn,
payerArbn,
payerIdentificationType1,
payerIdentificationTypeOther1,
payerIdentificationNumber1,
payerIdentificationIssuer1,
payerIdentificationCountry1,
payerIdentificationType2,
payerIdentificationTypeOther2,
payerIdentificationNumber2,
payerIdentificationIssuer2,
payerIdentificationCountry2,
payerIdentificationType3,
payerIdentificationTypeOther3,
payerIdentificationNumber3,
payerIdentificationIssuer3,
payerIdentificationCountry3,
payerCustNo,
payerIndividualDetailsDob,
payerDobSuburb,
payerDobState,
payerDobPostcode,
payerDobCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY 
)
VALUES
(
'IFTI_XML_REVW_PAYR',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payerSameAsSwiftOrdCust,
n.PayerFullName,
n.payerAddr,
n.payerSuburb,
n.payerState,
n.payerPostcode,
n.payerCountry,
n.payerAccountBSB1,
n.payerAccountNumber1,
n.payerAccountBSB2,
n.payerAccountNumber2,
n.payerAccountBSB3,
n.payerAccountNumber3,
n.payerAbn,
n.payerAcn,
n.payerArbn,
n.payerIdentificationType1,
n.payerIdentificationTypeOther1,
n.payerIdentificationNumber1,
n.payerIdentificationIssuer1,
n.payerIdentificationCountry1,
n.payerIdentificationType2,
n.payerIdentificationTypeOther2,
n.payerIdentificationNumber2,
n.payerIdentificationIssuer2,
n.payerIdentificationCountry2,
n.payerIdentificationType3,
n.payerIdentificationTypeOther3,
n.payerIdentificationNumber3,
n.payerIdentificationIssuer3,
n.payerIdentificationCountry3,
n.payerCustNo,
n.payerIndividualDetailsDob,
n.payerDobSuburb,
n.payerDobState,
n.payerDobPostcode,
n.payerDobCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY 
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_CE_System </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_CE_System
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_CE_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_CE_System_Log (
      CE_Id
    , Ctl_Id
    , CE_Ctl_Rltnshp_Type
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Ctl_Id
    , n.CE_Ctl_Rltnshp_Type
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Id_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Id_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDM_Job_Id
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Id_Hist
  (
     Job_Id
    ,Job_Stus_Cd
    ,Job_Start_Ts
    ,Job_End_Ts
    ,Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Id
    ,o.Job_Stus_Cd
    ,o.Job_Start_Ts
    ,o.Job_End_Ts
    ,o.Job_Cd
    ,o.UOW_Id
    ,o.Job_Priority
    ,o.Job_Type
    ,o.Job_Source
    ,o.Job_Description
    ,o.Source_TDPID
    ,o.Source_User
    ,o.Source_Passwd_Encrypted
    ,o.Source_Logon_Mechanism
    ,o.Source_Logon_Mechanism_Data
    ,o.Source_Userid_Pool
    ,o.Source_Account_Id
    ,o.Source_Sessions
    ,o.Target_TDPID
    ,o.Target_User
    ,o.Target_Passwd_Encrypted
    ,o.Target_Logon_Mechanism
    ,o.Target_Logon_Mechanism_Data
    ,o.Target_Userid_Pool
    ,o.Target_Account_Id
    ,o.Target_Sessions
    ,o.Data_Streams
    ,o.Response_Timeout
    ,o.Overwrite_Existing_Obj
    ,o.Force_Utility
    ,o.Log_Level
    ,o.Online_Archive
    ,o.Freeze_Job_Steps
    ,o.Max_Agents_Per_Task
    ,o.Additional_ARC_Parameters
    ,o.Query_Band_Str
    ,o.Sync_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_TTN_REML_UPDT_PARN_LIST_HIST_DEL </sc-trigger> */

/*CREATE DEL TRIGGER FOR  TTN_REML_UPDT_PARN_LIST_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_TTN_REML_UPDT_PARN_LIST_HIST_DEL 
AFTER DELETE ON U_D_DSV_001_REM_1.TTN_REML_UPDT_PARN_LIST_HIST 
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.TTN_REML_UPDT_PARN_LIST_HIST_LOG  SELECT A.*,USER,'DEL', CURRENT_TIMESTAMP FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Stream_BusDate </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Stream_BusDate
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Business_Date
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Business_Date
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_System_File_Extract </sc-trigger> */
REPLACE  TRIGGER PDGENTCF.CTLFW_TI_System_File_Extract
AFTER INSERT ON PDGENTCF.CTLFW_T_System_File_Extract
REFERENCING NEW ROW AS n
FOR EACH ROW (
INSERT INTO PDGENTCF.CTLFW_T_SFE_Log (
      Ctl_Id
    , File_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , File_Qualifier
    , Data_File_Name
    , Ctl_File_Name
    , Extraction_Date
    , Ext_Start_Time
    , Ext_End_Time
    , TD_Session_Id
    , Tool_Session_Id
    , Load_Status
    , Count_Source
    , Count_Target
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.File_Qualifier
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Extraction_Date
    , n.Ext_Start_Time
    , n.Ext_End_Time
    , n.TD_Session_Id
    , n.Tool_Session_Id
    , n.Load_Status
    , n.Count_Source
    , n.Count_Target
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Step_Cd_ADT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Step_Cd_ADT
  AFTER DELETE ON PDGDWPIIOBF.Dtf_Job_Step_Cd
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Step_Cd_Hst
  (
     Job_Step_No
    ,Job_Step_Nm
    ,Job_Step_Dscrp
    ,Modify_Action
   ) VALUES (
     o.Job_Step_No
    ,o.Job_Step_Nm
    ,o.Job_Step_Dscrp
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Limit_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Limit_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Limit
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Limit_Hist
  (
     Limit_No
    ,Candidates_Per_Iteration
    ,Obj_Per_Iteration
    ,MB_Per_Iteration
    ,Candidates_Per_Hour
    ,Obj_Per_Hour
    ,MB_Per_Hour
    ,Num_Running_DM_Jobs
    ,Num_Running_Tbl
    ,Num_Staged_Jobs
    ,Num_Staged_Tbl
    ,Num_Total_Jobs
    ,Num_Total_Tbl
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Limit_No
    ,o.Candidates_Per_Iteration
    ,o.Obj_Per_Iteration
    ,o.MB_Per_Iteration
    ,o.Candidates_Per_Hour
    ,o.Obj_Per_Hour
    ,o.MB_Per_Hour
    ,o.Num_Running_DM_Jobs
    ,o.Num_Running_Tbl
    ,o.Num_Staged_Jobs
    ,o.Num_Staged_Tbl
    ,o.Num_Total_Jobs
    ,o.Num_Total_Tbl
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Transform_KeyCol </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Transform_KeyCol
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,Key_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.Key_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR_TRIG_I </sc-trigger> */


-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR
 --------------------------------------------------------------------- 
 --DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendersCorrespondentCode1,
sendersCorrespondentName1,
sendersCorrespondentCity1,
sendersCorrespondentCountry1,
sendersCorrespondentCode2,
sendersCorrespondentName2,
sendersCorrespondentCity2,
sendersCorrespondentCountry2,
sendersCorrespondentCode3,
sendersCorrespondentName3,
sendersCorrespondentCity3,
sendersCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_CORR',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendersCorrespondentCode1,
n.sendersCorrespondentName1,
n.sendersCorrespondentCity1,
n.sendersCorrespondentCountry1,
n.sendersCorrespondentCode2,
n.sendersCorrespondentName2,
n.sendersCorrespondentCity2,
n.sendersCorrespondentCountry2,
n.sendersCorrespondentCode3,
n.sendersCorrespondentName3,
n.sendersCorrespondentCity3,
n.sendersCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Stream_Id </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Stream_Id
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Id_Log (
      Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */

-- Bankwest  Credit Insert Trigger
--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_BWA_CR_TRIG_I
replace trigger U_D_DSV_001_NIT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_I
after insert on U_D_DSV_001_NIT_1.NOST_BWA_CR_EXC_RPRT_ACCESS
referencing new row as n
for each row
insert into U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
values
(
 'NOST_BWA_CR_EXC_RPRT_ACCESS'
,user
,current_timestamp(6)
,'INSERT'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Process_Type </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Process_Type
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Type
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Log (
      Process_Type
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space_Hist
  (
     LogDate
    ,PermSpace
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.LogDate
    ,o.PermSpace
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_REML_PRJC_MSTR_HIST_DEL </sc-trigger> */

/*CREATE DEL TRIGGER FOR  REML_PRJC_MSTR_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_REML_PRJC_MSTR_HIST_DEL 
AFTER DELETE ON U_D_DSV_001_REM_1.REML_PRJC_MSTR_HIST 
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.REML_PRJC_MSTR_HIST_LOG  SELECT A.*,USER,'DEL', CURRENT_TIMESTAMP FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_File_Process </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_File_Process
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_File_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_File_Process_Log (
      Process_Name
    , Ctl_Id
    , File_Id
    , Check_Load_Status
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Ctl_Id
    , n.File_Id
    , n.Check_Load_Status
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_System_File </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_System_File
AFTER INSERT ON PDGENTCF.CTLFW_T_System_File
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_System_File_Log (
      Ctl_Id
    , File_Id
    , Data_File_Name
    , Ctl_File_Name
    , Queue_Name
    , Description
    , Data_File_Suffix
    , Ctl_File_Suffix
    , Data_File_Type
    , ET_Tolerance
    , UV_Tolerance
    , Sessions
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Queue_Name
    , n.Description
    , n.Data_File_Suffix
    , n.Ctl_File_Suffix
    , n.Data_File_Type
    , n.ET_Tolerance
    , n.UV_Tolerance
    , n.Sessions
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.TU_PIIDataScanningCheckType </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.TU_PIIDataScanningCheckType
AFTER UPDATE ON PDGDWPIIOBF.PIIDataScanningCheckType
REFERENCING OLD ROW AS o
FOR EACH ROW  (
INSERT INTO PDGDWPIIOBF.PIIDataScanningCheckTypeLog (
      CheckType
    , CheckTypeRule
    , CheckTypeDesc
    , PIIDataType
    , UpdateUser
    , UpdateTs
  ) VALUES (
      o.CheckType
    , o.CheckTypeRule
    , o.CheckTypeDesc
    , o.PIIDataType
    , o.UpdateUser
    , o.UpdateTs
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest_Hist
  (
     Destination
    ,DestinationDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Destination
    ,o.DestinationDesc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Stream_Id </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Stream_Id
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Id_Log (
      Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendingInstnCode,
sendingInstnName,
sendingInstnCity,
sendingInstnCountry,
sendingInstnBranchBSB,
sendingInstnBranchId,
sendingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_INST',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendingInstnCode,
n.sendingInstnName,
n.sendingInstnCity,
n.sendingInstnCountry,
n.sendingInstnBranchBSB,
n.sendingInstnBranchId,
n.sendingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_System_File_Extract </sc-trigger> */



REPLACE  TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_System_File_Extract
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_System_File_Extract
REFERENCING NEW ROW AS n
FOR EACH ROW (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_SFE_Log (
      Ctl_Id
    , File_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , File_Qualifier
    , Data_File_Name
    , Ctl_File_Name
    , Extraction_Date
    , Ext_Start_Time
    , Ext_End_Time
    , TD_Session_Id
    , Tool_Session_Id
    , Load_Status
    , Count_Source
    , Count_Target
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.File_Qualifier
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Extraction_Date
    , n.Ext_Start_Time
    , n.Ext_End_Time
    , n.TD_Session_Id
    , n.Tool_Session_Id
    , n.Load_Status
    , n.Count_Source
    , n.Count_Target
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST_TRIG_I </sc-trigger> */
-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST
 --------------------------------------------------------------------- PAYR
 --DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendingInstnCode,
sendingInstnName,
sendingInstnCity,
sendingInstnCountry,
sendingInstnBranchBSB,
sendingInstnBranchId,
sendingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_INST',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendingInstnCode,
n.sendingInstnName,
n.sendingInstnCity,
n.sendingInstnCountry,
n.sendingInstnBranchBSB,
n.sendingInstnBranchId,
n.sendingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Process </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Process
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Log (
      Process_Name
    , Description
    , Process_Type
    , Ctl_Id
    , Stream_Key
    , In_DB_Name
    , In_Object_Name
    , Out_DB_Name
    , Out_Object_Name
    , Target_TableDatabaseName
    , Target_TableName
    , Temp_DatabaseName
    , Key_Set_Id
    , Domain_Id
    , Code_Set_Id
    , Collect_Stats
    , Truncate_Target
    , Verification_Flag
    , File_Qualifier_Reset_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Description
    , n.Process_Type
    , n.Ctl_Id
    , n.Stream_Key
    , n.In_DB_Name
    , n.In_Object_Name
    , n.Out_DB_Name
    , n.Out_Object_Name
    , n.Target_TableDatabaseName
    , n.Target_TableName
    , n.Temp_DatabaseName
    , n.Key_Set_Id
    , n.Domain_Id
    , n.Code_Set_Id
    , n.Collect_Stats
    , n.Truncate_Target
    , n.Verification_Flag
    , n.File_Qualifier_Reset_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_Hist
  (
     Cycl_Start_Ts
    ,Cycl_End_Ts
    ,Cycl_Stat
    ,Src_Sys
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cycl_Start_Ts
    ,n.Cycl_End_Ts
    ,n.Cycl_Stat
    ,n.Src_Sys
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_REML_ACCT_AGGR_HIST_UPD </sc-trigger> */


/*CREATE UPD TRIGGER FOR  REML_ACCT_AGGR_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_REML_ACCT_AGGR_HIST_UPD
AFTER UPDATE ON U_D_DSV_001_REM_1.REML_ACCT_AGGR_HIST
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.REML_ACCT_AGGR_HIST_LOG  SELECT A.*,USER,'UPD', CURRENT_TIMESTAMP  FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Transform </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Transform
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Transform
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Transform_Log 
 (
 Process_Name
 ,Business_Date
 ,Business_Date_Cycle_Num
 ,Business_Date_Cycle_Start_Ts
 ,Process_Id
 ,TD_Session_Id
 ,Tool_Session_Id
 ,In_DB_Name
 ,In_Object_Name
 ,Out_DB_Name
 ,Out_Object_Name
 ,Rows_Inserted
 ,Rows_Updated
 ,Rows_Deleted
 ,Run_Date
 ,Start_Time
 ,End_Time
 ,Update_Date
 ,Update_User
 ,Update_Time
 ,Update_Ts
   ) 
  VALUES 
   (
 n.Process_Name
 ,n.Business_Date
 ,n.Business_Date_Cycle_Num
 ,n.Business_Date_Cycle_Start_Ts
 ,n.Process_Id
 ,n.TD_Session_Id
 ,n.Tool_Session_Id
 ,n.In_DB_Name
 ,n.In_Object_Name
 ,n.Out_DB_Name
 ,n.Out_Object_Name
 ,n.Rows_Inserted
 ,n.Rows_Updated
 ,n.Rows_Deleted
 ,n.Run_Date
 ,n.Start_Time
 ,n.End_Time
 ,n.Update_Date
 ,n.Update_User
 ,n.Update_Time
 ,n.Update_Ts
   );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_File_Process </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_File_Process
AFTER INSERT ON PDGENTCF.CTLFW_T_File_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_File_Process_Log (
      Process_Name
    , Ctl_Id
    , File_Id
    , Check_Load_Status
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Ctl_Id
    , n.File_Id
    , n.Check_Load_Status
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Step_AIT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Step_AIT
  AFTER INSERT ON PDGDWPIIOBF.Dtf_Job_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Step_Hst
  (
     Job_Id
    ,Job_Step_No
    ,Job_Step_Seq_No
    ,Job_Step_Strt_Ts
    ,Job_Step_End_Ts
    ,Job_Step_Sts_Cd
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Job_Step_No
    ,n.Job_Step_Seq_No
    ,n.Job_Step_Strt_Ts
    ,n.Job_Step_End_Ts
    ,n.Job_Step_Sts_Cd
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Step_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Step_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDM_Job_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Step_Hist
  (
     Job_Cd
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Step_No
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Executable_User
    ,n.Executable_Platform_Name
    ,n.Executable_BT_ET_Flag
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Process_Type_Param </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Process_Type_Param
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param_Log (
      Process_Type
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST_TRIG_D </sc-trigger> */

-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendingInstnCode,
sendingInstnName,
sendingInstnCity,
sendingInstnCountry,
sendingInstnBranchBSB,
sendingInstnBranchId,
sendingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_INST',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendingInstnCode,
n.sendingInstnName,
n.sendingInstnCity,
n.sendingInstnCountry,
n.sendingInstnBranchBSB,
n.sendingInstnBranchId,
n.sendingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Transform_SurrKeyCol </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Transform_SurrKeyCol
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Transform_SurrKeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Transform_SKCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,SurrKey_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.SurrKey_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_BASE_TRIG_I </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_BASE_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_BASE
REFERENCING NEW ROW AS n
FOR EACH ROW
WHEN (n.UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
filename,
txnRefNo ,
interceptFlag ,
isSwiftOrStructured,
swiftMsg ,
transferDate,
direction,
currency ,
currencyAmount,
valueDate ,    
RPRT_STUS ,
REVW_MODF_BY,
REVW_DTTM ,
SUPV_MODF_BY,
SUPV_DTTM ,
CRET_BY,
CRET_DTTM ,
INDA_DRFT ,
INDA_REDY,
INDA_APRD,
INDA_RWRK,
INDA_NOT_RPRT,
EXC_REAS ,
ENUS_CMMT ,
SDS_MSG_ID ,
SDS_MSG_SEQ,
base_transferDate,
base_direction,
base_currency ,
base_currencyAmount,
base_valueDate , 
INDA_PAYR,
INDA_PAYE,
INDA_ORDR_INST,
INDA_SEND_INST,
INDA_SEND_CORR,
INDA_BENE_INST,
INDA_RECE_INST,
INDA_RECE_CORR,
INDA_ADDN_DETL,       
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_BASE',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.filename,
n.txnRefNo ,
n.interceptFlag ,
n.isSwiftOrStructured,
n.swiftMsg ,
n.transferDate,
n.direction,
n.currency ,
n.currencyAmount,
n.valueDate ,    
n.RPRT_STUS ,
n.REVW_MODF_BY,
n.REVW_DTTM ,
n.SUPV_MODF_BY,
n.SUPV_DTTM ,
n.CRET_BY,
n.CRET_DTTM ,
n.INDA_DRFT ,
n.INDA_REDY,
n.INDA_APRD,
n.INDA_RWRK,
n.INDA_NOT_RPRT,
n.EXC_REAS ,
n.ENUS_CMMT ,
n.SDS_MSG_ID ,
n.SDS_MSG_SEQ,
n.base_transferDate,
n.base_direction,
n.base_currency ,
n.base_currencyAmount,
n.base_valueDate , 
n.INDA_PAYR,
n.INDA_PAYE,
n.INDA_ORDR_INST,
n.INDA_SEND_INST,
n.INDA_SEND_CORR,
n.INDA_BENE_INST,
n.INDA_RECE_INST,
n.INDA_RECE_CORR,
n.INDA_ADDN_DETL,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Process_Id </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Process_Id
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Id_Log (
      Process_Name
    , Process_Id
    , Process_State
    , Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Process_Id
    , n.Process_State
    , n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_DM_Stus_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_DM_Stus_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDM_DM_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_DM_Stus_Hist
  (
     ResourceId
    ,UOWId
    ,Job_CD
    ,Job_Stus_Cd
    ,Start_TS
    ,End_TS
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.ResourceId
    ,n.UOWId
    ,n.Job_CD
    ,n.Job_Stus_Cd
    ,n.Start_TS
    ,n.End_TS
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> U_D_DSV_001_REM_1.TRG_REML_CUST_AGGR_SMRY_HIST_UPD </sc-trigger> */


/*CREATE UPD TRIGGER FOR  REML_CUST_AGGR_SMRY_HIST */
CREATE TRIGGER U_D_DSV_001_REM_1.TRG_REML_CUST_AGGR_SMRY_HIST_UPD
AFTER UPDATE ON U_D_DSV_001_REM_1.REML_CUST_AGGR_SMRY_HIST
REFERENCING OLD_TABLE AS O1
INSERT INTO U_D_DSV_001_REM_1.REML_CUST_AGGR_SMRY_HIST_LOG  SELECT A.*,USER,'UPD', CURRENT_TIMESTAMP  FROM O1 AS A;

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Stream_Id </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Stream_Id
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Id_Log (
      Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Character_Set </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Character_Set
AFTER INSERT ON PDGENTCF.CTLFW_T_Character_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Character_Set_Log (
      Character_Set_Name
    , Description
    , Max_Character_Length
    , Min_Character_Length
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Character_Set_Name
    , n.Description
    , n.Max_Character_Length
    , n.Min_Character_Length
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_ADT </sc-trigger> */
REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_ADT
  AFTER DELETE ON PDGDWPIIOBF.Dtf_Job
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Hst
  (
     Job_Id
    ,Job_Strt_Dt
    ,Job_Strt_Tm
    ,Job_End_Dt
    ,Job_End_Tm
    ,Job_Cd
    ,Stus_Cd
    ,Modify_Action
   ) VALUES (
     o.Job_Id
    ,o.Job_Strt_Dt
    ,o.Job_Strt_Tm
    ,o.Job_End_Dt
    ,o.Job_End_Tm
    ,o.Job_Cd
    ,o.Stus_Cd
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log_Hist
  (
     Purge_Db
    ,Purge_Tbl
    ,Log_Date
    ,Start_Time
    ,Stop_Time
    ,Rows_Returned
    ,Status
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Purge_Db
    ,n.Purge_Tbl
    ,n.Log_Date
    ,n.Start_Time
    ,n.Stop_Time
    ,n.Rows_Returned
    ,n.Status
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_System_File_Extract </sc-trigger> */
REPLACE  TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_System_File_Extract
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_System_File_Extract
REFERENCING NEW ROW AS n
FOR EACH ROW (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_SFE_Log (
      Ctl_Id
    , File_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , File_Qualifier
    , Data_File_Name
    , Ctl_File_Name
    , Extraction_Date
    , Ext_Start_Time
    , Ext_End_Time
    , TD_Session_Id
    , Tool_Session_Id
    , Load_Status
    , Count_Source
    , Count_Target
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.File_Qualifier
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Extraction_Date
    , n.Ext_Start_Time
    , n.Ext_End_Time
    , n.TD_Session_Id
    , n.Tool_Session_Id
    , n.Load_Status
    , n.Count_Source
    , n.Count_Target
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_EXC_CMMT </sc-trigger> */
Create TRIGGER U_D_DSV_001_NFT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_EXC_CMMT 
AFTER UPDATE OF OTHR_CMMT ON U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_IDENTIFIER,
            EXC_OFCR,
            EXC_OFCR_DTTM,
            EXC_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_IDENTIFIER,
            COALESCE (NewRow.EXC_OFCR, NULL, CURRENT_USER),
            COALESCE (NewRow.EXC_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
            NewRow.OTHR_CMMT
            );

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Transform </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Transform
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Transform
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Transform_Log 
 (
 Process_Name
 ,Business_Date
 ,Business_Date_Cycle_Num
 ,Business_Date_Cycle_Start_Ts
 ,Process_Id
 ,TD_Session_Id
 ,Tool_Session_Id
 ,In_DB_Name
 ,In_Object_Name
 ,Out_DB_Name
 ,Out_Object_Name
 ,Rows_Inserted
 ,Rows_Updated
 ,Rows_Deleted
 ,Run_Date
 ,Start_Time
 ,End_Time
 ,Update_Date
 ,Update_User
 ,Update_Time
 ,Update_Ts
 ) 
VALUES 
 (
 n.Process_Name
 ,n.Business_Date
 ,n.Business_Date_Cycle_Num
 ,n.Business_Date_Cycle_Start_Ts
 ,n.Process_Id
 ,n.TD_Session_Id
 ,n.Tool_Session_Id
 ,n.In_DB_Name
 ,n.In_Object_Name
 ,n.Out_DB_Name
 ,n.Out_Object_Name
 ,n.Rows_Inserted
 ,n.Rows_Updated
 ,n.Rows_Deleted
 ,n.Run_Date
 ,n.Start_Time
 ,n.End_Time
 ,n.Update_Date
 ,n.Update_User
 ,n.Update_Time
 ,n.Update_Ts
 );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendingInstnCode,
sendingInstnName,
sendingInstnCity,
sendingInstnCountry,
sendingInstnBranchBSB,
sendingInstnBranchId,
sendingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_INST',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendingInstnCode,
n.sendingInstnName,
n.sendingInstnCity,
n.sendingInstnCountry,
n.sendingInstnBranchBSB,
n.sendingInstnBranchId,
n.sendingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Stream </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Stream
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Stream
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Stream_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Stream_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Obj_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Obj_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDM_Job_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Obj_Hist
  (
     Job_Cd
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Db_Nm
    ,n.Obj_Nm
    ,n.Obj_Type
    ,n.Copy_Stat
    ,n.Compare_DDL
    ,n.Where_Clause
    ,n.Key_Column
    ,n.Query_Band_Str
    ,n.Job_Desc
    ,n.Staging_Database_Nm
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Process </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Process
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Log (
      Process_Name
    , Description
    , Process_Type
    , Ctl_Id
    , Stream_Key
    , In_DB_Name
    , In_Object_Name
    , Out_DB_Name
    , Out_Object_Name
    , Target_TableDatabaseName
    , Target_TableName
    , Temp_DatabaseName
    , Key_Set_Id
    , Domain_Id
    , Code_Set_Id
    , Collect_Stats
    , Truncate_Target
    , Verification_Flag
    , File_Qualifier_Reset_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Description
    , n.Process_Type
    , n.Ctl_Id
    , n.Stream_Key
    , n.In_DB_Name
    , n.In_Object_Name
    , n.Out_DB_Name
    , n.Out_Object_Name
    , n.Target_TableDatabaseName
    , n.Target_TableName
    , n.Temp_DatabaseName
    , n.Key_Set_Id
    , n.Domain_Id
    , n.Code_Set_Id
    , n.Collect_Stats
    , n.Truncate_Target
    , n.Verification_Flag
    , n.File_Qualifier_Reset_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_System </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_System
AFTER UPDATE ON PDGENTCF.CTLFW_T_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_System_Log (
      Ctl_Id
    , System_Name
    , Path_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.System_Name
    , n.Path_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.TD_MaskingMetadataRequest </sc-trigger> */




REPLACE TRIGGER PDGDWPIIOBF.TD_MaskingMetadataRequest
AFTER DELETE ON PDGDWPIIOBF.Masking_Metadata_Request
REFERENCING OLD TABLE AS o
FOR EACH STATEMENT  (
INSERT INTO PDGDWPIIOBF.Masking_Metadata_Request_Log
    (
        DatabaseName
        ,TableName
        ,ColumnName
        ,ColumnExpression
        ,MaskingExpression
        ,Comments
        ,LoggingType
        ,LoggingUser
        ,LoggingDate
        ,LoggingTimestamp 
    )
SELECT  o.DatabaseName
        ,o.TableName
        ,o.ColumnName
        ,o.ColumnExpression
        ,o.MaskingExpression
        ,o.Comments
        ,'DELETE'
        ,CURRENT_USER
        ,CURRENT_DATE
        ,CURRENT_TIMESTAMP(6)

FROM o
;
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg_Hist
  (
    Cand_Source
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
    n.Cand_Source
    ,n.TDM_Job_Priority
    ,n.TDM_Job_Type
    ,n.TDM_Job_Source
    ,n.TDM_Job_Description
    ,n.TDM_Source_TDPID
    ,n.TDM_Source_User
    ,n.TDM_Source_Passwd_Encrypted
    ,n.TDM_Source_Logon_Mech
    ,n.TDM_Source_Logon_Mech_Data
    ,n.TDM_Source_Userid_Pool
    ,n.TDM_Source_Account_Id
    ,n.TDM_Source_Sessions
    ,n.TDM_Target_TDPID
    ,n.TDM_Target_User
    ,n.TDM_Target_Passwd_Encrypted
    ,n.TDM_Target_Logon_Mech
    ,n.TDM_Target_Logon_Mech_Data
    ,n.TDM_Target_Userid_Pool
    ,n.TDM_Target_Account_Id
    ,n.TDM_Target_Sessions
    ,n.TDM_Data_Streams
    ,n.TDM_Response_Timeout
    ,n.TDM_Overwrite_Existing_Obj
    ,n.TDM_Force_Utility
    ,n.TDM_Log_Level
    ,n.TDM_Online_Archive
    ,n.TDM_Freeze_Job_Steps
    ,n.TDM_Max_Agents_Per_Task
    ,n.TDM_Additional_ARC_Parameters
    ,n.TDM_Query_Band_Str
    ,n.TDM_Sync_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_File_Process </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_File_Process
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_File_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_File_Process_Log (
      Process_Name
    , Ctl_Id
    , File_Id
    , Check_Load_Status
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Ctl_Id
    , n.File_Id
    , n.Check_Load_Status
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE_TRIG_D </sc-trigger> */


--DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payeeSameAsSwiftBenCust,
payeeFullName,
payeeAddr,
payeeSuburb,
payeeState,
payeePostcode,
payeeCountry,
payeeAccountBSB1,
payeeAccountNumber1,
payeeAccountBSB2,
payeeAccountNumber2,
payeeAccountBSB3,
payeeAccountNumber3,
payeeIdentificationType1,
payeeIdentificationTypeOther1,
payeeIdentificationNumber1,
payeeIdentificationIssuer1,
payeeIdentificationCountry1,
payeeIdentificationType2,
payeeIdentificationTypeOther2,
payeeIdentificationNumber2,
payeeIdentificationIssuer2,
payeeIdentificationCountry2,
payeeIdentificationType3,
payeeIdentificationTypeOther3,
payeeIdentificationNumber3,
payeeIdentificationIssuer3,
payeeIdentificationCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_PAYE',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payeeSameAsSwiftBenCust,
n.payeeFullName,
n.payeeAddr,
n.payeeSuburb,
n.payeeState,
n.payeePostcode,
n.payeeCountry,
n.payeeAccountBSB1,
n.payeeAccountNumber1,
n.payeeAccountBSB2,
n.payeeAccountNumber2,
n.payeeAccountBSB3,
n.payeeAccountNumber3,
n.payeeIdentificationType1,
n.payeeIdentificationTypeOther1,
n.payeeIdentificationNumber1,
n.payeeIdentificationIssuer1,
n.payeeIdentificationCountry1,
n.payeeIdentificationType2,
n.payeeIdentificationTypeOther2,
n.payeeIdentificationNumber2,
n.payeeIdentificationIssuer2,
n.payeeIdentificationCountry2,
n.payeeIdentificationType3,
n.payeeIdentificationTypeOther3,
n.payeeIdentificationNumber3,
n.payeeIdentificationIssuer3,
n.payeeIdentificationCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_System </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_System
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_System_Log (
      Ctl_Id
    , System_Name
    , Path_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.System_Name
    , n.Path_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.test_NOST_EXC_RPRT_ACCESS_TRIG_U </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.test_NOST_EXC_RPRT_ACCESS_TRIG_U
AFTER UPDATE OF (Reason, Other_Comment, Investigation_Status, swift_50, Decision_Matrix_Id)
ON U_D_DSV_001_NIT_1.test_NOST_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS old_row
	NEW ROW AS new_row
FOR EACH ROW
UPDATE U_D_DSV_001_NIT_1.test_NOST_EXC_RPRT_ACCESS
SET last_updated_by = CURRENT_USER
, last_updated_date = CURRENT_TIMESTAMP
WHERE unique_identifier = old_row.unique_identifier;

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_System_File_Param </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_System_File_Param
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_System_File_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_System_File_Param_Log (
      Ctl_Id
 , File_Id
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
 , n.File_Id
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Next_Job_Id_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Next_Job_Id_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDM_Next_Job_Id
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Next_Job_Id_Hist
  (
     Fixed_Value
    ,Next_Job_Id
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Fixed_Value
    ,n.Next_Job_Id
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Process_Id </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Process_Id
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Id_Log (
      Process_Name
    , Process_Id
    , Process_State
    , Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Process_Id
    , n.Process_State
    , n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Corporate_Entity </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Corporate_Entity
AFTER INSERT ON PDGENTCF.CTLFW_T_Corporate_Entity
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Corporate_Entity_Log (
      CE_Id
    , Corporate_Entity_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Corporate_Entity_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.TI_BKey_Key_Set </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.TI_BKey_Key_Set
AFTER INSERT ON PDGDWPIIOBF.BKey_Key_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGDWPIIOBF.BKey_Key_Set_Log (
      Key_Set_Id
    , Description
    , Key_Table_Name
    , Key_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Description
    , n.Key_Table_Name
    , n.Key_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step_Hist
  (
     Job_Id
    ,Step_No
    ,Step_Start_Ts
    ,Step_End_Ts
    ,Step_Stus_Cd
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Final_Step_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Step_No
    ,n.Step_Start_Ts
    ,n.Step_End_Ts
    ,n.Step_Stus_Cd
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Executable_User
    ,n.Executable_Platform_Name
    ,n.Executable_BT_ET_Flag
    ,n.Enabled_Flag
    ,n.Final_Step_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Process_Param </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Process_Param
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Param_Log (
      Process_Name
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_D </sc-trigger> */


-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
additionalDetailsOfPayment,
additionalOtherDetails,
additionalInstnCode,
additionalInstnName,
additionalInstnCity,
additionalInstnCountry,
additionalInstnAccountBSB,
additionalInstnAccountNumber,
senderToReceiverInfo1,
senderToReceiverInfo2,
senderToReceiverInfo3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ADDN_DETL',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.additionalDetailsOfPayment,
n.additionalOtherDetails,
n.additionalInstnCode,
n.additionalInstnName,
n.additionalInstnCity,
n.additionalInstnCountry,
n.additionalInstnAccountBSB,
n.additionalInstnAccountNumber,
n.senderToReceiverInfo1,
n.senderToReceiverInfo2,
n.senderToReceiverInfo3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_System_File_Param </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_System_File_Param
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_System_File_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_System_File_Param_Log (
      Ctl_Id
 , File_Id
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
 , n.File_Id
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_BASE_TRIG_U </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_BASE_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_BASE
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
filename,
txnRefNo ,
interceptFlag ,
isSwiftOrStructured,
swiftMsg ,
transferDate,
direction,
currency ,
currencyAmount,
valueDate ,    
RPRT_STUS ,
REVW_MODF_BY,
REVW_DTTM ,
SUPV_MODF_BY,
SUPV_DTTM ,
CRET_BY,
CRET_DTTM ,
INDA_DRFT ,
INDA_REDY,
INDA_APRD,
INDA_RWRK,
INDA_NOT_RPRT,
EXC_REAS ,
ENUS_CMMT ,
SDS_MSG_ID ,
SDS_MSG_SEQ,
base_transferDate,
base_direction,
base_currency ,
base_currencyAmount,
base_valueDate , 
INDA_PAYR,
INDA_PAYE,
INDA_ORDR_INST,
INDA_SEND_INST,
INDA_SEND_CORR,
INDA_BENE_INST,
INDA_RECE_INST,
INDA_RECE_CORR,
INDA_ADDN_DETL,
LINK_KEY       
)
VALUES
(
'IFTI_XML_REVW_BASE',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.filename,
n.txnRefNo ,
n.interceptFlag ,
n.isSwiftOrStructured,
n.swiftMsg ,
n.transferDate,
n.direction,
n.currency ,
n.currencyAmount,
n.valueDate ,    
n.RPRT_STUS ,
n.REVW_MODF_BY,
n.REVW_DTTM ,
n.SUPV_MODF_BY,
n.SUPV_DTTM ,
n.CRET_BY,
n.CRET_DTTM ,
n.INDA_DRFT ,
n.INDA_REDY,
n.INDA_APRD,
n.INDA_RWRK,
n.INDA_NOT_RPRT,
n.EXC_REAS ,
n.ENUS_CMMT ,
n.SDS_MSG_ID ,
n.SDS_MSG_SEQ,
n.base_transferDate,
n.base_direction,
n.base_currency ,
n.base_currencyAmount,
n.base_valueDate , 
n.INDA_PAYR,
n.INDA_PAYE,
n.INDA_ORDR_INST,
n.INDA_SEND_INST,
n.INDA_SEND_CORR,
n.INDA_BENE_INST,
n.INDA_RECE_INST,
n.INDA_RECE_CORR,
n.INDA_ADDN_DETL,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Stream_Param </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Stream_Param
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
      Stream_Key
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg_Hist
  (
     DataBaseName
    ,TableName
    ,Version
    ,CreatorName
    ,CreateTimeStamp
    ,LastAlterName
    ,LastAlterTimeStamp
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DataBaseName
    ,n.TableName
    ,n.Version
    ,n.CreatorName
    ,n.CreateTimeStamp
    ,n.LastAlterName
    ,n.LastAlterTimeStamp
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Character_Set </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Character_Set
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Character_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Character_Set_Log (
      Character_Set_Name
    , Description
    , Max_Character_Length
    , Min_Character_Length
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Character_Set_Name
    , n.Description
    , n.Max_Character_Length
    , n.Min_Character_Length
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Param_Group </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Param_Group
AFTER UPDATE ON PDGENTCF.CTLFW_T_Param_Group
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Param_Group_Log (
      Param_Group
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Param_Group
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Detl_ADT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Detl_ADT
  AFTER DELETE ON PDGDWPIIOBF.Dtf_Job_Detl
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Detl_Hst
  (
     Job_Cd
    ,Job_Priority
    ,Source_Tdpid
    ,Target_Tdpid
    ,Source_User
    ,Source_Passwd
    ,Target_User
    ,Target_Passwd
    ,Max_Agents_Per_Task
    ,Data_Streams
    ,Source_Sessions
    ,Target_Sessions
    ,Overwrite_Existing_Objects
    ,Force_Utility
    ,Foreign_Server
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Staging_Database
    ,Staging_Database_For_Table
    ,Object_Include_Flag
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.Job_Priority
    ,o.Source_Tdpid
    ,o.Target_Tdpid
    ,o.Source_User
    ,o.Source_Passwd
    ,o.Target_User
    ,o.Target_Passwd
    ,o.Max_Agents_Per_Task
    ,o.Data_Streams
    ,o.Source_Sessions
    ,o.Target_Sessions
    ,o.Overwrite_Existing_Objects
    ,o.Force_Utility
    ,o.Foreign_Server
    ,o.Log_Level
    ,o.Online_Archive
    ,o.Freeze_Job_Steps
    ,o.Staging_Database
    ,o.Staging_Database_For_Table
    ,o.Object_Include_Flag
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_Hist
  (
     Cand_Obj_Source_Type_Cd
    ,Cand_Obj_Source_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Source_Type_Cd
    ,n.Cand_Obj_Source_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_File_Process </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_File_Process
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_File_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_File_Process_Log (
      Process_Name
    , Ctl_Id
    , File_Id
    , Check_Load_Status
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Ctl_Id
    , n.File_Id
    , n.Check_Load_Status
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_VERF_CMMT </sc-trigger> */
Create TRIGGER U_D_DSV_001_NFT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_VERF_CMMT 
AFTER UPDATE OF VERF_OFCR_CMMT ON U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCESS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NFT_1.NOST_VOST_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_ID,
            VERF_OFCR,
            VERF_OFCR_DTTM,
            VERF_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_ID,
            COALESCE (NewRow.VERF_OFCR, NULL, CURRENT_USER),
            COALESCE (NewRow.VERF_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
            NewRow.VERF_OFCR_CMMT
            );

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFWX_TU_Xfm_Process </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFWX_TU_Xfm_Process
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process
REFERENCING NEW ROW AS NR
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process_Log (
      Process_Name,           
      Busn_Dt ,         
      Xfm_Run_Dt ,                        
      Xfm_Rows_Inserted ,                       
      Xfm_Stg_Ld_Status ,           
      Xfm_Stg_Ld_Start_Time ,
      Xfm_Stg_Ld_End_Time ,   
      Xfm_Stg_Val_Start_Time ,
      Xfm_Stg_Val_End_Time ,  
      Msur_Mdl_Pub_Run_Dt ,         
      Msur_Mdl_Pub_Process_Id ,           
      Msur_Mdl_Pub_Start_Time ,           
      Msur_Mdl_Pub_End_Time ,
      Msur_Mdl_Pub_Status ,   
      Msur_Mdl_Pub_Priority , 
      Pub_Stream_Key ,
      Update_User ,           
      Update_Ts 
  ) VALUES (
      NR.Process_Name,        
      NR.Busn_Dt ,            
      NR.Xfm_Run_Dt ,                     
      NR.Xfm_Rows_Inserted ,                    
      NR.Xfm_Stg_Ld_Status ,        
      NR.Xfm_Stg_Ld_Start_Time ,
      NR.Xfm_Stg_Ld_End_Time ,
      NR.Xfm_Stg_Val_Start_Time ,
      NR.Xfm_Stg_Val_End_Time ,     
      NR.Msur_Mdl_Pub_Run_Dt ,            
      NR.Msur_Mdl_Pub_Process_Id ,        
      NR.Msur_Mdl_Pub_Start_Time ,        
      NR.Msur_Mdl_Pub_End_Time ,
      NR.Msur_Mdl_Pub_Status ,      
      NR.Msur_Mdl_Pub_Priority ,    
      NR.Pub_Stream_Key ,
      NR.Update_User ,        
      NR.Update_Ts 
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.QTLR_RPRT_EXC_UI_TRIG_U </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.QTLR_RPRT_EXC_UI_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.QTLR_RPRT_EXC_UI
REFERENCING NEW ROW AS N
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.QTLR_RPRT_EXC_AUDT
(
	 AUDT_TABL_M                   
	,MODF_USER_M                   
	,MODF_DTTS                     
	,OPER_TYPE  , 
	  UID ,
      CLYR ,
      QRTR ,
      NOTC ,
      REPORTING_ENTITY_NO ,
      REPORTING_ENTITY_NAME ,
      REPORT_NO ,
      TRN ,
      TRANSACTION_VALUE_DATE ,
      TRANSACTION_CURRENCY_CODE ,
      TRANSACTION_AMOUNT ,
      BENEFICIARY_CUSTOMER ,
      ORDERING_INSTITUTION ,
      HEADER_BLOCKS ,
      BSB_NO ,
      ACCOUNT_NO ,
      ACCOUNT_NAME ,
      SEQUENCE_NO ,
      FAMILY_NAME ,
      FIRST_GIVEN_NAME ,
      SECOND_GIVEN_NAME ,
      BIRTH_DATE ,
      BUSINESS_NAME ,
      ADDRESS_LINE_1 ,
      ADDRESS_LINE_2 ,
      SUBURB ,
      STATE ,
      POSTCODE ,
      ABN ,
      ACN ,
      EXCEPTIONS ,
      BENEFICIARY_INSTITUTION_NAME ,
      RELATED_REFERENCE ,
      RECEIVING_BANK ,
      PAYMENT_CURRENCY_CODE ,
      PAYMENT_AMOUNT ,
      CHANNEL ,
      TRANSFER_DATE ,
      HEAD_BLOK_1 ,
      HEAD_BLOK_2 ,
      SWF_CODE ,
      PROS_ENTY ,
      INDA_BWA_NON_AUD ,
      TRN_DATE ,
      TRN_NUMBER ,
      TRN_TIMESTAMP,
      PAY_DATE ,
      CAN_MEMO ,
      TRADER_CTRL ,
      SWIFT_ID ,
      CDT_WIR_ID ,
      CDT_SPC_INST1 ,
      SBK_REF_NUM ,
      MESSAGE_TEXT ,
      SOURCE_CD ,
      IN_TYPE_CD ,
      IN_SUBTYPE ,
      CDT_ADV_TYP ,
      CURRENCY_CODE ,
      AMOUNT ,
      DBT_ID ,
      DBT_NAME1 ,
      DBT_BNK_INF1 ,
      DBT_BNK_INF2 ,
      DBT_BNK_INF3 ,
      DBT_BNK_INF4 ,
      DBT_CURRENCY ,
      DBT_AMOUNT ,
      DBT_SEC_ACCTG_AMT ,
      BBK_ID ,
      BBK_IDTYPE ,
      BBK_NAME1 ,
      BNP_ID ,
      BNP_NAME1 ,
      CDT_ID ,
      CDT_NAME1 ,
      CDT_CURRENCY ,
      CDT_AMOUNT ,
      CANCELLED_FLAG ,
      INDA_RANK_REL_REFN,
      INDA_AMH_FLTR ,
      INDA_MTCH_CCP2P ,
      DIFF_TRAN_AMT ,
      DIFF_TRAN_DATE,
      INDA_OFFSYST_BSB ,
      BULK_BSB_NORM ,
      BULK_ACCT_NORM ,
      INDA_RETN ,
      INDA_REJT ,
      DE_BSB_NUMB ,
      DE_ACCT_NUMB ,
      DE_INDA_RTN ,
      INDA_MTCH_DE ,
      RESP_BENEFICIARY_INSTITUTION_NAME ,
      RESP_RELATED_REFERENCE ,
      RESP_RECEIVING_BANK ,
      RESP_PAYMENT_CURRENCY_CODE ,
      RESP_PAYMENT_AMOUNT ,
      CDT_ADV_TYP_CHNL ,
      RESP_CHANNEL ,
      RESP_TRANSFER_DATE ,
      INDA_DUPL_ACCT ,
      ACCT_I ,
      PATY_ACCT_REL_M ,
      PATY_I ,
      PATY_TYPE_C ,
      ADRS_I ,
      CLSE_D ,
      MAX_REL_EXPY_D ,
      MAX_ACCT_M_EXPY_D ,
      PATY_LIFE_CYCL_EXPY_D ,
      ACCT_NAME ,
      ACCOUNT_NAME_2 ,
      SEQN_NUMB,
      FRST_M ,
      SCND_M ,
      SRNM_M ,
      PATY_LIFE_CYCL_D ,
      BUSN_NAME ,
      ADRS_CATG_C ,
      ADRS_QUAL_C ,
      ADRS_LINE_1_X ,
      ADRS_LINE_2_X ,
      SURB_X ,
      CITY_X ,
      PCOD_C ,
      STAT_C ,
      IDNN_VALU_X_ABN ,
      IDNN_VALU_X_ACN ,
      INDA_CBA_ACCT ,
      BWA_SRCE_SYST_ACCT ,
      BWA_PATY_TYPE_C ,
      BWA_ACCT_I ,
      BWA_PATY_ACCT_REL_C ,
      BWA_ORGN_TYPE_X ,
      BWA_PATY_I ,
      BWA_ADRS_I ,
      BWA_ACCT_NAME ,
      BWA_SEQN_NUMB,
      BWA_SRNM_M ,
      BWA_FRST_M ,
      BWA_SCND_M ,
      BWA_PATY_LIFE_CYCL_D ,
      BWA_BUSN_NAME ,
      BWA_IDNN_VALU_X_ABN ,
      BWA_IDNN_VALU_X_ACN ,
      BWA_ADRS_LINE_1_X ,
      BWA_ADRS_LINE_2_X ,
      BWA_SURB_X ,
      BWA_STAT_C ,
      BWA_PCOD_C ,
      BWA_ISO_CNTY_C ,
      RESP_EXCEPTIONS ,
      RESP_BSB_NO ,
      RESP_ACCOUNT_NO ,
      RESP_ACCOUNT_NAME ,
      RESP_SEQUENCE_NO,
      RESP_FAMILY_NAME ,
      RESP_FIRST_GIVEN_NAME ,
      RESP_SECOND_GIVEN_NAME ,
      RESP_BIRTH_DATE ,
      RESP_BUSINESS_NAME ,
      RESP_ADDRESS_LINE_1 ,
      RESP_ADDRESS_LINE_2 ,
      RESP_SUBURB ,
      RESP_STATE_INMD ,
      RESP_STATE ,
      RESP_POSTCODE ,
      RESP_ABN ,
      RESP_ACN ,
      UID2 ,
      INDA_ONWD_TRAN ,
      INDA_MTCH_BWA_MANL_RQST ,
      INDA_MTCH_BWA_MANL_RESP ,
      ENUS_REVW_STUS ,
      ENUS_CMMT ,
      ENUS_EXC_REFN ,
      ENUS_MODF_BY ,
      ENUS_REVW_DTTM,
      SUPV_REVW_STUS ,
      SUPV_MODF_BY ,
      SUPV_REVW_DTTM
	  )
VALUES
(
	 'QTLR_RPRT_EXC_UI'
	,USER
	,CURRENT_TIMESTAMP(6)
	,'UPDATE' , 
		  N.UID ,
      N.CLYR ,
      N.QRTR ,
      N.NOTC ,
      N.REPORTING_ENTITY_NO ,
      N.REPORTING_ENTITY_NAME ,
      N.REPORT_NO ,
      N.TRN ,
      N.TRANSACTION_VALUE_DATE ,
      N.TRANSACTION_CURRENCY_CODE ,
      N.TRANSACTION_AMOUNT ,
      N.BENEFICIARY_CUSTOMER ,
      N.ORDERING_INSTITUTION ,
      N.HEADER_BLOCKS ,
      N.BSB_NO ,
      N.ACCOUNT_NO ,
      N.ACCOUNT_NAME ,
      N.SEQUENCE_NO ,
      N.FAMILY_NAME ,
      N.FIRST_GIVEN_NAME ,
      N.SECOND_GIVEN_NAME ,
      N.BIRTH_DATE ,
      N.BUSINESS_NAME ,
      N.ADDRESS_LINE_1 ,
      N.ADDRESS_LINE_2 ,
      N.SUBURB ,
      N.STATE ,
      N.POSTCODE ,
      N.ABN ,
      N.ACN ,
      N.EXCEPTIONS ,
      N.BENEFICIARY_INSTITUTION_NAME ,
      N.RELATED_REFERENCE ,
      N.RECEIVING_BANK ,
      N.PAYMENT_CURRENCY_CODE ,
      N.PAYMENT_AMOUNT ,
      N.CHANNEL ,
      N.TRANSFER_DATE ,
      N.HEAD_BLOK_1 ,
      N.HEAD_BLOK_2 ,
      N.SWF_CODE ,
      N.PROS_ENTY ,
      N.INDA_BWA_NON_AUD ,
      N.TRN_DATE ,
      N.TRN_NUMBER ,
      N.TRN_TIMESTAMP,
      N.PAY_DATE ,
      N.CAN_MEMO ,
      N.TRADER_CTRL ,
      N.SWIFT_ID ,
      N.CDT_WIR_ID ,
      N.CDT_SPC_INST1 ,
      N.SBK_REF_NUM ,
      N.MESSAGE_TEXT ,
      N.SOURCE_CD ,
      N.IN_TYPE_CD ,
      N.IN_SUBTYPE ,
      N.CDT_ADV_TYP ,
      N.CURRENCY_CODE ,
      N.AMOUNT ,
      N.DBT_ID ,
      N.DBT_NAME1 ,
      N.DBT_BNK_INF1 ,
      N.DBT_BNK_INF2 ,
      N.DBT_BNK_INF3 ,
      N.DBT_BNK_INF4 ,
      N.DBT_CURRENCY ,
      N.DBT_AMOUNT ,
      N.DBT_SEC_ACCTG_AMT ,
      N.BBK_ID ,
      N.BBK_IDTYPE ,
      N.BBK_NAME1 ,
      N.BNP_ID ,
      N.BNP_NAME1 ,
      N.CDT_ID ,
      N.CDT_NAME1 ,
      N.CDT_CURRENCY ,
      N.CDT_AMOUNT ,
      N.CANCELLED_FLAG ,
      N.INDA_RANK_REL_REFN,
      N.INDA_AMH_FLTR ,
      N.INDA_MTCH_CCP2P ,
      N.DIFF_TRAN_AMT ,
      N.DIFF_TRAN_DATE,
      N.INDA_OFFSYST_BSB ,
      N.BULK_BSB_NORM ,
      N.BULK_ACCT_NORM ,
      N.INDA_RETN ,
      N.INDA_REJT ,
      N.DE_BSB_NUMB ,
      N.DE_ACCT_NUMB ,
      N.DE_INDA_RTN ,
      N.INDA_MTCH_DE ,
      N.RESP_BENEFICIARY_INSTITUTION_NAME ,
      N.RESP_RELATED_REFERENCE ,
      N.RESP_RECEIVING_BANK ,
      N.RESP_PAYMENT_CURRENCY_CODE ,
      N.RESP_PAYMENT_AMOUNT ,
      N.CDT_ADV_TYP_CHNL ,
      N.RESP_CHANNEL ,
      N.RESP_TRANSFER_DATE ,
      N.INDA_DUPL_ACCT ,
      N.ACCT_I ,
      N.PATY_ACCT_REL_M ,
      N.PATY_I ,
      N.PATY_TYPE_C ,
      N.ADRS_I ,
      N.CLSE_D ,
      N.MAX_REL_EXPY_D ,
      N.MAX_ACCT_M_EXPY_D ,
      N.PATY_LIFE_CYCL_EXPY_D ,
      N.ACCT_NAME ,
      N.ACCOUNT_NAME_2 ,
      N.SEQN_NUMB,
      N.FRST_M ,
      N.SCND_M ,
      N.SRNM_M ,
      N.PATY_LIFE_CYCL_D ,
      N.BUSN_NAME ,
      N.ADRS_CATG_C ,
      N.ADRS_QUAL_C ,
      N.ADRS_LINE_1_X ,
      N.ADRS_LINE_2_X ,
      N.SURB_X ,
      N.CITY_X ,
      N.PCOD_C ,
      N.STAT_C ,
      N.IDNN_VALU_X_ABN ,
      N.IDNN_VALU_X_ACN ,
      N.INDA_CBA_ACCT ,
      N.BWA_SRCE_SYST_ACCT ,
      N.BWA_PATY_TYPE_C ,
      N.BWA_ACCT_I ,
      N.BWA_PATY_ACCT_REL_C ,
      N.BWA_ORGN_TYPE_X ,
      N.BWA_PATY_I ,
      N.BWA_ADRS_I ,
      N.BWA_ACCT_NAME ,
      N.BWA_SEQN_NUMB,
      N.BWA_SRNM_M ,
      N.BWA_FRST_M ,
      N.BWA_SCND_M ,
      N.BWA_PATY_LIFE_CYCL_D ,
      N.BWA_BUSN_NAME ,
      N.BWA_IDNN_VALU_X_ABN ,
      N.BWA_IDNN_VALU_X_ACN ,
      N.BWA_ADRS_LINE_1_X ,
      N.BWA_ADRS_LINE_2_X ,
      N.BWA_SURB_X ,
      N.BWA_STAT_C ,
      N.BWA_PCOD_C ,
      N.BWA_ISO_CNTY_C ,
      N.RESP_EXCEPTIONS ,
      N.RESP_BSB_NO ,
      N.RESP_ACCOUNT_NO ,
      N.RESP_ACCOUNT_NAME ,
      N.RESP_SEQUENCE_NO,
      N.RESP_FAMILY_NAME ,
      N.RESP_FIRST_GIVEN_NAME ,
      N.RESP_SECOND_GIVEN_NAME ,
      N.RESP_BIRTH_DATE ,
      N.RESP_BUSINESS_NAME ,
      N.RESP_ADDRESS_LINE_1 ,
      N.RESP_ADDRESS_LINE_2 ,
      N.RESP_SUBURB ,
      N.RESP_STATE_INMD ,
      N.RESP_STATE ,
      N.RESP_POSTCODE ,
      N.RESP_ABN ,
      N.RESP_ACN ,
      N.UID2 ,
      N.INDA_ONWD_TRAN ,
      N.INDA_MTCH_BWA_MANL_RQST ,
      N.INDA_MTCH_BWA_MANL_RESP ,
      N.ENUS_REVW_STUS ,
      N.ENUS_CMMT ,
      N.ENUS_EXC_REFN ,
      N.ENUS_MODF_BY ,
      N.ENUS_REVW_DTTM,
      N.SUPV_REVW_STUS ,
      N.SUPV_MODF_BY ,
      N.SUPV_REVW_DTTM
	  );

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Stream_Id </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Stream_Id
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Id_Log (
      Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Cfg_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Cfg_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Cand_Cfg
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Cfg_Hist
  (
     Cand_Source
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Source
    ,o.TDM_Job_Priority
    ,o.TDM_Job_Type
    ,o.TDM_Job_Source
    ,o.TDM_Job_Description
    ,o.TDM_Source_TDPID
    ,o.TDM_Source_User
    ,o.TDM_Source_Passwd_Encrypted
    ,o.TDM_Source_Logon_Mech
    ,o.TDM_Source_Logon_Mech_Data
    ,o.TDM_Source_Userid_Pool
    ,o.TDM_Source_Account_Id
    ,o.TDM_Source_Sessions
    ,o.TDM_Target_TDPID
    ,o.TDM_Target_User
    ,o.TDM_Target_Passwd_Encrypted
    ,o.TDM_Target_Logon_Mech
    ,o.TDM_Target_Logon_Mech_Data
    ,o.TDM_Target_Userid_Pool
    ,o.TDM_Target_Account_Id
    ,o.TDM_Target_Sessions
    ,o.TDM_Data_Streams
    ,o.TDM_Response_Timeout
    ,o.TDM_Overwrite_Existing_Obj
    ,o.TDM_Force_Utility
    ,o.TDM_Log_Level
    ,o.TDM_Online_Archive
    ,o.TDM_Freeze_Job_Steps
    ,o.TDM_Max_Agents_Per_Task
    ,o.TDM_Additional_ARC_Parameters
    ,o.TDM_Query_Band_Str
    ,o.TDM_Sync_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Stream_Param </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Stream_Param
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
      Stream_Key
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TD_Stream_Param </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TD_Stream_Param
AFTER DELETE ON PDGENTCF.CTLFW_T_Stream_Param
REFERENCING OLD ROW AS O  
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Stream_Param_Log (
    Stream_Key
    , Param_Group
    , Param_Name
    , Description
    , Param_Value
    , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
    o.Stream_Key
    , o.Param_Group
    , o.Param_Name
    , o.Description
    , o.Param_Value
    , o.Param_Cast
    , DATE
    , USER
    , TIME
    , CURRENT_TIMESTAMP(6) -  INTERVAL '0.000001' second /*Change Delete Trigger and change update_ts - 0.000001 second*/
  );
)
;

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Step_Cd_AIT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Step_Cd_AIT
  AFTER INSERT ON PDGDWPIIOBF.Dtf_Job_Step_Cd
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Step_Cd_Hst
  (
     Job_Step_No
    ,Job_Step_Nm
    ,Job_Step_Dscrp
    ,Modify_Action
   ) VALUES (
     n.Job_Step_No
    ,n.Job_Step_Nm
    ,n.Job_Step_Dscrp
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_Hist
  (
     Log_Date
    ,Log_TS
    ,The_Count
    ,DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Gen_RI_SQL_Text
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Log_Date
    ,o.Log_TS
    ,o.The_Count
    ,o.DB_Name
    ,o.Obj_Name
    ,o.Col_1_Name
    ,o.Col_2_Name
    ,o.Join_Col_1_Name
    ,o.Join_Col_2_Name
    ,o.Predicate_Clause_Text
    ,o.Join_Type
    ,o.Rltd_DB_Name
    ,o.Rltd_Obj_Name
    ,o.Rltd_Col_1_Name
    ,o.Rltd_Col_2_Name
    ,o.Rltd_Join_Col_1_Name
    ,o.Rltd_Join_Col_2_Name
    ,o.Rltd_Predicate_Clause_Text
    ,o.Gen_RI_SQL_Text
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Transform_KeyCol </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Transform_KeyCol
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,Key_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.Key_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_ADMT_TRIG_I </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_ADMT_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.IFTI_XML_ADMT
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_ADMT_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
FILENAME,
RPRT_CNT,
IFTI_XML,
FILE_COUR,
CRET_BY,
CRET_DTTM,
FILE_RNAM_BY,
FILE_RNAM_DTTM,
APRV_BY,
APRV_DTTM,
RCPT_N,
AUSTC_REFN,
SUBM_DT,
ENUS_CMMT,
REVER_OFCR_CMMT,
REVER_OFCR,
REVER_OFCR_DTTM,
VERF_OFCR_CMMT,
VERF_OFCR,
VERF_OFCR_DTTM,
QA_OFCR_CMMT,
QA_OFCR,
QA_OFCR_DTTM,
IS_REVIEWED
)
VALUES
(
'IFTI_XML_ADMT',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.FILENAME,
n.RPRT_CNT,
n.IFTI_XML,
n.FILE_COUR,
n.CRET_BY,
n.CRET_DTTM,
n.FILE_RNAM_BY,
n.FILE_RNAM_DTTM,
n.APRV_BY,
n.APRV_DTTM,
n.RCPT_N,
n.AUSTC_REFN,
n.SUBM_DT,
n.ENUS_CMMT,
n.REVER_OFCR_CMMT,
n.REVER_OFCR,
n.REVER_OFCR_DTTM,
n.VERF_OFCR_CMMT,
n.VERF_OFCR,
n.VERF_OFCR_DTTM,
n.QA_OFCR_CMMT,
n.QA_OFCR,
n.QA_OFCR_DTTM,
n.IS_REVIEWED
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Load </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Load
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Load
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Load_Log 
 (
  Ctl_Id
  ,File_Id
  ,Business_Date
  ,Business_Date_Cycle_Num
  ,Business_Date_Cycle_Start_Ts
  ,File_Qualifier
  ,Process_Name
  ,Process_Id
  ,TD_Session_Id
  ,Tool_Session_Id
  ,S_Tbl_Name
  ,S_DB_Name
  ,Rows_Input
  ,Rows_Considered
  ,Rows_Not_Considered
  ,Rows_Rejected
  ,Rows_Inserted
  ,Rows_Updated
  ,Rows_Deleted
  ,Rows_ET
  ,Rows_UV
  ,Run_Date
  ,Start_Time
  ,End_Time
  ,Load_Status
  ,Update_Date
  ,Update_User
  ,Update_Time
  ,Update_Ts
  ) 
VALUES 
 (
  n.Ctl_Id
  ,n.File_Id
  ,n.Business_Date
  ,n.Business_Date_Cycle_Num
  ,n.Business_Date_Cycle_Start_Ts
  ,n.File_Qualifier
  ,n.Process_Name
  ,n.Process_Id
  ,n.TD_Session_Id
  ,n.Tool_Session_Id
  ,n.S_Tbl_Name
  ,n.S_DB_Name
  ,n.Rows_Input
  ,n.Rows_Considered
  ,n.Rows_Not_Considered
  ,n.Rows_Rejected
  ,n.Rows_Inserted
  ,n.Rows_Updated
  ,n.Rows_Deleted
  ,n.Rows_ET
  ,n.Rows_UV
  ,n.Run_Date
  ,n.Start_Time
  ,n.End_Time
  ,n.Load_Status
  ,n.Update_Date
  ,n.Update_User
  ,n.Update_Time
  ,n.Update_Ts
 );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */




-- Bankwest  Credit Update Trigger
--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_BWA_CR_TRIG_U
replace trigger U_D_DSV_001_NIT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_U
after update on U_D_DSV_001_NIT_1.NOST_BWA_CR_EXC_RPRT_ACCESS
referencing new row as n
for each row
insert into U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
values
(
 'NOST_BWA_CR_EXC_RPRT_ACCESS'
,user
,current_timestamp(6)
,'UPDATE'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TD_Stream_Param </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TD_Stream_Param
AFTER DELETE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING OLD ROW AS O  
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
      Stream_Key
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      o.Stream_Key
 , o.Param_Group
 , o.Param_Name
    , o.Description
 , o.Param_Value
 , o.Param_Cast
    , DATE              
   , USER              
    , TIME             
  , CURRENT_TIMESTAMP(6)

  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Repl_Type_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Repl_Type_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Repl_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Repl_Type_Hist
  (
     Repl_Type_Cd
    ,Repl_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Repl_Type_Cd
    ,n.Repl_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Process_Param </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Process_Param
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Param_Log (
      Process_Name
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFWX_TI_Xfm_Process </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFWX_TI_Xfm_Process
AFTER INSERT ON PDGENTCF.CTLFWX_T_Xfm_Process
REFERENCING NEW ROW AS NR
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFWX_T_Xfm_Process_Log (
      Process_Name,           
      Busn_Dt ,         
      Xfm_Run_Dt ,                        
      Xfm_Rows_Inserted ,                       
      Xfm_Stg_Ld_Status ,           
      Xfm_Stg_Ld_Start_Time ,
      Xfm_Stg_Ld_End_Time ,         
      Xfm_Stg_Val_Start_Time ,
      Xfm_Stg_Val_End_Time ,
      Msur_Mdl_Pub_Run_Dt ,         
      Msur_Mdl_Pub_Process_Id ,           
      Msur_Mdl_Pub_Start_Time ,           
      Msur_Mdl_Pub_End_Time ,
      Msur_Mdl_Pub_Status ,   
      Msur_Mdl_Pub_Priority , 
      Pub_Stream_Key ,
      Update_User ,           
      Update_Ts 
  ) VALUES (
      NR.Process_Name,        
      NR.Busn_Dt ,            
      NR.Xfm_Run_Dt ,                     
      NR.Xfm_Rows_Inserted ,                    
      NR.Xfm_Stg_Ld_Status ,        
      NR.Xfm_Stg_Ld_Start_Time ,
      NR.Xfm_Stg_Ld_End_Time ,      
      NR.Xfm_Stg_Val_Start_Time ,
      NR.Xfm_Stg_Val_End_Time ,           
      NR.Msur_Mdl_Pub_Run_Dt ,            
      NR.Msur_Mdl_Pub_Process_Id ,        
      NR.Msur_Mdl_Pub_Start_Time ,        
      NR.Msur_Mdl_Pub_End_Time ,
      NR.Msur_Mdl_Pub_Status ,      
      NR.Msur_Mdl_Pub_Priority ,    
      NR.Pub_Stream_Key ,
      NR.Update_User ,        
      NR.Update_Ts 
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.TU_BKey_Domain </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.TU_BKey_Domain
AFTER UPDATE ON PDGDWPIIOBF.BKey_Domain
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGDWPIIOBF.BKey_Domain_Log (
      Key_Set_Id
    , Domain_Id
    , Description
    , Update_Date
    , Update_User
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Domain_Id
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Obj_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Obj_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDM_Job_Obj
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Hist
  (
     Job_Cd
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.Db_Nm
    ,o.Obj_Nm
    ,o.Obj_Type
    ,o.Copy_Stat
    ,o.Compare_DDL
    ,o.Where_Clause
    ,o.Key_Column
    ,o.Query_Band_Str
    ,o.Job_Desc
    ,o.Staging_Database_Nm
    ,o.Enabled_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFWX_TI_Xfm_Process </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFWX_TI_Xfm_Process
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process
REFERENCING NEW ROW AS NR
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process_Log (
      Process_Name,           
      Busn_Dt ,         
      Xfm_Run_Dt ,                        
      Xfm_Rows_Inserted ,                       
      Xfm_Stg_Ld_Status ,           
      Xfm_Stg_Ld_Start_Time ,
      Xfm_Stg_Ld_End_Time ,         
      Xfm_Stg_Val_Start_Time ,
      Xfm_Stg_Val_End_Time ,
      Msur_Mdl_Pub_Run_Dt ,         
      Msur_Mdl_Pub_Process_Id ,           
      Msur_Mdl_Pub_Start_Time ,           
      Msur_Mdl_Pub_End_Time ,
      Msur_Mdl_Pub_Status ,   
      Msur_Mdl_Pub_Priority , 
      Pub_Stream_Key ,
      Update_User ,           
      Update_Ts 
  ) VALUES (
      NR.Process_Name,        
      NR.Busn_Dt ,            
      NR.Xfm_Run_Dt ,                     
      NR.Xfm_Rows_Inserted ,                    
      NR.Xfm_Stg_Ld_Status ,        
      NR.Xfm_Stg_Ld_Start_Time ,
      NR.Xfm_Stg_Ld_End_Time ,      
      NR.Xfm_Stg_Val_Start_Time ,
      NR.Xfm_Stg_Val_End_Time ,           
      NR.Msur_Mdl_Pub_Run_Dt ,            
      NR.Msur_Mdl_Pub_Process_Id ,        
      NR.Msur_Mdl_Pub_Start_Time ,        
      NR.Msur_Mdl_Pub_End_Time ,
      NR.Msur_Mdl_Pub_Status ,      
      NR.Msur_Mdl_Pub_Priority ,    
      NR.Pub_Stream_Key ,
      NR.Update_User ,        
      NR.Update_Ts 
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_QA_CMMT </sc-trigger> */
replace TRIGGER U_D_DSV_001_NFT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_QA_CMMT 
AFTER UPDATE OF QA_OFCR_CMMT ON U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_IDENTIFIER,
            QA_OFCR,
            QA_OFCR_DTTM,
            QA_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_IDENTIFIER,
            COALESCE (NewRow.QA_OFCR, NULL, CURRENT_USER),
            COALESCE (NewRow.QA_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
            NewRow.QA_OFCR_CMMT
            );

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Process_Type_Param </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Process_Type_Param
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param_Log (
      Process_Type
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.TRI_UPDATE_tbl_Hugo_HIST </sc-trigger> */
create TRIGGER U_D_DSV_001_NIT_1.TRI_UPDATE_tbl_Hugo_HIST
AFTER UPDATE ON U_D_DSV_001_NIT_1.tbl_Hugo
	REFERENCING OLD AS OldRow NEW as NewRow
	FOR EACH ROW
		Update U_D_DSV_001_NIT_1.tbl_Hugo_HIST
		SET col1 = COALESCE (col1, NULL, '') || NewRow.col1||  x'0A', col2 = COALESCE (col2, NULL, '') || NewRow.col2||  x'0A' 
		Where int_col = NewRow.int_col;

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_CE_System </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_CE_System
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_CE_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_CE_System_Log (
      CE_Id
    , Ctl_Id
    , CE_Ctl_Rltnshp_Type
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Ctl_Id
    , n.CE_Ctl_Rltnshp_Type
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_Hist
  (
     Log_Id
    ,Prog_Host
    ,Prog_Name
    ,Func_Name
    ,Proc_Id
    ,Mesg_Type
    ,Mesg_Body
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Prog_Mode
    ,Prog_Vers
    ,Send_Alert_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Log_Id
    ,o.Prog_Host
    ,o.Prog_Name
    ,o.Func_Name
    ,o.Proc_Id
    ,o.Mesg_Type
    ,o.Mesg_Body
    ,o.Prog_Stat_Cd
    ,o.Prog_Stat_Desc
    ,o.Prog_Mode
    ,o.Prog_Vers
    ,o.Send_Alert_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_System </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_System
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_System_Log (
      Ctl_Id
    , System_Name
    , Path_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.System_Name
    , n.Path_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Process_Type </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Process_Type
AFTER INSERT ON PDGENTCF.CTLFW_T_Process_Type
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Process_Type_Log (
      Process_Type
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr_AIT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr_AIT
  AFTER INSERT ON PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Obj_Refresh_Mstr_Hst
  (
     RfshGroupNm
 ,TgtDatabaseNm
 ,SrcDatabaseNm
 ,SrcTableNm
 ,SrcTableCurrPerm
 ,SrcTablePredicateInfo
 ,SrcKeyColumn
 ,SelectionCriteriaRuleNbr
 ,SuggestedUtility
 ,TruncateTgtTable
 ,RfshGroupEntryTimestamp
 ,RfshGroupEntryStatus
    ,Modify_Action
   ) VALUES (
     n.RfshGroupNm
 ,n.TgtDatabaseNm
 ,n.SrcDatabaseNm
 ,n.SrcTableNm
 ,n.SrcTableCurrPerm
 ,n.SrcTablePredicateInfo
 ,n.SrcKeyColumn
 ,n.SelectionCriteriaRuleNbr
 ,n.SuggestedUtility
 ,n.TruncateTgtTable
 ,n.RfshGroupEntryTimestamp
 ,n.RfshGroupEntryStatus
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_Hist
  (
     Cand_Obj_Id
    ,Related_Cand_Obj_Id
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Id
    ,n.Related_Cand_Obj_Id
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Process_Type_Param </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Process_Type_Param
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param_Log (
      Process_Type
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_ADMT_TRIG_D </sc-trigger> */

Replace TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_ADMT_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.IFTI_XML_ADMT
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_ADMT_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
FILENAME,
RPRT_CNT,
IFTI_XML,
FILE_COUR,
CRET_BY,
CRET_DTTM,
FILE_RNAM_BY,
FILE_RNAM_DTTM,
APRV_BY,
APRV_DTTM,
RCPT_N,
AUSTC_REFN,
SUBM_DT,
ENUS_CMMT,
REVER_OFCR_CMMT,
REVER_OFCR,
REVER_OFCR_DTTM,
VERF_OFCR_CMMT,
VERF_OFCR,
VERF_OFCR_DTTM,
QA_OFCR_CMMT,
QA_OFCR,
QA_OFCR_DTTM,
IS_REVIEWED
)
VALUES
(
'IFTI_XML_ADMT',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.FILENAME,
n.RPRT_CNT,
n.IFTI_XML,
n.FILE_COUR,
n.CRET_BY,
n.CRET_DTTM,
n.FILE_RNAM_BY,
n.FILE_RNAM_DTTM,
n.APRV_BY,
n.APRV_DTTM,
n.RCPT_N,
n.AUSTC_REFN,
n.SUBM_DT,
n.ENUS_CMMT,
n.REVER_OFCR_CMMT,
n.REVER_OFCR,
n.REVER_OFCR_DTTM,
n.VERF_OFCR_CMMT,
n.VERF_OFCR,
n.VERF_OFCR_DTTM,
n.QA_OFCR_CMMT,
n.QA_OFCR,
n.QA_OFCR_DTTM,
n.IS_REVIEWED
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Stream </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Stream
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Stream
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Stream_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Stream_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYE_TRIG_U </sc-trigger> */


--update
--DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYE_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYE_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYE
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payeeSameAsSwiftBenCust,
payeeFullName,
payeeAddr,
payeeSuburb,
payeeState,
payeePostcode,
payeeCountry,
payeeAccountBSB1,
payeeAccountNumber1,
payeeAccountBSB2,
payeeAccountNumber2,
payeeAccountBSB3,
payeeAccountNumber3,
payeeIdentificationType1,
payeeIdentificationTypeOther1,
payeeIdentificationNumber1,
payeeIdentificationIssuer1,
payeeIdentificationCountry1,
payeeIdentificationType2,
payeeIdentificationTypeOther2,
payeeIdentificationNumber2,
payeeIdentificationIssuer2,
payeeIdentificationCountry2,
payeeIdentificationType3,
payeeIdentificationTypeOther3,
payeeIdentificationNumber3,
payeeIdentificationIssuer3,
payeeIdentificationCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_PAYE',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payeeSameAsSwiftBenCust,
n.payeeFullName,
n.payeeAddr,
n.payeeSuburb,
n.payeeState,
n.payeePostcode,
n.payeeCountry,
n.payeeAccountBSB1,
n.payeeAccountNumber1,
n.payeeAccountBSB2,
n.payeeAccountNumber2,
n.payeeAccountBSB3,
n.payeeAccountNumber3,
n.payeeIdentificationType1,
n.payeeIdentificationTypeOther1,
n.payeeIdentificationNumber1,
n.payeeIdentificationIssuer1,
n.payeeIdentificationCountry1,
n.payeeIdentificationType2,
n.payeeIdentificationTypeOther2,
n.payeeIdentificationNumber2,
n.payeeIdentificationIssuer2,
n.payeeIdentificationCountry2,
n.payeeIdentificationType3,
n.payeeIdentificationTypeOther3,
n.payeeIdentificationNumber3,
n.payeeIdentificationIssuer3,
n.payeeIdentificationCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Process_Type </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Process_Type
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Type
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Log (
      Process_Type
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Repl_Type_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Repl_Type_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Repl_Type
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Repl_Type_Hist
  (
     Repl_Type_Cd
    ,Repl_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Repl_Type_Cd
    ,o.Repl_Type_Desc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Process_Type </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Process_Type
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Type
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Log (
      Process_Type
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_System_File_Param </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_System_File_Param
AFTER UPDATE ON PDGENTCF.CTLFW_T_System_File_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_System_File_Param_Log (
      Ctl_Id
 , File_Id
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
 , n.File_Id
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Step_Parm_AIT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Step_Parm_AIT
  AFTER INSERT ON PDGDWPIIOBF.Dtf_Job_Step_Parm
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Step_Parm_Hst
  (
     Job_Cd
    ,Job_Step_No
    ,Job_Step_Seq_No
    ,Job_Step_Parm
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Job_Step_No
    ,n.Job_Step_Seq_No
    ,n.Job_Step_Parm
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Sys_Name
    ,Update_Tally
    ,Elapsed_Secs
    ,Key_Type1
    ,Key_Type2
    ,Key_Type3
    ,Key_Value
    ,Key_Where_Flag
    ,Key_Where_Val
    ,Key_Where_Cond
    ,Key_Partition_Flag
    ,Key_Partition_Val
    ,Key_Partition_Col
    ,Prev_Create_Date
    ,Prev_Create_Ts
    ,Is_Current_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Db_Name
    ,o.Obj_Name
    ,o.Obj_Type
    ,o.Sys_Name
    ,o.Update_Tally
    ,o.Elapsed_Secs
    ,o.Key_Type1
    ,o.Key_Type2
    ,o.Key_Type3
    ,o.Key_Value
    ,o.Key_Where_Flag
    ,o.Key_Where_Val
    ,o.Key_Where_Cond
    ,o.Key_Partition_Flag
    ,o.Key_Partition_Val
    ,o.Key_Partition_Col
    ,o.Prev_Create_Date
    ,o.Prev_Create_Ts
    ,o.Is_Current_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Stream </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Stream
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Stream
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Stream_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Stream_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_VERF_CMMT </sc-trigger> */
Create TRIGGER U_D_DSV_001_NFT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_VERF_CMMT 
AFTER UPDATE OF VERF_OFCR_CMMT ON U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_IDENTIFIER,
            VERF_OFCR,
            VERF_OFCR_DTTM,
            VERF_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_IDENTIFIER,
            COALESCE (NewRow.VERF_OFCR, NULL, CURRENT_USER),
            COALESCE (NewRow.VERF_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
            NewRow.VERF_OFCR_CMMT
            );

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Process_Type </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Process_Type
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Type
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Log (
      Process_Type
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST_TRIG_D </sc-trigger> */
-- DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_INST
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendingInstnCode,
sendingInstnName,
sendingInstnCity,
sendingInstnCountry,
sendingInstnBranchBSB,
sendingInstnBranchId,
sendingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_INST',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendingInstnCode,
n.sendingInstnName,
n.sendingInstnCity,
n.sendingInstnCountry,
n.sendingInstnBranchBSB,
n.sendingInstnBranchId,
n.sendingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_System_File </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_System_File
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_System_File
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_System_File_Log (
      Ctl_Id
    , File_Id
    , Data_File_Name
    , Ctl_File_Name
    , Queue_Name
    , Description
    , Data_File_Suffix
    , Ctl_File_Suffix
    , Data_File_Type
    , ET_Tolerance
    , UV_Tolerance
    , Sessions
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Queue_Name
    , n.Description
    , n.Data_File_Suffix
    , n.Ctl_File_Suffix
    , n.Data_File_Type
    , n.ET_Tolerance
    , n.UV_Tolerance
    , n.Sessions
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus_Hist
  (
     LogDate
    ,NumRows
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.LogDate
    ,n.NumRows
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Process </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Process
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Log (
      Process_Name
    , Description
    , Process_Type
    , Ctl_Id
    , Stream_Key
    , In_DB_Name
    , In_Object_Name
    , Out_DB_Name
    , Out_Object_Name
    , Target_TableDatabaseName
    , Target_TableName
    , Temp_DatabaseName
    , Key_Set_Id
    , Domain_Id
    , Code_Set_Id
    , Collect_Stats
    , Truncate_Target
    , Verification_Flag
    , File_Qualifier_Reset_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Description
    , n.Process_Type
    , n.Ctl_Id
    , n.Stream_Key
    , n.In_DB_Name
    , n.In_Object_Name
    , n.Out_DB_Name
    , n.Out_Object_Name
    , n.Target_TableDatabaseName
    , n.Target_TableName
    , n.Temp_DatabaseName
    , n.Key_Set_Id
    , n.Domain_Id
    , n.Code_Set_Id
    , n.Collect_Stats
    , n.Truncate_Target
    , n.Verification_Flag
    , n.File_Qualifier_Reset_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Load </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Load
AFTER UPDATE ON PDGENTCF.CTLFW_T_Load
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Load_Log 
 (
  Ctl_Id
  ,File_Id
  ,Business_Date
  ,Business_Date_Cycle_Num
  ,Business_Date_Cycle_Start_Ts
  ,File_Qualifier
  ,Process_Name
  ,Process_Id
  ,TD_Session_Id
  ,Tool_Session_Id
  ,S_Tbl_Name
  ,S_DB_Name
  ,Rows_Input
  ,Rows_Considered
  ,Rows_Not_Considered
  ,Rows_Rejected
  ,Rows_Inserted
  ,Rows_Updated
  ,Rows_Deleted
  ,Rows_ET
  ,Rows_UV
  ,Run_Date
  ,Start_Time
  ,End_Time
  ,Load_Status
  ,Update_Date
  ,Update_User
  ,Update_Time
  ,Update_Ts
   ) 
  VALUES 
   (
  n.Ctl_Id
  ,n.File_Id
  ,n.Business_Date
  ,n.Business_Date_Cycle_Num
  ,n.Business_Date_Cycle_Start_Ts
  ,n.File_Qualifier
  ,n.Process_Name
  ,n.Process_Id
  ,n.TD_Session_Id
  ,n.Tool_Session_Id
  ,n.S_Tbl_Name
  ,n.S_DB_Name
  ,n.Rows_Input
  ,n.Rows_Considered
  ,n.Rows_Not_Considered
  ,n.Rows_Rejected
  ,n.Rows_Inserted
  ,n.Rows_Updated
  ,n.Rows_Deleted
  ,n.Rows_ET
  ,n.Rows_UV
  ,n.Run_Date
  ,n.Start_Time
  ,n.End_Time
  ,n.Load_Status
  ,n.Update_Date
  ,n.Update_User
  ,n.Update_Time
  ,n.Update_Ts
   );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_AUT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_AUT
  AFTER UPDATE ON PDGDWPIIOBF.Dtf_Job
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Hst
  (
     Job_Id
    ,Job_Strt_Dt
    ,Job_Strt_Tm
    ,Job_End_Dt
    ,Job_End_Tm
    ,Job_Cd
    ,Stus_Cd
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Job_Strt_Dt
    ,n.Job_Strt_Tm
    ,n.Job_End_Dt
    ,n.Job_End_Tm
    ,n.Job_Cd
    ,n.Stus_Cd
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Scope_Stream_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Scope_Stream_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Scope_Stream
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Scope_Stream_Hist
  (
     Stream_Name
    ,Stream_Type
    ,Job_Name
    ,Enabled_Flag
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Name
    ,n.Stream_Type
    ,n.Job_Name
    ,n.Enabled_Flag
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Start_Ts
    ,n.End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Process_Id </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Process_Id
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Id_Log (
      Process_Name
    , Process_Id
    , Process_State
    , Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Process_Id
    , n.Process_State
    , n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
additionalDetailsOfPayment,
additionalOtherDetails,
additionalInstnCode,
additionalInstnName,
additionalInstnCity,
additionalInstnCountry,
additionalInstnAccountBSB,
additionalInstnAccountNumber,
senderToReceiverInfo1,
senderToReceiverInfo2,
senderToReceiverInfo3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ADDN_DETL',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.additionalDetailsOfPayment,
n.additionalOtherDetails,
n.additionalInstnCode,
n.additionalInstnName,
n.additionalInstnCity,
n.additionalInstnCountry,
n.additionalInstnAccountBSB,
n.additionalInstnAccountNumber,
n.senderToReceiverInfo1,
n.senderToReceiverInfo2,
n.senderToReceiverInfo3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Stream </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Stream
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Stream
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Stream_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Stream_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */





REPLACE TRIGGER U_D_DSV_001_NIT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.VOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated
)
VALUES
(
 'VOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'DELETE'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Process_Type_Param </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Process_Type_Param
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param_Log (
      Process_Type
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Data_Rtntn_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Data_Rtntn_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Data_Rtntn
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Data_Rtntn_Hist
  (
     DB_Name
    ,Tbl_Name
    ,Col_Name
    ,Ret_Period
    ,Impacted_Obj_DB_Name
    ,Impacted_Obj_Name
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DB_Name
    ,n.Tbl_Name
    ,n.Col_Name
    ,n.Ret_Period
    ,n.Impacted_Obj_DB_Name
    ,n.Impacted_Obj_Name
    ,n.Is_Enabled
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Character_Set </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Character_Set
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Character_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Character_Set_Log (
      Character_Set_Name
    , Description
    , Max_Character_Length
    , Min_Character_Length
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Character_Set_Name
    , n.Description
    , n.Max_Character_Length
    , n.Min_Character_Length
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Stream_Param </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Stream_Param
AFTER UPDATE ON PDGENTCF.CTLFW_T_Stream_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Stream_Param_Log (
      Stream_Key
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Step_ADT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Step_ADT
  AFTER DELETE ON PDGDWPIIOBF.Dtf_Job_Step
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Step_Hst
  (
     Job_Id
    ,Job_Step_No
    ,Job_Step_Seq_No
    ,Job_Step_Strt_Ts
    ,Job_Step_End_Ts
    ,Job_Step_Sts_Cd
    ,Modify_Action
   ) VALUES (
     o.Job_Id
    ,o.Job_Step_No
    ,o.Job_Step_Seq_No
    ,o.Job_Step_Strt_Ts
    ,o.Job_Step_End_Ts
    ,o.Job_Step_Sts_Cd
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_NM
    ,TDM_Key_Column
    ,Pros_Isac_DB_NM
    ,ODS_Insert_Col_Flag
    ,ODS_Update_Col_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Nm
    ,n.Db_Nm
    ,n.Tbl_NM
    ,n.TDM_Key_Column
    ,n.Pros_Isac_DB_NM
    ,n.ODS_Insert_Col_Flag
    ,n.ODS_Update_Col_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_System </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_System
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_System_Log (
      Ctl_Id
    , System_Name
    , Path_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.System_Name
    , n.Path_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE_TRIG_I </sc-trigger> */
--DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE
REFERENCING NEW ROW AS n
FOR EACH ROW
WHEN (n.UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
filename,
txnRefNo ,
interceptFlag ,
isSwiftOrStructured,
swiftMsg ,
transferDate,
direction,
currency ,
currencyAmount,
valueDate ,    
RPRT_STUS ,
REVW_MODF_BY,
REVW_DTTM ,
SUPV_MODF_BY,
SUPV_DTTM ,
CRET_BY,
CRET_DTTM ,
INDA_DRFT ,
INDA_REDY,
INDA_APRD,
INDA_RWRK,
INDA_NOT_RPRT,
EXC_REAS ,
ENUS_CMMT ,
SDS_MSG_ID ,
SDS_MSG_SEQ,
base_transferDate,
base_direction,
base_currency ,
base_currencyAmount,
base_valueDate , 
INDA_PAYR,
INDA_PAYE,
INDA_ORDR_INST,
INDA_SEND_INST,
INDA_SEND_CORR,
INDA_BENE_INST,
INDA_RECE_INST,
INDA_RECE_CORR,
INDA_ADDN_DETL,       
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_BASE',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.filename,
n.txnRefNo ,
n.interceptFlag ,
n.isSwiftOrStructured,
n.swiftMsg ,
n.transferDate,
n.direction,
n.currency ,
n.currencyAmount,
n.valueDate ,    
n.RPRT_STUS ,
n.REVW_MODF_BY,
n.REVW_DTTM ,
n.SUPV_MODF_BY,
n.SUPV_DTTM ,
n.CRET_BY,
n.CRET_DTTM ,
n.INDA_DRFT ,
n.INDA_REDY,
n.INDA_APRD,
n.INDA_RWRK,
n.INDA_NOT_RPRT,
n.EXC_REAS ,
n.ENUS_CMMT ,
n.SDS_MSG_ID ,
n.SDS_MSG_SEQ,
n.base_transferDate,
n.base_direction,
n.base_currency ,
n.base_currencyAmount,
n.base_valueDate , 
n.INDA_PAYR,
n.INDA_PAYE,
n.INDA_ORDR_INST,
n.INDA_SEND_INST,
n.INDA_SEND_CORR,
n.INDA_BENE_INST,
n.INDA_RECE_INST,
n.INDA_RECE_CORR,
n.INDA_ADDN_DETL,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Stream_Param </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Stream_Param
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
      Stream_Key
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */




-- cba Tigger Delete
--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_CBA_DR_TRIG_D
REPLACE TRIGGER U_D_DSV_001_NIT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.NOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS o
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'DELETE'  
,o.unique_identifier             
,o.match_type                    
,o.Nostro_acct                   
,o.CURR_NOSTRO                   
,o.Group_area                    
,o.Acct_Name                     
,o.external_party_bic            
,o.value_date                    
,o.AMOUNT                        
,o.Message_Type                  
,o.message_type_desc             
,o.tag_61_stmt_line              
,o.internal_ref                  
,o.external_ref                  
,o.description                   
,o.department                    
,o.Investigation_Flag            
,o.WS_flag                       
,o.deal_number                   
,o.cust_number                   
,o.broker_name                   
,o.product_cat                   
,o.Department_Responsible        
,o.Reason                        
,o.Other_Comment                 
,o.Escalate                      
,o.IFTI_Reportable               
,o.Payor_Bank                    
,o.Payor_Details                 
,o.Payee_Bank                    
,o.Payee_Details                 
,o.Investigation_Status          
,o.MatchID                       
,o.deal_num_list                 
,o.Beneficiary                   
,o.Remark                        
,o.Settle                        
,o.Category_Correct              
,o.Updated                       
,o.msg_id                        
,o.MSG_TAG_SEQ                   
,o.CBA_BIC                       
,o.currency_tag62                
,o.date_tag_created              
,o.amount_matched                
,o.Buckets_f                     
,o.Bucket                        
,o.WS_GDW_source                 
,o.cust_code                     
,o.cust_type                     
,o.ext_ref_nr                    
,o.portfolio_no                  
,o.Acc                           
,o.Additional_Field              
,o.Pay_Meth                      
,o.swift_50                      
,o.Remittance_info               
,o.Enough_Data_For_IFTI          
,o.swf_File_Ref                  
,o.compliance_exclusion
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Process_Id </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Process_Id
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Id_Log (
      Process_Name
    , Process_Id
    , Process_State
    , Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Process_Id
    , n.Process_State
    , n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Job_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Job_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Job
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Job_Hist
  (
     Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Param_Group </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Param_Group
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Param_Group
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Param_Group_Log (
      Param_Group
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Param_Group
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_System_File </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_System_File
AFTER UPDATE ON PDGENTCF.CTLFW_T_System_File
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_System_File_Log (
      Ctl_Id
    , File_Id
    , Data_File_Name
    , Ctl_File_Name
    , Queue_Name
    , Description
    , Data_File_Suffix
    , Ctl_File_Suffix
    , Data_File_Type
    , ET_Tolerance
    , UV_Tolerance
    , Sessions
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Queue_Name
    , n.Description
    , n.Data_File_Suffix
    , n.Ctl_File_Suffix
    , n.Data_File_Type
    , n.ET_Tolerance
    , n.UV_Tolerance
    , n.Sessions
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Step_Cd_AUT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Step_Cd_AUT
  AFTER UPDATE ON PDGDWPIIOBF.Dtf_Job_Step_Cd
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Step_Cd_Hst
  (
     Job_Step_No
    ,Job_Step_Nm
    ,Job_Step_Dscrp
    ,Modify_Action
   ) VALUES (
     n.Job_Step_No
    ,n.Job_Step_Nm
    ,n.Job_Step_Dscrp
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Id_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Id_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDM_Job_Id
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Id_Hist
  (
     Job_Id
    ,Job_Stus_Cd
    ,Job_Start_Ts
    ,Job_End_Ts
    ,Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Id
    ,o.Job_Stus_Cd
    ,o.Job_Start_Ts
    ,o.Job_End_Ts
    ,o.Job_Cd
    ,o.UOW_Id
    ,o.Job_Priority
    ,o.Job_Type
    ,o.Job_Source
    ,o.Job_Description
    ,o.Source_TDPID
    ,o.Source_User
    ,o.Source_Passwd_Encrypted
    ,o.Source_Logon_Mechanism
    ,o.Source_Logon_Mechanism_Data
    ,o.Source_Userid_Pool
    ,o.Source_Account_Id
    ,o.Source_Sessions
    ,o.Target_TDPID
    ,o.Target_User
    ,o.Target_Passwd_Encrypted
    ,o.Target_Logon_Mechanism
    ,o.Target_Logon_Mechanism_Data
    ,o.Target_Userid_Pool
    ,o.Target_Account_Id
    ,o.Target_Sessions
    ,o.Data_Streams
    ,o.Response_Timeout
    ,o.Overwrite_Existing_Obj
    ,o.Force_Utility
    ,o.Log_Level
    ,o.Online_Archive
    ,o.Freeze_Job_Steps
    ,o.Max_Agents_Per_Task
    ,o.Additional_ARC_Parameters
    ,o.Query_Band_Str
    ,o.Sync_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_System_File_Extract </sc-trigger> */
REPLACE  TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_System_File_Extract
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_System_File_Extract
REFERENCING NEW ROW AS n
FOR EACH ROW (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_SFE_Log (
      Ctl_Id
    , File_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , File_Qualifier
    , Data_File_Name
    , Ctl_File_Name
    , Extraction_Date
    , Ext_Start_Time
    , Ext_End_Time
    , TD_Session_Id
    , Tool_Session_Id
    , Load_Status
    , Count_Source
    , Count_Target
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.File_Qualifier
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Extraction_Date
    , n.Ext_Start_Time
    , n.Ext_End_Time
    , n.TD_Session_Id
    , n.Tool_Session_Id
    , n.Load_Status
    , n.Count_Source
    , n.Count_Target
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_I </sc-trigger> */


-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL
 
 --------------------------------------------------------------------- 
 --DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_ADDN_DETL
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
additionalDetailsOfPayment,
additionalOtherDetails,
additionalInstnCode,
additionalInstnName,
additionalInstnCity,
additionalInstnCountry,
additionalInstnAccountBSB,
additionalInstnAccountNumber,
senderToReceiverInfo1,
senderToReceiverInfo2,
senderToReceiverInfo3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ADDN_DETL',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.additionalDetailsOfPayment,
n.additionalOtherDetails,
n.additionalInstnCode,
n.additionalInstnName,
n.additionalInstnCity,
n.additionalInstnCountry,
n.additionalInstnAccountBSB,
n.additionalInstnAccountNumber,
n.senderToReceiverInfo1,
n.senderToReceiverInfo2,
n.senderToReceiverInfo3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Process </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Process
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Log (
      Process_Name
    , Description
    , Process_Type
    , Ctl_Id
    , Stream_Key
    , In_DB_Name
    , In_Object_Name
    , Out_DB_Name
    , Out_Object_Name
    , Target_TableDatabaseName
    , Target_TableName
    , Temp_DatabaseName
    , Key_Set_Id
    , Domain_Id
    , Code_Set_Id
    , Collect_Stats
    , Truncate_Target
    , Verification_Flag
    , File_Qualifier_Reset_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Description
    , n.Process_Type
    , n.Ctl_Id
    , n.Stream_Key
    , n.In_DB_Name
    , n.In_Object_Name
    , n.Out_DB_Name
    , n.Out_Object_Name
    , n.Target_TableDatabaseName
    , n.Target_TableName
    , n.Temp_DatabaseName
    , n.Key_Set_Id
    , n.Domain_Id
    , n.Code_Set_Id
    , n.Collect_Stats
    , n.Truncate_Target
    , n.Verification_Flag
    , n.File_Qualifier_Reset_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */


REPLACE TRIGGER U_D_DSV_001_NIT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.VOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated        
)
VALUES
(
 'VOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'INSERT'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Character_Set </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Character_Set
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Character_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Character_Set_Log (
      Character_Set_Name
    , Description
    , Max_Character_Length
    , Min_Character_Length
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Character_Set_Name
    , n.Description
    , n.Max_Character_Length
    , n.Min_Character_Length
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Id_Step_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Id_Step_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDM_Job_Id_Step
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Id_Step_Hist
  (
     Job_Id
    ,Step_No
    ,Step_Start_Ts
    ,Step_End_Ts
    ,Step_Stus_Cd
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Final_Step_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Id
    ,o.Step_No
    ,o.Step_Start_Ts
    ,o.Step_End_Ts
    ,o.Step_Stus_Cd
    ,o.Step_Desc
    ,o.Step_Type
    ,o.Executable_Nm
    ,o.Executable_Db
    ,o.Executable_Type
    ,o.Executable_Param
    ,o.Executable_User
    ,o.Executable_Platform_Name
    ,o.Executable_BT_ET_Flag
    ,o.Enabled_Flag
    ,o.Final_Step_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Process_Id </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Process_Id
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Id_Log (
      Process_Name
    , Process_Id
    , Process_State
    , Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Process_Id
    , n.Process_State
    , n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Stream_BusDate </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Stream_BusDate
AFTER UPDATE ON PDGENTCF.CTLFW_T_Stream_BusDate
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Stream_BusDate_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Business_Date
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Business_Date
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.Dtf_Job_Detl_AUT </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.Dtf_Job_Detl_AUT
  AFTER UPDATE ON PDGDWPIIOBF.Dtf_Job_Detl
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO PDGDWPIIOBF.Dtf_Job_Detl_Hst
  (
     Job_Cd
    ,Job_Priority
    ,Source_Tdpid
    ,Target_Tdpid
    ,Source_User
    ,Source_Passwd
    ,Target_User
    ,Target_Passwd
    ,Max_Agents_Per_Task
    ,Data_Streams
    ,Source_Sessions
    ,Target_Sessions
    ,Overwrite_Existing_Objects
    ,Force_Utility
    ,Foreign_Server
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Staging_Database
    ,Staging_Database_For_Table
    ,Object_Include_Flag
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Job_Priority
    ,n.Source_Tdpid
    ,n.Target_Tdpid
    ,n.Source_User
    ,n.Source_Passwd
    ,n.Target_User
    ,n.Target_Passwd
    ,n.Max_Agents_Per_Task
    ,n.Data_Streams
    ,n.Source_Sessions
    ,n.Target_Sessions
    ,n.Overwrite_Existing_Objects
    ,n.Force_Utility
    ,n.Foreign_Server
    ,n.Log_Level
    ,n.Online_Archive
    ,n.Freeze_Job_Steps
    ,n.Staging_Database
    ,n.Staging_Database_For_Table
    ,n.Object_Include_Flag
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_Hist
  (
     Cand_Obj_Stus_Type_Cd
    ,Cand_Obj_Stus_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Stus_Type_Cd
    ,n.Cand_Obj_Stus_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Character_Set </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Character_Set
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Character_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Character_Set_Log (
      Character_Set_Name
    , Description
    , Max_Character_Length
    , Min_Character_Length
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Character_Set_Name
    , n.Description
    , n.Max_Character_Length
    , n.Min_Character_Length
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST_TRIG_I </sc-trigger> */

-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST
 --------------------------------------------------------------------- PAYR
 --DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_BENE_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
beneficiaryInstnCode,
beneficiaryInstnName,
beneficiaryInstnCity,
beneficiaryInstnCountry,
beneficiaryInstnBranchBSB,
beneficiaryInstnBranchId,
beneficiaryInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_BENE_INST',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.beneficiaryInstnCode,
n.beneficiaryInstnName,
n.beneficiaryInstnCity,
n.beneficiaryInstnCountry,
n.beneficiaryInstnBranchBSB,
n.beneficiaryInstnBranchId,
n.beneficiaryInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Param_Group </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Param_Group
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Param_Group
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Param_Group_Log (
      Param_Group
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Param_Group
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */




-- cba Tigger Delete
--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_CBA_CR_TRIG_D
REPLACE TRIGGER U_D_DSV_001_NIT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.NOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS o
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'DELETE'  
,o.unique_identifier             
,o.match_type                    
,o.Nostro_acct                   
,o.CURR_NOSTRO                   
,o.Group_area                    
,o.Acct_Name                     
,o.external_party_bic            
,o.value_date                    
,o.AMOUNT                        
,o.Message_Type                  
,o.message_type_desc             
,o.tag_61_stmt_line              
,o.internal_ref                  
,o.external_ref                  
,o.description                   
,o.department                    
,o.Investigation_Flag            
,o.WS_flag                       
,o.deal_number                   
,o.cust_number                   
,o.broker_name                   
,o.product_cat                   
,o.Department_Responsible        
,o.Reason                        
,o.Other_Comment                 
,o.Escalate                      
,o.IFTI_Reportable               
,o.Payor_Bank                    
,o.Payor_Details                 
,o.Payee_Bank                    
,o.Payee_Details                 
,o.Investigation_Status          
,o.MatchID                       
,o.deal_num_list                 
,o.Beneficiary                   
,o.Remark                        
,o.Settle                        
,o.Category_Correct              
,o.Updated                       
,o.msg_id                        
,o.MSG_TAG_SEQ                   
,o.CBA_BIC                       
,o.currency_tag62                
,o.date_tag_created              
,o.amount_matched                
,o.Buckets_f                     
,o.Bucket                        
,o.WS_GDW_source                 
,o.cust_code                     
,o.cust_type                     
,o.ext_ref_nr                    
,o.portfolio_no                  
,o.Acc                           
,o.Additional_Field              
,o.Pay_Meth                      
,o.swift_50                      
,o.Remittance_info               
,o.Enough_Data_For_IFTI          
,o.swf_File_Ref                  
,o.compliance_exclusion
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Transform_KeyCol </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Transform_KeyCol
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,Key_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.Key_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_Hist
  (
     Cand_Obj_Stus_Type_Cd
    ,Cand_Obj_Stus_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Stus_Type_Cd
    ,n.Cand_Obj_Stus_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Process_Type_Param </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Process_Type_Param
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param_Log (
      Process_Type
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Process_Param </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Process_Param
AFTER INSERT ON PDGENTCF.CTLFW_T_Process_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Process_Param_Log (
      Process_Name
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> PDGDWPIIOBF.TU_MaskingMetadataRequest </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.TU_MaskingMetadataRequest
AFTER UPDATE ON PDGDWPIIOBF.Masking_Metadata_Request
REFERENCING NEW TABLE AS n OLD TABLE AS o
FOR EACH STATEMENT  (
INSERT INTO PDGDWPIIOBF.Masking_Metadata_Request_Log
    (
        DatabaseName
        ,TableName
        ,ColumnName
        ,ColumnExpression
        ,MaskingExpression
        ,Comments
        ,LoggingType
        ,LoggingUser
        ,LoggingDate
        ,LoggingTimestamp 
    )
SELECT
        n.DatabaseName
        ,n.TableName
        ,n.ColumnName
        ,n.ColumnExpression
        ,n.MaskingExpression
        ,n.Comments
        ,'UPDATE_NEW'
        ,CURRENT_USER
        ,CURRENT_DATE
        ,CURRENT_TIMESTAMP(6)
FROM n
;
INSERT INTO PDGDWPIIOBF.Masking_Metadata_Request_Log
    (
        DatabaseName
        ,TableName
        ,ColumnName
        ,ColumnExpression
        ,MaskingExpression
        ,Comments
        ,LoggingType
        ,LoggingUser
        ,LoggingDate
        ,LoggingTimestamp 
    )
SELECT
        o.DatabaseName
        ,o.TableName
        ,o.ColumnName
        ,o.ColumnExpression
        ,o.MaskingExpression
        ,o.Comments
        ,'UPDATE_OLD'
        ,CURRENT_USER
        ,CURRENT_DATE
        ,CURRENT_TIMESTAMP(6)

FROM o
;
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_Hist
  (
     Cand_Obj_Id
    ,Related_Cand_Obj_Id
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Obj_Id
    ,o.Related_Cand_Obj_Id
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Transform_SurrKeyCol </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Transform_SurrKeyCol
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Transform_SurrKeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Transform_SKCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,SurrKey_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.SurrKey_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_EXC_CMMT </sc-trigger> */
Create TRIGGER U_D_DSV_001_NFT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_EXC_CMMT 
AFTER UPDATE OF OTHR_CMMT ON U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCESS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NFT_1.NOST_VOST_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_ID,
            EXC_OFCR,
            EXC_OFCR_DTTM,
            EXC_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_ID,
            COALESCE (NewRow.EXC_OFCR, NULL, CURRENT_USER),
            COALESCE (NewRow.EXC_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
            NewRow.OTHR_CMMT
            );

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Transform </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Transform
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Transform
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Transform_Log 
 (
 Process_Name
 ,Business_Date
 ,Business_Date_Cycle_Num
 ,Business_Date_Cycle_Start_Ts
 ,Process_Id
 ,TD_Session_Id
 ,Tool_Session_Id
 ,In_DB_Name
 ,In_Object_Name
 ,Out_DB_Name
 ,Out_Object_Name
 ,Rows_Inserted
 ,Rows_Updated
 ,Rows_Deleted
 ,Run_Date
 ,Start_Time
 ,End_Time
 ,Update_Date
 ,Update_User
 ,Update_Time
 ,Update_Ts
   ) 
  VALUES 
   (
 n.Process_Name
 ,n.Business_Date
 ,n.Business_Date_Cycle_Num
 ,n.Business_Date_Cycle_Start_Ts
 ,n.Process_Id
 ,n.TD_Session_Id
 ,n.Tool_Session_Id
 ,n.In_DB_Name
 ,n.In_Object_Name
 ,n.Out_DB_Name
 ,n.Out_Object_Name
 ,n.Rows_Inserted
 ,n.Rows_Updated
 ,n.Rows_Deleted
 ,n.Run_Date
 ,n.Start_Time
 ,n.End_Time
 ,n.Update_Date
 ,n.Update_User
 ,n.Update_Time
 ,n.Update_Ts
   );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYE_TRIG_I </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYE_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYE
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payeeSameAsSwiftBenCust,
payeeFullName,
payeeAddr,
payeeSuburb,
payeeState,
payeePostcode,
payeeCountry,
payeeAccountBSB1,
payeeAccountNumber1,
payeeAccountBSB2,
payeeAccountNumber2,
payeeAccountBSB3,
payeeAccountNumber3,
payeeIdentificationType1,
payeeIdentificationTypeOther1,
payeeIdentificationNumber1,
payeeIdentificationIssuer1,
payeeIdentificationCountry1,
payeeIdentificationType2,
payeeIdentificationTypeOther2,
payeeIdentificationNumber2,
payeeIdentificationIssuer2,
payeeIdentificationCountry2,
payeeIdentificationType3,
payeeIdentificationTypeOther3,
payeeIdentificationNumber3,
payeeIdentificationIssuer3,
payeeIdentificationCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_PAYE',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payeeSameAsSwiftBenCust,
n.payeeFullName,
n.payeeAddr,
n.payeeSuburb,
n.payeeState,
n.payeePostcode,
n.payeeCountry,
n.payeeAccountBSB1,
n.payeeAccountNumber1,
n.payeeAccountBSB2,
n.payeeAccountNumber2,
n.payeeAccountBSB3,
n.payeeAccountNumber3,
n.payeeIdentificationType1,
n.payeeIdentificationTypeOther1,
n.payeeIdentificationNumber1,
n.payeeIdentificationIssuer1,
n.payeeIdentificationCountry1,
n.payeeIdentificationType2,
n.payeeIdentificationTypeOther2,
n.payeeIdentificationNumber2,
n.payeeIdentificationIssuer2,
n.payeeIdentificationCountry2,
n.payeeIdentificationType3,
n.payeeIdentificationTypeOther3,
n.payeeIdentificationNumber3,
n.payeeIdentificationIssuer3,
n.payeeIdentificationCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Stream_Id </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Stream_Id
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Id_Log (
      Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDM_Job
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Hist
  (
     Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.UOW_Id
    ,n.Job_Priority
    ,n.Job_Type
    ,n.Job_Source
    ,n.Job_Description
    ,n.Source_TDPID
    ,n.Source_User
    ,n.Source_Passwd_Encrypted
    ,n.Source_Logon_Mechanism
    ,n.Source_Logon_Mechanism_Data
    ,n.Source_Userid_Pool
    ,n.Source_Account_Id
    ,n.Source_Sessions
    ,n.Target_TDPID
    ,n.Target_User
    ,n.Target_Passwd_Encrypted
    ,n.Target_Logon_Mechanism
    ,n.Target_Logon_Mechanism_Data
    ,n.Target_Userid_Pool
    ,n.Target_Account_Id
    ,n.Target_Sessions
    ,n.Data_Streams
    ,n.Response_Timeout
    ,n.Overwrite_Existing_Obj
    ,n.Force_Utility
    ,n.Log_Level
    ,n.Online_Archive
    ,n.Freeze_Job_Steps
    ,n.Max_Agents_Per_Task
    ,n.Additional_ARC_Parameters
    ,n.Query_Band_Str
    ,n.Sync_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Stream </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Stream
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Stream
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Stream_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Stream_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Process_Id </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Process_Id
AFTER UPDATE ON PDGENTCF.CTLFW_T_Process_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Process_Id_Log (
      Process_Name
    , Process_Id
    , Process_State
    , Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Process_Id
    , n.Process_State
    , n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGDWPIIOBF.TU_BKey_Key_Set </sc-trigger> */

--------------------------------------------------------------------------------

REPLACE TRIGGER PDGDWPIIOBF.TU_BKey_Key_Set
AFTER UPDATE ON PDGDWPIIOBF.BKey_Key_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGDWPIIOBF.BKey_Key_Set_Log (
      Key_Set_Id
    , Description
    , Key_Table_Name
    , Key_Table_DB_Name
    , Update_Date
    , Update_User
    , Update_Ts
  ) VALUES (
      n.Key_Set_Id
    , n.Description
    , n.Key_Table_Name
    , n.Key_Table_DB_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_Hist
  (
     Log_Id
    ,Prog_Host
    ,Prog_Name
    ,Func_Name
    ,Proc_Id
    ,Mesg_Type
    ,Mesg_Body
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Prog_Mode
    ,Prog_Vers
    ,Send_Alert_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Log_Id
    ,n.Prog_Host
    ,n.Prog_Name
    ,n.Func_Name
    ,n.Proc_Id
    ,n.Mesg_Type
    ,n.Mesg_Body
    ,n.Prog_Stat_Cd
    ,n.Prog_Stat_Desc
    ,n.Prog_Mode
    ,n.Prog_Vers
    ,n.Send_Alert_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_System_File_Param </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_System_File_Param
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_System_File_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_System_File_Param_Log (
      Ctl_Id
 , File_Id
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
 , n.File_Id
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR_TRIG_D </sc-trigger> */

-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendersCorrespondentCode1,
sendersCorrespondentName1,
sendersCorrespondentCity1,
sendersCorrespondentCountry1,
sendersCorrespondentCode2,
sendersCorrespondentName2,
sendersCorrespondentCity2,
sendersCorrespondentCountry2,
sendersCorrespondentCode3,
sendersCorrespondentName3,
sendersCorrespondentCity3,
sendersCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_CORR',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendersCorrespondentCode1,
n.sendersCorrespondentName1,
n.sendersCorrespondentCity1,
n.sendersCorrespondentCountry1,
n.sendersCorrespondentCode2,
n.sendersCorrespondentName2,
n.sendersCorrespondentCity2,
n.sendersCorrespondentCountry2,
n.sendersCorrespondentCode3,
n.sendersCorrespondentName3,
n.sendersCorrespondentCity3,
n.sendersCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Corporate_Entity </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Corporate_Entity
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity_Log (
      CE_Id
    , Corporate_Entity_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Corporate_Entity_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */



--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_BWA_DR_TRIG_U
REPLACE TRIGGER U_D_DSV_001_NIT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.VOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated
)
VALUES
(
 'VOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'UPDATE'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_System </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_System
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_System_Log (
      Ctl_Id
    , System_Name
    , Path_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.System_Name
    , n.Path_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Step_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Step_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Cand_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Step_Hist
  (
     Cand_Id
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Exec_Param_KVP
    ,Executable_User
    ,Executable_TDPID
    ,Executable_BT_ET_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Step_No
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Exec_Param_KVP
    ,n.Executable_User
    ,n.Executable_TDPID
    ,n.Executable_BT_ET_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Transform </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Transform
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Transform
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Transform_Log 
 (
 Process_Name
 ,Business_Date
 ,Business_Date_Cycle_Num
 ,Business_Date_Cycle_Start_Ts
 ,Process_Id
 ,TD_Session_Id
 ,Tool_Session_Id
 ,In_DB_Name
 ,In_Object_Name
 ,Out_DB_Name
 ,Out_Object_Name
 ,Rows_Inserted
 ,Rows_Updated
 ,Rows_Deleted
 ,Run_Date
 ,Start_Time
 ,End_Time
 ,Update_Date
 ,Update_User
 ,Update_Time
 ,Update_Ts
 ) 
VALUES 
 (
 n.Process_Name
 ,n.Business_Date
 ,n.Business_Date_Cycle_Num
 ,n.Business_Date_Cycle_Start_Ts
 ,n.Process_Id
 ,n.TD_Session_Id
 ,n.Tool_Session_Id
 ,n.In_DB_Name
 ,n.In_Object_Name
 ,n.Out_DB_Name
 ,n.Out_Object_Name
 ,n.Rows_Inserted
 ,n.Rows_Updated
 ,n.Rows_Deleted
 ,n.Run_Date
 ,n.Start_Time
 ,n.End_Time
 ,n.Update_Date
 ,n.Update_User
 ,n.Update_Time
 ,n.Update_Ts
 );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_CE_System </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_CE_System
AFTER UPDATE ON PDGENTCF.CTLFW_T_CE_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_CE_System_Log (
      CE_Id
    , Ctl_Id
    , CE_Ctl_Rltnshp_Type
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Ctl_Id
    , n.CE_Ctl_Rltnshp_Type
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj_Hist
  (
     Job_Id
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Id
    ,o.Db_Nm
    ,o.Obj_Nm
    ,o.Obj_Type
    ,o.Copy_Stat
    ,o.Compare_DDL
    ,o.Where_Clause
    ,o.Key_Column
    ,o.Query_Band_Str
    ,o.Job_Desc
    ,o.Staging_Database_Nm
    ,o.Enabled_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Process_Type </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Process_Type
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Type
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Log (
      Process_Type
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_QA_CMMT </sc-trigger> */
Create TRIGGER U_D_DSV_001_NFT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_QA_CMMT 
AFTER UPDATE OF QA_OFCR_CMMT ON U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCESS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NFT_1.NOST_VOST_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_ID,
            QA_OFCR,
            QA_OFCR_DTTM,
            QA_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_ID,

   COALESCE (NewRow.QA_OFCR, NULL, CURRENT_USER),
   COALESCE (NewRow.QA_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
   NewRow.QA_OFCR_CMMT
            );

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Character_Set </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Character_Set
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Character_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Character_Set_Log (
      Character_Set_Name
    , Description
    , Max_Character_Length
    , Min_Character_Length
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Character_Set_Name
    , n.Description
    , n.Max_Character_Length
    , n.Min_Character_Length
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.tri_tbl_HS_Update </sc-trigger> */
Create Trigger U_D_DSV_001_NIT_1.tri_tbl_HS_Update
AFTER UPDATE OF Col_01 on U_D_DSV_001_NIT_1.tbl_HS
REFERENCING OLD AS OldRow NEW AS NewRow
For Each Row
Insert U_D_DSV_001_NIT_1.tbl_HS_HIST (Col_Id, HIST_01) values (NewRow.Col_Id, NewRow.Col_01);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_File_Process </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_File_Process
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_File_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_File_Process_Log (
      Process_Name
    , Ctl_Id
    , File_Id
    , Check_Load_Status
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Ctl_Id
    , n.File_Id
    , n.Check_Load_Status
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Sys_Name
    ,Update_Tally
    ,Elapsed_Secs
    ,Key_Type1
    ,Key_Type2
    ,Key_Type3
    ,Key_Value
    ,Key_Where_Flag
    ,Key_Where_Val
    ,Key_Where_Cond
    ,Key_Partition_Flag
    ,Key_Partition_Val
    ,Key_Partition_Col
    ,Prev_Create_Date
    ,Prev_Create_Ts
    ,Is_Current_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Sys_Name
    ,n.Update_Tally
    ,n.Elapsed_Secs
    ,n.Key_Type1
    ,n.Key_Type2
    ,n.Key_Type3
    ,n.Key_Value
    ,n.Key_Where_Flag
    ,n.Key_Where_Val
    ,n.Key_Where_Cond
    ,n.Key_Partition_Flag
    ,n.Key_Partition_Val
    ,n.Key_Partition_Col
    ,n.Prev_Create_Date
    ,n.Prev_Create_Ts
    ,n.Is_Current_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_System </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_System
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_System_Log (
      Ctl_Id
    , System_Name
    , Path_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.System_Name
    , n.Path_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Stream </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Stream
AFTER INSERT ON PDGENTCF.CTLFW_T_Stream
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Stream_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Stream_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Stream_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_Hist
  (
     DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.DB_Name
    ,o.Obj_Name
    ,o.Col_1_Name
    ,o.Col_2_Name
    ,o.Join_Col_1_Name
    ,o.Join_Col_2_Name
    ,o.Predicate_Clause_Text
    ,o.Join_Type
    ,o.Rltd_DB_Name
    ,o.Rltd_Obj_Name
    ,o.Rltd_Col_1_Name
    ,o.Rltd_Col_2_Name
    ,o.Rltd_Join_Col_1_Name
    ,o.Rltd_Join_Col_2_Name
    ,o.Rltd_Predicate_Clause_Text
    ,o.Is_Enabled
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Stream </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Stream
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Stream
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Stream_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Stream_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */



--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_BWA_DR_TRIG_U
REPLACE TRIGGER U_D_DSV_001_NFT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.VOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated
)
VALUES
(
 'VOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'UPDATE'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Character_Set </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Character_Set
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Character_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Character_Set_Log (
      Character_Set_Name
    , Description
    , Max_Character_Length
    , Min_Character_Length
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Character_Set_Name
    , n.Description
    , n.Max_Character_Length
    , n.Min_Character_Length
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.MT202_EXC_RPRT_AUDT
(
REPORT                        
,SWIFT_MESSAGE_TEXT            
,UNIQUE_IDENTIFIER             
,MSG_ID                        
,MSG_SEQ                       
,MSG_TYPE                      
,DB_DATE                       
,SOURCE                        
,BRAND                         
,CBA_BIC                       
,EXTERNAL_PARTY_BIC            
,INP_OUT_IDENTIFIER            
,AMOUNT                        
,CURRENCY                      
,VAL_DATE                      
,REMITTER                      
,BENEFICIARY                   
,FIELD_20                      
,FIELD_21                      
,CREATED_DATE                  
,CDT_ID                        
,CDT_NAME                      
,DBT_ID                        
,DBT_NAME                      
,TAG_52                        
,TAG_58                        
,TAG_72                        
,SWF_FILE_REF                  
,TRN_PROC_PMS                  
,BUSN_DAYS                     
,IS_REVIEWED                   
,TEAM_RESP                     
,INVESTIGATION_STUS            
,TRAN_CATG                     
,PRODUCT                       
,OTHR_CMMT                     
,VERF_BY_IFTI_MNGR             
,IFTI_RPRT                     
,SUBM_FILE_NAME                
,SUBM_DATE                     
,SUBM_RCPT_NO                  
,INSERTION_DATE                
,EXC_OFCR                      
,EXC_OFCR_DTTM                 
,VERF_OFCR_CMMT                
,VERF_OFCR                     
,VERF_OFCR_DTTM                
,QA_OFCR_CMMT                  
,QA_OFCR                       
,QA_OFCR_DTTM                  
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(
n.REPORT                        
,n.SWIFT_MESSAGE_TEXT            
,n.UNIQUE_IDENTIFIER             
,n.MSG_ID                        
,n.MSG_SEQ                       
,n.MSG_TYPE                      
,n.DB_DATE                       
,n.SOURCE                        
,n.BRAND                         
,n.CBA_BIC                       
,n.EXTERNAL_PARTY_BIC            
,n.INP_OUT_IDENTIFIER            
,n.AMOUNT                        
,n.CURRENCY                      
,n.VAL_DATE                      
,n.REMITTER                      
,n.BENEFICIARY                   
,n.FIELD_20                      
,n.FIELD_21                      
,n.CREATED_DATE                  
,n.CDT_ID                        
,n.CDT_NAME                      
,n.DBT_ID                        
,n.DBT_NAME                      
,n.TAG_52                        
,n.TAG_58                        
,n.TAG_72                        
,n.SWF_FILE_REF                  
,n.TRN_PROC_PMS                  
,n.BUSN_DAYS                     
,n.IS_REVIEWED                   
,n.TEAM_RESP                     
,n.INVESTIGATION_STUS            
,n.TRAN_CATG                     
,n.PRODUCT                       
,n.OTHR_CMMT                     
,n.VERF_BY_IFTI_MNGR             
,n.IFTI_RPRT                     
,n.SUBM_FILE_NAME                
,n.SUBM_DATE                     
,n.SUBM_RCPT_NO                  
,n.INSERTION_DATE                
,n.EXC_OFCR                      
,n.EXC_OFCR_DTTM                 
,n.VERF_OFCR_CMMT                
,n.VERF_OFCR                     
,n.VERF_OFCR_DTTM                
,n.QA_OFCR_CMMT                  
,n.QA_OFCR                       
,n.QA_OFCR_DTTM                  
,USER
,Current_Timestamp(6)
,'UPDATE' 
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Param_Group </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Param_Group
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Param_Group
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Param_Group_Log (
      Param_Group
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Param_Group
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus_Hist
  (
     LogDate
    ,NumRows
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.LogDate
    ,n.NumRows
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Stream_BusDate </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Stream_BusDate
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Business_Date
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Business_Date
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Transform_KeyCol </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Transform_KeyCol
AFTER UPDATE ON PDGENTCF.CTLFW_T_Transform_KeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Transform_KeyCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,Key_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.Key_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDM_Job
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Hist
  (
     Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.UOW_Id
    ,n.Job_Priority
    ,n.Job_Type
    ,n.Job_Source
    ,n.Job_Description
    ,n.Source_TDPID
    ,n.Source_User
    ,n.Source_Passwd_Encrypted
    ,n.Source_Logon_Mechanism
    ,n.Source_Logon_Mechanism_Data
    ,n.Source_Userid_Pool
    ,n.Source_Account_Id
    ,n.Source_Sessions
    ,n.Target_TDPID
    ,n.Target_User
    ,n.Target_Passwd_Encrypted
    ,n.Target_Logon_Mechanism
    ,n.Target_Logon_Mechanism_Data
    ,n.Target_Userid_Pool
    ,n.Target_Account_Id
    ,n.Target_Sessions
    ,n.Data_Streams
    ,n.Response_Timeout
    ,n.Overwrite_Existing_Obj
    ,n.Force_Utility
    ,n.Log_Level
    ,n.Online_Archive
    ,n.Freeze_Job_Steps
    ,n.Max_Agents_Per_Task
    ,n.Additional_ARC_Parameters
    ,n.Query_Band_Str
    ,n.Sync_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Param_Group </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Param_Group
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Param_Group
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Param_Group_Log (
      Param_Group
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Param_Group
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */




-- cba Tigger Delete
--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_CBA_DR_TRIG_D
REPLACE TRIGGER U_D_DSV_001_NFT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.NOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS o
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'DELETE'  
,o.unique_identifier             
,o.match_type                    
,o.Nostro_acct                   
,o.CURR_NOSTRO                   
,o.Group_area                    
,o.Acct_Name                     
,o.external_party_bic            
,o.value_date                    
,o.AMOUNT                        
,o.Message_Type                  
,o.message_type_desc             
,o.tag_61_stmt_line              
,o.internal_ref                  
,o.external_ref                  
,o.description                   
,o.department                    
,o.Investigation_Flag            
,o.WS_flag                       
,o.deal_number                   
,o.cust_number                   
,o.broker_name                   
,o.product_cat                   
,o.Department_Responsible        
,o.Reason                        
,o.Other_Comment                 
,o.Escalate                      
,o.IFTI_Reportable               
,o.Payor_Bank                    
,o.Payor_Details                 
,o.Payee_Bank                    
,o.Payee_Details                 
,o.Investigation_Status          
,o.MatchID                       
,o.deal_num_list                 
,o.Beneficiary                   
,o.Remark                        
,o.Settle                        
,o.Category_Correct              
,o.Updated                       
,o.msg_id                        
,o.MSG_TAG_SEQ                   
,o.CBA_BIC                       
,o.currency_tag62                
,o.date_tag_created              
,o.amount_matched                
,o.Buckets_f                     
,o.Bucket                        
,o.WS_GDW_source                 
,o.cust_code                     
,o.cust_type                     
,o.ext_ref_nr                    
,o.portfolio_no                  
,o.Acc                           
,o.Additional_Field              
,o.Pay_Meth                      
,o.swift_50                      
,o.Remittance_info               
,o.Enough_Data_For_IFTI          
,o.swf_File_Ref                  
,o.compliance_exclusion
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Process_Type_Param </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Process_Type_Param
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Type_Param_Log (
      Process_Type
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYR_TRIG_I </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYR_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYR
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payerSameAsSwiftOrdCust,
PayerFullName,
payerAddr,
payerSuburb,
payerState,
payerPostcode,
payerCountry,
payerAccountBSB1,
payerAccountNumber1,
payerAccountBSB2,
payerAccountNumber2,
payerAccountBSB3,
payerAccountNumber3,
payerAbn,
payerAcn,
payerArbn,
payerIdentificationType1,
payerIdentificationTypeOther1,
payerIdentificationNumber1,
payerIdentificationIssuer1,
payerIdentificationCountry1,
payerIdentificationType2,
payerIdentificationTypeOther2,
payerIdentificationNumber2,
payerIdentificationIssuer2,
payerIdentificationCountry2,
payerIdentificationType3,
payerIdentificationTypeOther3,
payerIdentificationNumber3,
payerIdentificationIssuer3,
payerIdentificationCountry3,
payerCustNo,
payerIndividualDetailsDob,
payerDobSuburb,
payerDobState,
payerDobPostcode,
payerDobCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY 
)
VALUES
(
'IFTI_XML_REVW_PAYR',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payerSameAsSwiftOrdCust,
n.PayerFullName,
n.payerAddr,
n.payerSuburb,
n.payerState,
n.payerPostcode,
n.payerCountry,
n.payerAccountBSB1,
n.payerAccountNumber1,
n.payerAccountBSB2,
n.payerAccountNumber2,
n.payerAccountBSB3,
n.payerAccountNumber3,
n.payerAbn,
n.payerAcn,
n.payerArbn,
n.payerIdentificationType1,
n.payerIdentificationTypeOther1,
n.payerIdentificationNumber1,
n.payerIdentificationIssuer1,
n.payerIdentificationCountry1,
n.payerIdentificationType2,
n.payerIdentificationTypeOther2,
n.payerIdentificationNumber2,
n.payerIdentificationIssuer2,
n.payerIdentificationCountry2,
n.payerIdentificationType3,
n.payerIdentificationTypeOther3,
n.payerIdentificationNumber3,
n.payerIdentificationIssuer3,
n.payerIdentificationCountry3,
n.payerCustNo,
n.payerIndividualDetailsDob,
n.payerDobSuburb,
n.payerDobState,
n.payerDobPostcode,
n.payerDobCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY 
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Corporate_Entity </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Corporate_Entity
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity_Log (
      CE_Id
    , Corporate_Entity_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Corporate_Entity_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands_Hist
  (
     Cand_Id
    ,Cand_Trtmnt_Cd
    ,Job_Cd
    ,Job_Stus_Cd
    ,Created_Ts
    ,TargetTable
    ,Business_Date
    ,Business_Date_Cycle_Start_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Cand_Trtmnt_Cd
    ,n.Job_Cd
    ,n.Job_Stus_Cd
    ,n.Created_Ts
    ,n.TargetTable
    ,n.Business_Date
    ,n.Business_Date_Cycle_Start_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Stream_Param </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Stream_Param
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
      Stream_Key
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Process_Type_Param </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Process_Type_Param
AFTER UPDATE ON PDGENTCF.CTLFW_T_Process_Type_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Process_Type_Param_Log (
      Process_Type
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn_Hist
  (
     DB_Name
    ,Tbl_Name
    ,Col_Name
    ,Ret_Period
    ,Impacted_Obj_DB_Name
    ,Impacted_Obj_Name
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.DB_Name
    ,o.Tbl_Name
    ,o.Col_Name
    ,o.Ret_Period
    ,o.Impacted_Obj_DB_Name
    ,o.Impacted_Obj_Name
    ,o.Is_Enabled
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Stream_BusDate </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Stream_BusDate
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Business_Date
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Business_Date
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */




REPLACE TRIGGER U_D_DSV_001_NFT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.VOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated
)
VALUES
(
 'VOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'DELETE'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFWX_TI_Xfm_Process </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFWX_TI_Xfm_Process
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process
REFERENCING NEW ROW AS NR
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFWX_T_Xfm_Process_Log (
      Process_Name,           
      Busn_Dt ,         
      Xfm_Run_Dt ,                        
      Xfm_Rows_Inserted ,                       
      Xfm_Stg_Ld_Status ,           
      Xfm_Stg_Ld_Start_Time ,
      Xfm_Stg_Ld_End_Time ,         
      Xfm_Stg_Val_Start_Time ,
      Xfm_Stg_Val_End_Time ,
      Msur_Mdl_Pub_Run_Dt ,         
      Msur_Mdl_Pub_Process_Id ,           
      Msur_Mdl_Pub_Start_Time ,           
      Msur_Mdl_Pub_End_Time ,
      Msur_Mdl_Pub_Status ,   
      Msur_Mdl_Pub_Priority , 
      Pub_Stream_Key ,
      Update_User ,           
      Update_Ts 
  ) VALUES (
      NR.Process_Name,        
      NR.Busn_Dt ,            
      NR.Xfm_Run_Dt ,                     
      NR.Xfm_Rows_Inserted ,                    
      NR.Xfm_Stg_Ld_Status ,        
      NR.Xfm_Stg_Ld_Start_Time ,
      NR.Xfm_Stg_Ld_End_Time ,      
      NR.Xfm_Stg_Val_Start_Time ,
      NR.Xfm_Stg_Val_End_Time ,           
      NR.Msur_Mdl_Pub_Run_Dt ,            
      NR.Msur_Mdl_Pub_Process_Id ,        
      NR.Msur_Mdl_Pub_Start_Time ,        
      NR.Msur_Mdl_Pub_End_Time ,
      NR.Msur_Mdl_Pub_Status ,      
      NR.Msur_Mdl_Pub_Priority ,    
      NR.Pub_Stream_Key ,
      NR.Update_User ,        
      NR.Update_Ts 
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */
-- Bankwest  Credit Insert Trigger
--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_BWA_DR_TRIG_I
REPLACE TRIGGER U_D_DSV_001_NIT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.NOST_BWA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_BWA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'INSERT'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_System_File </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_System_File
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_System_File
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_System_File_Log (
      Ctl_Id
    , File_Id
    , Data_File_Name
    , Ctl_File_Name
    , Queue_Name
    , Description
    , Data_File_Suffix
    , Ctl_File_Suffix
    , Data_File_Type
    , ET_Tolerance
    , UV_Tolerance
    , Sessions
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Queue_Name
    , n.Description
    , n.Data_File_Suffix
    , n.Ctl_File_Suffix
    , n.Data_File_Type
    , n.ET_Tolerance
    , n.UV_Tolerance
    , n.Sessions
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg_Hist
  (
     DBName
    ,TBName
    ,ColName
    ,PPI_Status
    ,PPI_START_DATE
    ,PPI_END_DATE
    ,ADD_START_DATE
    ,ADD_END_DATE
    ,DROP_START_DATE
    ,DROP_END_DATE
    ,Ret_Period
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.DBName
    ,o.TBName
    ,o.ColName
    ,o.PPI_Status
    ,o.PPI_START_DATE
    ,o.PPI_END_DATE
    ,o.ADD_START_DATE
    ,o.ADD_END_DATE
    ,o.DROP_START_DATE
    ,o.DROP_END_DATE
    ,o.Ret_Period
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Load </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Load
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Load
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Load_Log 
 (
  Ctl_Id
  ,File_Id
  ,Business_Date
  ,Business_Date_Cycle_Num
  ,Business_Date_Cycle_Start_Ts
  ,File_Qualifier
  ,Process_Name
  ,Process_Id
  ,TD_Session_Id
  ,Tool_Session_Id
  ,S_Tbl_Name
  ,S_DB_Name
  ,Rows_Input
  ,Rows_Considered
  ,Rows_Not_Considered
  ,Rows_Rejected
  ,Rows_Inserted
  ,Rows_Updated
  ,Rows_Deleted
  ,Rows_ET
  ,Rows_UV
  ,Run_Date
  ,Start_Time
  ,End_Time
  ,Load_Status
  ,Update_Date
  ,Update_User
  ,Update_Time
  ,Update_Ts
  ) 
VALUES 
 (
  n.Ctl_Id
  ,n.File_Id
  ,n.Business_Date
  ,n.Business_Date_Cycle_Num
  ,n.Business_Date_Cycle_Start_Ts
  ,n.File_Qualifier
  ,n.Process_Name
  ,n.Process_Id
  ,n.TD_Session_Id
  ,n.Tool_Session_Id
  ,n.S_Tbl_Name
  ,n.S_DB_Name
  ,n.Rows_Input
  ,n.Rows_Considered
  ,n.Rows_Not_Considered
  ,n.Rows_Rejected
  ,n.Rows_Inserted
  ,n.Rows_Updated
  ,n.Rows_Deleted
  ,n.Rows_ET
  ,n.Rows_UV
  ,n.Run_Date
  ,n.Start_Time
  ,n.End_Time
  ,n.Load_Status
  ,n.Update_Date
  ,n.Update_User
  ,n.Update_Time
  ,n.Update_Ts
 );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Param_Group </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Param_Group
AFTER INSERT ON PDGENTCF.CTLFW_T_Param_Group
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Param_Group_Log (
      Param_Group
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Param_Group
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id_Hist
  (
     Fixed_Value
    ,Next_Job_Id
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Fixed_Value
    ,n.Next_Job_Id
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Corporate_Entity </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Corporate_Entity
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity_Log (
      CE_Id
    , Corporate_Entity_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Corporate_Entity_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */

-- Bankwest  Credit Insert Trigger
--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_BWA_DR_TRIG_I
REPLACE TRIGGER U_D_DSV_001_NFT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.NOST_BWA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_BWA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'INSERT'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_System_File </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_System_File
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_System_File
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_System_File_Log (
      Ctl_Id
    , File_Id
    , Data_File_Name
    , Ctl_File_Name
    , Queue_Name
    , Description
    , Data_File_Suffix
    , Ctl_File_Suffix
    , Data_File_Type
    , ET_Tolerance
    , UV_Tolerance
    , Sessions
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Queue_Name
    , n.Description
    , n.Data_File_Suffix
    , n.Ctl_File_Suffix
    , n.Data_File_Type
    , n.ET_Tolerance
    , n.UV_Tolerance
    , n.Sessions
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_VERF_CMMT </sc-trigger> */
Create TRIGGER U_D_DSV_001_NIT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_VERF_CMMT 
AFTER UPDATE OF VERF_OFCR_CMMT ON U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCESS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NIT_1.NOST_VOST_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_ID,
            VERF_OFCR,
            VERF_OFCR_DTTM,
            VERF_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_ID,
            COALESCE (NewRow.VERF_OFCR, NULL, CURRENT_USER),
            COALESCE (NewRow.VERF_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
            NewRow.VERF_OFCR_CMMT
            );

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Param_Group </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Param_Group
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Param_Group
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Param_Group_Log (
      Param_Group
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Param_Group
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Stus_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Stus_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDM_Job_Stus
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Stus_Hist
  (
     Job_Cd
    ,User_Stus_Cd
    ,System_Stus_Cd
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.User_Stus_Cd
    ,o.System_Stus_Cd
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_Transform_SurrKeyCol </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_Transform_SurrKeyCol
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_Transform_SurrKeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Transform_SKCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,SurrKey_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.SurrKey_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Character_Set </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Character_Set
AFTER UPDATE ON PDGENTCF.CTLFW_T_Character_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Character_Set_Log (
      Character_Set_Name
    , Description
    , Max_Character_Length
    , Min_Character_Length
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Character_Set_Name
    , n.Description
    , n.Max_Character_Length
    , n.Min_Character_Length
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Rule_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Rule_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Rule
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Rule_Hist
  (
     Rule_No
    ,Cand_Source
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,Priority
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Rule_No
    ,o.Cand_Source
    ,o.TDM_Database_Nm
    ,o.TDM_Obj_Nm
    ,o.Priority
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Stream_Param </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Stream_Param
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
      Stream_Key
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */




-- cba Tigger Delete
--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_CBA_CR_TRIG_D
REPLACE TRIGGER U_D_DSV_001_NFT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.NOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS o
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'DELETE'  
,o.unique_identifier             
,o.match_type                    
,o.Nostro_acct                   
,o.CURR_NOSTRO                   
,o.Group_area                    
,o.Acct_Name                     
,o.external_party_bic            
,o.value_date                    
,o.AMOUNT                        
,o.Message_Type                  
,o.message_type_desc             
,o.tag_61_stmt_line              
,o.internal_ref                  
,o.external_ref                  
,o.description                   
,o.department                    
,o.Investigation_Flag            
,o.WS_flag                       
,o.deal_number                   
,o.cust_number                   
,o.broker_name                   
,o.product_cat                   
,o.Department_Responsible        
,o.Reason                        
,o.Other_Comment                 
,o.Escalate                      
,o.IFTI_Reportable               
,o.Payor_Bank                    
,o.Payor_Details                 
,o.Payee_Bank                    
,o.Payee_Details                 
,o.Investigation_Status          
,o.MatchID                       
,o.deal_num_list                 
,o.Beneficiary                   
,o.Remark                        
,o.Settle                        
,o.Category_Correct              
,o.Updated                       
,o.msg_id                        
,o.MSG_TAG_SEQ                   
,o.CBA_BIC                       
,o.currency_tag62                
,o.date_tag_created              
,o.amount_matched                
,o.Buckets_f                     
,o.Bucket                        
,o.WS_GDW_source                 
,o.cust_code                     
,o.cust_type                     
,o.ext_ref_nr                    
,o.portfolio_no                  
,o.Acc                           
,o.Additional_Field              
,o.Pay_Meth                      
,o.swift_50                      
,o.Remittance_info               
,o.Enough_Data_For_IFTI          
,o.swf_File_Ref                  
,o.compliance_exclusion
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Transform_KeyCol </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Transform_KeyCol
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,Key_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.Key_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_EXC_CMMT </sc-trigger> */
Create TRIGGER U_D_DSV_001_NIT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_EXC_CMMT 
AFTER UPDATE OF OTHR_CMMT ON U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCESS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NIT_1.NOST_VOST_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_ID,
            EXC_OFCR,
            EXC_OFCR_DTTM,
            EXC_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_ID,
            COALESCE (NewRow.EXC_OFCR, NULL, CURRENT_USER),
            COALESCE (NewRow.EXC_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
            NewRow.OTHR_CMMT
            );

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Stream_BusDate </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Stream_BusDate
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Business_Date
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Business_Date
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Id_Obj_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Id_Obj_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDM_Job_Id_Obj
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Id_Obj_Hist
  (
     Job_Id
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Id
    ,o.Db_Nm
    ,o.Obj_Nm
    ,o.Obj_Type
    ,o.Copy_Stat
    ,o.Compare_DDL
    ,o.Where_Clause
    ,o.Key_Column
    ,o.Query_Band_Str
    ,o.Job_Desc
    ,o.Staging_Database_Nm
    ,o.Enabled_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_System_File_Param </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_System_File_Param
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_System_File_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_System_File_Param_Log (
      Ctl_Id
 , File_Id
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
 , n.File_Id
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Stream_Id </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Stream_Id
AFTER INSERT ON PDGENTCF.CTLFW_T_Stream_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Stream_Id_Log (
      Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Job_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Job_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Job
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Job_Hist
  (
     Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Corporate_Entity </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Corporate_Entity
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity_Log (
      CE_Id
    , Corporate_Entity_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Corporate_Entity_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */





REPLACE TRIGGER U_D_DSV_001_NFT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.VOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated
)
VALUES
(
 'VOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'DELETE'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TU_Process_Id </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TU_Process_Id
AFTER UPDATE ON P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Id_Log (
      Process_Name
    , Process_Id
    , Process_State
    , Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Process_Id
    , n.Process_State
    , n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.MT202_EXC_RPRT_AUDT
(
REPORT                        
,SWIFT_MESSAGE_TEXT            
,UNIQUE_IDENTIFIER             
,MSG_ID                        
,MSG_SEQ                       
,MSG_TYPE                      
,DB_DATE                       
,SOURCE                        
,BRAND                         
,CBA_BIC                       
,EXTERNAL_PARTY_BIC            
,INP_OUT_IDENTIFIER            
,AMOUNT                        
,CURRENCY                      
,VAL_DATE                      
,REMITTER                      
,BENEFICIARY                   
,FIELD_20                      
,FIELD_21                      
,CREATED_DATE                  
,CDT_ID                        
,CDT_NAME                      
,DBT_ID                        
,DBT_NAME                      
,TAG_52                        
,TAG_58                        
,TAG_72                        
,SWF_FILE_REF                  
,TRN_PROC_PMS                  
,BUSN_DAYS                     
,IS_REVIEWED                   
,TEAM_RESP                     
,INVESTIGATION_STUS            
,TRAN_CATG                     
,PRODUCT                       
,OTHR_CMMT                     
,VERF_BY_IFTI_MNGR             
,IFTI_RPRT                     
,SUBM_FILE_NAME                
,SUBM_DATE                     
,SUBM_RCPT_NO                  
,INSERTION_DATE                
,EXC_OFCR                      
,EXC_OFCR_DTTM                 
,VERF_OFCR_CMMT                
,VERF_OFCR                     
,VERF_OFCR_DTTM                
,QA_OFCR_CMMT                  
,QA_OFCR                       
,QA_OFCR_DTTM                  
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(
n.REPORT                        
,n.SWIFT_MESSAGE_TEXT            
,n.UNIQUE_IDENTIFIER             
,n.MSG_ID                        
,n.MSG_SEQ                       
,n.MSG_TYPE                      
,n.DB_DATE                       
,n.SOURCE                        
,n.BRAND                         
,n.CBA_BIC                       
,n.EXTERNAL_PARTY_BIC            
,n.INP_OUT_IDENTIFIER            
,n.AMOUNT                        
,n.CURRENCY                      
,n.VAL_DATE                      
,n.REMITTER                      
,n.BENEFICIARY                   
,n.FIELD_20                      
,n.FIELD_21                      
,n.CREATED_DATE                  
,n.CDT_ID                        
,n.CDT_NAME                      
,n.DBT_ID                        
,n.DBT_NAME                      
,n.TAG_52                        
,n.TAG_58                        
,n.TAG_72                        
,n.SWF_FILE_REF                  
,n.TRN_PROC_PMS                  
,n.BUSN_DAYS                     
,n.IS_REVIEWED                   
,n.TEAM_RESP                     
,n.INVESTIGATION_STUS            
,n.TRAN_CATG                     
,n.PRODUCT                       
,n.OTHR_CMMT                     
,n.VERF_BY_IFTI_MNGR             
,n.IFTI_RPRT                     
,n.SUBM_FILE_NAME                
,n.SUBM_DATE                     
,n.SUBM_RCPT_NO                  
,n.INSERTION_DATE                
,n.EXC_OFCR                      
,n.EXC_OFCR_DTTM                 
,n.VERF_OFCR_CMMT                
,n.VERF_OFCR                     
,n.VERF_OFCR_DTTM                
,n.QA_OFCR_CMMT                  
,n.QA_OFCR                       
,n.QA_OFCR_DTTM                  
,USER
,Current_Timestamp(6)
,'DELETE' 
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Stream_Param </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Stream_Param
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
      Stream_Key
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Id_Obj_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Id_Obj_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDM_Job_Id_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Id_Obj_Hist
  (
     Job_Id
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Db_Nm
    ,n.Obj_Nm
    ,n.Obj_Type
    ,n.Copy_Stat
    ,n.Compare_DDL
    ,n.Where_Clause
    ,n.Key_Column
    ,n.Query_Band_Str
    ,n.Job_Desc
    ,n.Staging_Database_Nm
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_CE_System </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_CE_System
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_CE_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_CE_System_Log (
      CE_Id
    , Ctl_Id
    , CE_Ctl_Rltnshp_Type
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Ctl_Id
    , n.CE_Ctl_Rltnshp_Type
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFWX_TU_Xfm_Process </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFWX_TU_Xfm_Process
AFTER UPDATE ON PDGENTCF.CTLFWX_T_Xfm_Process
REFERENCING NEW ROW AS NR
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFWX_T_Xfm_Process_Log (
      Process_Name,           
      Busn_Dt ,         
      Xfm_Run_Dt ,                        
      Xfm_Rows_Inserted ,                       
      Xfm_Stg_Ld_Status ,           
      Xfm_Stg_Ld_Start_Time ,
      Xfm_Stg_Ld_End_Time ,   
      Xfm_Stg_Val_Start_Time ,
      Xfm_Stg_Val_End_Time ,  
      Msur_Mdl_Pub_Run_Dt ,         
      Msur_Mdl_Pub_Process_Id ,           
      Msur_Mdl_Pub_Start_Time ,           
      Msur_Mdl_Pub_End_Time ,
      Msur_Mdl_Pub_Status ,   
      Msur_Mdl_Pub_Priority , 
      Pub_Stream_Key ,
      Update_User ,           
      Update_Ts 
  ) VALUES (
      NR.Process_Name,        
      NR.Busn_Dt ,            
      NR.Xfm_Run_Dt ,                     
      NR.Xfm_Rows_Inserted ,                    
      NR.Xfm_Stg_Ld_Status ,        
      NR.Xfm_Stg_Ld_Start_Time ,
      NR.Xfm_Stg_Ld_End_Time ,
      NR.Xfm_Stg_Val_Start_Time ,
      NR.Xfm_Stg_Val_End_Time ,     
      NR.Msur_Mdl_Pub_Run_Dt ,            
      NR.Msur_Mdl_Pub_Process_Id ,        
      NR.Msur_Mdl_Pub_Start_Time ,        
      NR.Msur_Mdl_Pub_End_Time ,
      NR.Msur_Mdl_Pub_Status ,      
      NR.Msur_Mdl_Pub_Priority ,    
      NR.Pub_Stream_Key ,
      NR.Update_User ,        
      NR.Update_Ts 
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds_Hist
  (
     SQLStateCd
    ,ClassCd
    ,SubClassCd
    ,TDErrorCd
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     o.SQLStateCd
    ,o.ClassCd
    ,o.SubClassCd
    ,o.TDErrorCd
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_TS
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Stream_Id </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Stream_Id
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_Id_Log (
      Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */


--STEP 7 CREATE THE TRIGGERS CR

REPLACE TRIGGER U_D_DSV_001_NFT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.VOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated        
)
VALUES
(
 'VOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'INSERT'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_CE_System </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_CE_System
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_CE_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_CE_System_Log (
      CE_Id
    , Ctl_Id
    , CE_Ctl_Rltnshp_Type
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Ctl_Id
    , n.CE_Ctl_Rltnshp_Type
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR_TRIG_D </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receiversCorrespondentCode1,
receiversCorrespondentName1,
receiversCorrespondentCity1,
receiversCorrespondentCountry1,
receiversCorrespondentCode2,
receiversCorrespondentName2,
receiversCorrespondentCity2,
receiversCorrespondentCountry2,
receiversCorrespondentCode3,
receiversCorrespondentName3,
receiversCorrespondentCity3,
receiversCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_CORR',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receiversCorrespondentCode1,
n.receiversCorrespondentName1,
n.receiversCorrespondentCity1,
n.receiversCorrespondentCountry1,
n.receiversCorrespondentCode2,
n.receiversCorrespondentName2,
n.receiversCorrespondentCity2,
n.receiversCorrespondentCountry2,
n.receiversCorrespondentCode3,
n.receiversCorrespondentName3,
n.receiversCorrespondentCity3,
n.receiversCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TI_Character_Set </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TI_Character_Set
AFTER INSERT ON V_O00_D_TCF_001_STD_0.CTLFW_T_Character_Set
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Character_Set_Log (
      Character_Set_Name
    , Description
    , Max_Character_Length
    , Min_Character_Length
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Character_Set_Name
    , n.Description
    , n.Max_Character_Length
    , n.Min_Character_Length
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I_Src
    ,MAX_Pros_Key_I_Tgt
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Stream_Nm
    ,o.Db_Nm
    ,o.Tbl_Nm
    ,o.User_ID
    ,o.Max_Pros_Key_I_Src
    ,o.MAX_Pros_Key_I_Tgt
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_CE_System </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_CE_System
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_CE_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_CE_System_Log (
      CE_Id
    , Ctl_Id
    , CE_Ctl_Rltnshp_Type
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Ctl_Id
    , n.CE_Ctl_Rltnshp_Type
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Stream_BusDate </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Stream_BusDate
AFTER INSERT ON PDGENTCF.CTLFW_T_Stream_BusDate
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Stream_BusDate_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Business_Date
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Business_Date
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Stream_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Stream_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Stream
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Stream_Hist
  (
     Stream_Id
    ,Stream_Name
    ,Stream_RunNo
    ,Stream_Type
    ,Job_Name
    ,Job_Stat
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Id
    ,n.Stream_Name
    ,n.Stream_RunNo
    ,n.Stream_Type
    ,n.Job_Name
    ,n.Job_Stat
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Start_Ts
    ,n.End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_CE_System </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_CE_System
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_CE_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_CE_System_Log (
      CE_Id
    , Ctl_Id
    , CE_Ctl_Rltnshp_Type
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Ctl_Id
    , n.CE_Ctl_Rltnshp_Type
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */


REPLACE TRIGGER U_D_DSV_001_NFT_1.VOST_CBA_DR_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.VOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated        
)
VALUES
(
 'VOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'INSERT'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Corporate_Entity </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Corporate_Entity
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Corporate_Entity_Log (
      CE_Id
    , Corporate_Entity_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Corporate_Entity_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */


--STEP 7 CREATE THE TRIGGERS CR

REPLACE TRIGGER U_D_DSV_001_NIT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.VOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated        
)
VALUES
(
 'VOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'INSERT'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> V_O00_D_TCF_001_STD_0.CTLFW_TU_Process </sc-trigger> */
--------------------------------------------------------------------------------


REPLACE TRIGGER V_O00_D_TCF_001_STD_0.CTLFW_TU_Process
AFTER UPDATE ON V_O00_D_TCF_001_STD_0.CTLFW_T_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO V_O00_D_TCF_001_STD_0.CTLFW_T_Process_Log (
      Process_Name
    , Description
    , Process_Type
    , Ctl_Id
    , Stream_Key
    , In_DB_Name
    , In_Object_Name
    , Out_DB_Name
    , Out_Object_Name
    , Target_TableDatabaseName
    , Target_TableName
    , Temp_DatabaseName
    , Key_Set_Id
    , Domain_Id
    , Code_Set_Id
    , Collect_Stats
    , Truncate_Target
    , Verification_Flag
    , File_Qualifier_Reset_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Description
    , n.Process_Type
    , n.Ctl_Id
    , n.Stream_Key
    , n.In_DB_Name
    , n.In_Object_Name
    , n.Out_DB_Name
    , n.Out_Object_Name
    , n.Target_TableDatabaseName
    , n.Target_TableName
    , n.Temp_DatabaseName
    , n.Key_Set_Id
    , n.Domain_Id
    , n.Code_Set_Id
    , n.Collect_Stats
    , n.Truncate_Target
    , n.Verification_Flag
    , n.File_Qualifier_Reset_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Alrt_Dest_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Alrt_Dest_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Alrt_Dest
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Alrt_Dest_Hist
  (
     Destination
    ,DestinationDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Destination
    ,n.DestinationDesc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_System_File_Extract </sc-trigger> */


REPLACE  TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_System_File_Extract
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_System_File_Extract
REFERENCING NEW ROW AS n
FOR EACH ROW (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_SFE_Log (
      Ctl_Id
    , File_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , File_Qualifier
    , Data_File_Name
    , Ctl_File_Name
    , Extraction_Date
    , Ext_Start_Time
    , Ext_End_Time
    , TD_Session_Id
    , Tool_Session_Id
    , Load_Status
    , Count_Source
    , Count_Target
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.File_Qualifier
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Extraction_Date
    , n.Ext_Start_Time
    , n.Ext_End_Time
    , n.TD_Session_Id
    , n.Tool_Session_Id
    , n.Load_Status
    , n.Count_Source
    , n.Count_Target
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_CE_System </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_CE_System
AFTER INSERT ON PDGENTCF.CTLFW_T_CE_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_CE_System_Log (
      CE_Id
    , Ctl_Id
    , CE_Ctl_Rltnshp_Type
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Ctl_Id
    , n.CE_Ctl_Rltnshp_Type
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Step_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Step_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDM_Job_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Step_Hist
  (
     Job_Cd
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Step_No
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Executable_User
    ,n.Executable_Platform_Name
    ,n.Executable_BT_ET_Flag
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_Stream_BusDate </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_Stream_BusDate
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Stream_BusDate_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Business_Date
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Business_Date
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */



--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_BWA_DR_TRIG_U
REPLACE TRIGGER U_D_DSV_001_NFT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.NOST_BWA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_BWA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'UPDATE'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Process_Id </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Process_Id
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Id_Log (
      Process_Name
    , Process_Id
    , Process_State
    , Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Process_Id
    , n.Process_State
    , n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */




-- Bankwest Tigger Delete
--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_BWA_CR_TRIG_D
replace trigger U_D_DSV_001_NIT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_D
after delete on U_D_DSV_001_NIT_1.NOST_BWA_CR_EXC_RPRT_ACCESS
referencing old row as o
for each row
insert into U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
values
(
 'NOST_BWA_CR_EXC_RPRT_ACCESS'
,user
,current_timestamp(6)
,'DELETE'  
,o.unique_identifier             
,o.match_type                    
,o.Nostro_acct                   
,o.CURR_NOSTRO                   
,o.Group_area                    
,o.Acct_Name                     
,o.external_party_bic            
,o.value_date                    
,o.AMOUNT                        
,o.Message_Type                  
,o.message_type_desc             
,o.tag_61_stmt_line              
,o.internal_ref                  
,o.external_ref                  
,o.description                   
,o.department                    
,o.Investigation_Flag            
,o.WS_flag                       
,o.deal_number                   
,o.cust_number                   
,o.broker_name                   
,o.product_cat                   
,o.Department_Responsible        
,o.Reason                        
,o.Other_Comment                 
,o.Escalate                      
,o.IFTI_Reportable               
,o.Payor_Bank                    
,o.Payor_Details                 
,o.Payee_Bank                    
,o.Payee_Details                 
,o.Investigation_Status          
,o.MatchID                       
,o.deal_num_list                 
,o.Beneficiary                   
,o.Remark                        
,o.Settle                        
,o.Category_Correct              
,o.Updated                       
,o.msg_id                        
,o.MSG_TAG_SEQ                   
,o.CBA_BIC                       
,o.currency_tag62                
,o.date_tag_created              
,o.amount_matched                
,o.Buckets_f                     
,o.Bucket                        
,o.WS_GDW_source                 
,o.cust_code                     
,o.cust_type                     
,o.ext_ref_nr                    
,o.portfolio_no                  
,o.Acc                           
,o.Additional_Field              
,o.Pay_Meth                      
,o.swift_50                      
,o.Remittance_info               
,o.Enough_Data_For_IFTI          
,o.swf_File_Ref                  
,o.compliance_exclusion
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Stream_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Stream_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Cand_Stream
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Stream_Hist
  (
     Stream_Id
    ,Stream_Name
    ,Stream_RunNo
    ,Stream_Type
    ,Job_Name
    ,Job_Stat
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Stream_Id
    ,o.Stream_Name
    ,o.Stream_RunNo
    ,o.Stream_Type
    ,o.Job_Name
    ,o.Job_Stat
    ,o.Allow_Cnsldtn_Flag
    ,o.Allow_Partial_Flag
    ,o.Start_Ts
    ,o.End_Ts
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TI_System_File_Param </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TI_System_File_Param
AFTER INSERT ON D_S00_D_TCF_001_STD_0.CTLFW_T_System_File_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_System_File_Param_Log (
      Ctl_Id
 , File_Id
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
 , n.File_Id
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Corporate_Entity </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Corporate_Entity
AFTER UPDATE ON PDGENTCF.CTLFW_T_Corporate_Entity
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Corporate_Entity_Log (
      CE_Id
    , Corporate_Entity_Name
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Corporate_Entity_Name
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_Hist
  (
     Cand_Id
    ,Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Job_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Transform_SurrKeyCol </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Transform_SurrKeyCol
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Transform_SurrKeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Transform_SKCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,SurrKey_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.SurrKey_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */
-- cba  debit Insert Trigger
--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_CBA_DR_TRIG_I
REPLACE TRIGGER U_D_DSV_001_NFT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.NOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'INSERT'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Process_Param </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Process_Param
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Process_Param_Log (
      Process_Name
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */




-- Bankwest Tigger Delete
--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_BWA_DR_TRIG_D
REPLACE TRIGGER U_D_DSV_001_NIT_1.NOST_BWA_DR_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.NOST_BWA_DR_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS o
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_BWA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'DELETE'  
,o.unique_identifier             
,o.match_type                    
,o.Nostro_acct                   
,o.CURR_NOSTRO                   
,o.Group_area                    
,o.Acct_Name                     
,o.external_party_bic            
,o.value_date                    
,o.AMOUNT                        
,o.Message_Type                  
,o.message_type_desc             
,o.tag_61_stmt_line              
,o.internal_ref                  
,o.external_ref                  
,o.description                   
,o.department                    
,o.Investigation_Flag            
,o.WS_flag                       
,o.deal_number                   
,o.cust_number                   
,o.broker_name                   
,o.product_cat                   
,o.Department_Responsible        
,o.Reason                        
,o.Other_Comment                 
,o.Escalate                      
,o.IFTI_Reportable               
,o.Payor_Bank                    
,o.Payor_Details                 
,o.Payee_Bank                    
,o.Payee_Details                 
,o.Investigation_Status          
,o.MatchID                       
,o.deal_num_list                 
,o.Beneficiary                   
,o.Remark                        
,o.Settle                        
,o.Category_Correct              
,o.Updated                       
,o.msg_id                        
,o.MSG_TAG_SEQ                   
,o.CBA_BIC                       
,o.currency_tag62                
,o.date_tag_created              
,o.amount_matched                
,o.Buckets_f                     
,o.Bucket                        
,o.WS_GDW_source                 
,o.cust_code                     
,o.cust_type                     
,o.ext_ref_nr                    
,o.portfolio_no                  
,o.Acc                           
,o.Additional_Field              
,o.Pay_Meth                      
,o.swift_50                      
,o.Remittance_info               
,o.Enough_Data_For_IFTI          
,o.swf_File_Ref                  
,o.compliance_exclusion
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_Hist
  (
     Daemon_Name
    ,Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Enabled_Flag
    ,Loop_Delay_Secs
    ,Send_Alert_On_Error_Flag
    ,Autosys_User
    ,Autosys_Host
    ,Autosys_Host_IP
    ,Autosys_Stus_Restrict_Flag
    ,Autosys_Stus_Allowed_Set
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Daemon_Name
    ,n.Prog_Name
    ,n.Prog_Mode
    ,n.Prog_User
    ,n.Prog_Host
    ,n.Enabled_Flag
    ,n.Loop_Delay_Secs
    ,n.Send_Alert_On_Error_Flag
    ,n.Autosys_User
    ,n.Autosys_Host
    ,n.Autosys_Host_IP
    ,n.Autosys_Stus_Restrict_Flag
    ,n.Autosys_Stus_Allowed_Set
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TD_Stream_Param </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TD_Stream_Param
AFTER DELETE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Param
REFERENCING OLD ROW AS O  
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Param_Log (
    Stream_Key
    , Param_Group
    , Param_Name
    , Description
    , Param_Value
    , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
    o.Stream_Key
    , o.Param_Group
    , o.Param_Name
    , o.Description
    , o.Param_Value
    , o.Param_Cast
    , DATE
    , USER
    , TIME
    , CURRENT_TIMESTAMP(6) -  INTERVAL '0.000001' second /*Change Delete Trigger and change update_ts - 0.000001 second*/
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_System_File_Param </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_System_File_Param
AFTER INSERT ON PDGENTCF.CTLFW_T_System_File_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_System_File_Param_Log (
      Ctl_Id
 , File_Id
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
 , n.File_Id
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_Hist
  (
     Cand_Obj_Source_Type_Cd
    ,Cand_Obj_Source_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Source_Type_Cd
    ,n.Cand_Obj_Source_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_System_File </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_System_File
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_System_File
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_System_File_Log (
      Ctl_Id
    , File_Id
    , Data_File_Name
    , Ctl_File_Name
    , Queue_Name
    , Description
    , Data_File_Suffix
    , Ctl_File_Suffix
    , Data_File_Type
    , ET_Tolerance
    , UV_Tolerance
    , Sessions
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Ctl_Id
    , n.File_Id
    , n.Data_File_Name
    , n.Ctl_File_Name
    , n.Queue_Name
    , n.Description
    , n.Data_File_Suffix
    , n.Ctl_File_Suffix
    , n.Data_File_Type
    , n.ET_Tolerance
    , n.UV_Tolerance
    , n.Sessions
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_CORR
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendersCorrespondentCode1,
sendersCorrespondentName1,
sendersCorrespondentCity1,
sendersCorrespondentCountry1,
sendersCorrespondentCode2,
sendersCorrespondentName2,
sendersCorrespondentCity2,
sendersCorrespondentCountry2,
sendersCorrespondentCode3,
sendersCorrespondentName3,
sendersCorrespondentCity3,
sendersCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_CORR',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendersCorrespondentCode1,
n.sendersCorrespondentName1,
n.sendersCorrespondentCity1,
n.sendersCorrespondentCountry1,
n.sendersCorrespondentCode2,
n.sendersCorrespondentName2,
n.sendersCorrespondentCity2,
n.sendersCorrespondentCountry2,
n.sendersCorrespondentCode3,
n.sendersCorrespondentName3,
n.sendersCorrespondentCity3,
n.sendersCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Transform_KeyCol </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Transform_KeyCol
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Transform_KeyCol_Log (
    Out_DB_Name
    ,Out_Object_Name
    ,Key_Column
    ,Update_Date
    ,Update_User
    ,Update_Time
    ,Update_Ts
  ) VALUES (
    n.Out_DB_Name
    ,n.Out_Object_Name
    ,n.Key_Column
    ,n.Update_Date
    ,n.Update_User
    ,n.Update_Time
    ,n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */



--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_CBA_CR_TRIG_U
REPLACE TRIGGER U_D_DSV_001_NIT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.NOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'UPDATE'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Cand
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Hist
  (
     Cand_Id
    ,Description
    ,Trtmnt_Cd
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Priority
    ,Cand_Source
    ,Source_Id
    ,Stream_End_TS
    ,Enabled_Flag
    ,Num_Retries
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Exec_Param_KVP    
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Description
    ,n.Trtmnt_Cd
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Priority
    ,n.Cand_Source
    ,n.Source_Id
    ,n.Stream_End_TS
    ,n.Enabled_Flag
    ,n.Num_Retries
    ,n.TDM_Job_Priority
    ,n.TDM_Job_Type
    ,n.TDM_Job_Source
    ,n.TDM_Job_Description
    ,n.TDM_Source_TDPID
    ,n.TDM_Source_User
    ,n.TDM_Source_Passwd_Encrypted
    ,n.TDM_Source_Logon_Mech
    ,n.TDM_Source_Logon_Mech_Data
    ,n.TDM_Source_Userid_Pool
    ,n.TDM_Source_Account_Id
    ,n.TDM_Source_Sessions
    ,n.TDM_Target_TDPID
    ,n.TDM_Target_User
    ,n.TDM_Target_Passwd_Encrypted
    ,n.TDM_Target_Logon_Mech
    ,n.TDM_Target_Logon_Mech_Data
    ,n.TDM_Target_Userid_Pool
    ,n.TDM_Target_Account_Id
    ,n.TDM_Target_Sessions
    ,n.TDM_Data_Streams
    ,n.TDM_Response_Timeout
    ,n.TDM_Overwrite_Existing_Obj
    ,n.TDM_Force_Utility
    ,n.TDM_Log_Level
    ,n.TDM_Online_Archive
    ,n.TDM_Freeze_Job_Steps
    ,n.TDM_Max_Agents_Per_Task
    ,n.TDM_Additional_ARC_Parameters
    ,n.TDM_Query_Band_Str
    ,n.TDM_Sync_Flag
    ,n.Exec_Param_KVP    
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Stream_Id </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Stream_Id
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Id
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Id_Log (
      Stream_Key
    , Stream_Id
    , Business_Date
    , Business_Date_Cycle_Num
    , Business_Date_Cycle_Start_Ts
    , Next_Business_Date
    , Prev_Business_Date
    , Processing_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Stream_Id
    , n.Business_Date
    , n.Business_Date_Cycle_Num
    , n.Business_Date_Cycle_Start_Ts
    , n.Next_Business_Date
    , n.Prev_Business_Date
    , n.Processing_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TU_Stream </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TU_Stream
AFTER UPDATE ON PDGENTCF.CTLFW_T_Stream
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Stream_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Stream_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Stream_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_Hist
  (
     Cycl_Start_Ts
    ,Cycl_End_Ts
    ,Cycl_Stat
    ,Src_Sys
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cycl_Start_Ts
    ,n.Cycl_End_Ts
    ,n.Cycl_Stat
    ,n.Src_Sys
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TI_Process </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TI_Process
AFTER INSERT ON D_D00_D_TCF_001_STD_0.CTLFW_T_Process
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_Process_Log (
      Process_Name
    , Description
    , Process_Type
    , Ctl_Id
    , Stream_Key
    , In_DB_Name
    , In_Object_Name
    , Out_DB_Name
    , Out_Object_Name
    , Target_TableDatabaseName
    , Target_TableName
    , Temp_DatabaseName
    , Key_Set_Id
    , Domain_Id
    , Code_Set_Id
    , Collect_Stats
    , Truncate_Target
    , Verification_Flag
    , File_Qualifier_Reset_Flag
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Name
    , n.Description
    , n.Process_Type
    , n.Ctl_Id
    , n.Stream_Key
    , n.In_DB_Name
    , n.In_Object_Name
    , n.Out_DB_Name
    , n.Out_Object_Name
    , n.Target_TableDatabaseName
    , n.Target_TableName
    , n.Temp_DatabaseName
    , n.Key_Set_Id
    , n.Domain_Id
    , n.Code_Set_Id
    , n.Collect_Stats
    , n.Truncate_Target
    , n.Verification_Flag
    , n.File_Qualifier_Reset_Flag
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_ADMT_TRIG_U </sc-trigger> */
Replace TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_ADMT_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.IFTI_XML_ADMT
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_ADMT_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
FILENAME,
RPRT_CNT,
IFTI_XML,
FILE_COUR,
CRET_BY,
CRET_DTTM,
FILE_RNAM_BY,
FILE_RNAM_DTTM,
APRV_BY,
APRV_DTTM,
RCPT_N,
AUSTC_REFN,
SUBM_DT,
ENUS_CMMT,
REVER_OFCR_CMMT,
REVER_OFCR,
REVER_OFCR_DTTM,
VERF_OFCR_CMMT,
VERF_OFCR,
VERF_OFCR_DTTM,
QA_OFCR_CMMT,
QA_OFCR,
QA_OFCR_DTTM,
IS_REVIEWED
)
VALUES
(
'IFTI_XML_ADMT',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.FILENAME,
n.RPRT_CNT,
n.IFTI_XML,
n.FILE_COUR,
n.CRET_BY,
n.CRET_DTTM,
n.FILE_RNAM_BY,
n.FILE_RNAM_DTTM,
n.APRV_BY,
n.APRV_DTTM,
n.RCPT_N,
n.AUSTC_REFN,
n.SUBM_DT,
n.ENUS_CMMT,
n.REVER_OFCR_CMMT,
n.REVER_OFCR,
n.REVER_OFCR_DTTM,
n.VERF_OFCR_CMMT,
n.VERF_OFCR,
n.VERF_OFCR_DTTM,
n.QA_OFCR_CMMT,
n.QA_OFCR,
n.QA_OFCR_DTTM,
n.IS_REVIEWED
);

--
/* <sc-trigger> P_P00_D_TCF_001_STD_0.CTLFW_TI_Param_Group </sc-trigger> */



REPLACE TRIGGER P_P00_D_TCF_001_STD_0.CTLFW_TI_Param_Group
AFTER INSERT ON P_P00_D_TCF_001_STD_0.CTLFW_T_Param_Group
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO P_P00_D_TCF_001_STD_0.CTLFW_T_Param_Group_Log (
      Param_Group
    , Description
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Param_Group
    , n.Description
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST_TRIG_I </sc-trigger> */
-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST
 --------------------------------------------------------------------- 
 --DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
orderingInstnCode,
orderingInstnName,
orderingInstnCity,
orderingInstnCountry,
orderingInstnBranchBSB,
orderingInstnBranchId,
orderingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ORDR_INST',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.orderingInstnCode,
n.orderingInstnName,
n.orderingInstnCity,
n.orderingInstnCountry,
n.orderingInstnBranchBSB,
n.orderingInstnBranchId,
n.orderingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Stus_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Stus_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDM_Job_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Stus_Hist
  (
     Job_Cd
    ,User_Stus_Cd
    ,System_Stus_Cd
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.User_Stus_Cd
    ,n.System_Stus_Cd
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> D_S00_D_TCF_001_STD_0.CTLFW_TU_Stream </sc-trigger> */


REPLACE TRIGGER D_S00_D_TCF_001_STD_0.CTLFW_TU_Stream
AFTER UPDATE ON D_S00_D_TCF_001_STD_0.CTLFW_T_Stream
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_S00_D_TCF_001_STD_0.CTLFW_T_Stream_Log (
      Stream_Key
    , Cycle_Freq_Code
    , Stream_Name
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Stream_Key
    , n.Cycle_Freq_Code
    , n.Stream_Name
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> PDGENTCF.CTLFW_TI_Process_Type_Param </sc-trigger> */
REPLACE TRIGGER PDGENTCF.CTLFW_TI_Process_Type_Param
AFTER INSERT ON PDGENTCF.CTLFW_T_Process_Type_Param
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO PDGENTCF.CTLFW_T_Process_Type_Param_Log (
      Process_Type
 , Param_Group
 , Param_Name
    , Description
 , Param_Value
 , Param_Cast
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.Process_Type
 , n.Param_Group
 , n.Param_Name
    , n.Description
 , n.Param_Value
 , n.Param_Cast
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
)
;

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Job_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Job_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Job
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Job_Hist
  (
     Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> D_D00_D_TCF_001_STD_0.CTLFW_TU_CE_System </sc-trigger> */
REPLACE TRIGGER D_D00_D_TCF_001_STD_0.CTLFW_TU_CE_System
AFTER UPDATE ON D_D00_D_TCF_001_STD_0.CTLFW_T_CE_System
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO D_D00_D_TCF_001_STD_0.CTLFW_T_CE_System_Log (
      CE_Id
    , Ctl_Id
    , CE_Ctl_Rltnshp_Type
    , Update_Date
    , Update_User
    , Update_Time
    , Update_Ts
  ) VALUES (
      n.CE_Id
    , n.Ctl_Id
    , n.CE_Ctl_Rltnshp_Type
    , n.Update_Date
    , n.Update_User
    , n.Update_Time
    , n.Update_Ts
  );
);

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR_TRIG_D </sc-trigger> */


-- DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receiversCorrespondentCode1,
receiversCorrespondentName1,
receiversCorrespondentCity1,
receiversCorrespondentCountry1,
receiversCorrespondentCode2,
receiversCorrespondentName2,
receiversCorrespondentCity2,
receiversCorrespondentCountry2,
receiversCorrespondentCode3,
receiversCorrespondentName3,
receiversCorrespondentCity3,
receiversCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_CORR',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receiversCorrespondentCode1,
n.receiversCorrespondentName1,
n.receiversCorrespondentCity1,
n.receiversCorrespondentCountry1,
n.receiversCorrespondentCode2,
n.receiversCorrespondentName2,
n.receiversCorrespondentCity2,
n.receiversCorrespondentCountry2,
n.receiversCorrespondentCode3,
n.receiversCorrespondentName3,
n.receiversCorrespondentCity3,
n.receiversCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR_TRIG_I </sc-trigger> */
-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR
 
 --------------------------------------------------------------------- 
 --DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receiversCorrespondentCode1,
receiversCorrespondentName1,
receiversCorrespondentCity1,
receiversCorrespondentCountry1,
receiversCorrespondentCode2,
receiversCorrespondentName2,
receiversCorrespondentCity2,
receiversCorrespondentCountry2,
receiversCorrespondentCode3,
receiversCorrespondentName3,
receiversCorrespondentCity3,
receiversCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_CORR',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receiversCorrespondentCode1,
n.receiversCorrespondentName1,
n.receiversCorrespondentCity1,
n.receiversCorrespondentCountry1,
n.receiversCorrespondentCode2,
n.receiversCorrespondentName2,
n.receiversCorrespondentCity2,
n.receiversCorrespondentCountry2,
n.receiversCorrespondentCode3,
n.receiversCorrespondentName3,
n.receiversCorrespondentCity3,
n.receiversCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Alrt_Action_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Alrt_Action_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Alrt_Action
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Alrt_Action_Hist
  (
     ActionCode
    ,ActionDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.ActionCode
    ,o.ActionDesc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_DM_Stus_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_DM_Stus_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDM_DM_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_DM_Stus_Hist
  (
     ResourceId
    ,UOWId
    ,Job_CD
    ,Job_Stus_Cd
    ,Start_TS
    ,End_TS
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.ResourceId
    ,n.UOWId
    ,n.Job_CD
    ,n.Job_Stus_Cd
    ,n.Start_TS
    ,n.End_TS
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE_TRIG_U </sc-trigger> */


--update
--DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYE
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payeeSameAsSwiftBenCust,
payeeFullName,
payeeAddr,
payeeSuburb,
payeeState,
payeePostcode,
payeeCountry,
payeeAccountBSB1,
payeeAccountNumber1,
payeeAccountBSB2,
payeeAccountNumber2,
payeeAccountBSB3,
payeeAccountNumber3,
payeeIdentificationType1,
payeeIdentificationTypeOther1,
payeeIdentificationNumber1,
payeeIdentificationIssuer1,
payeeIdentificationCountry1,
payeeIdentificationType2,
payeeIdentificationTypeOther2,
payeeIdentificationNumber2,
payeeIdentificationIssuer2,
payeeIdentificationCountry2,
payeeIdentificationType3,
payeeIdentificationTypeOther3,
payeeIdentificationNumber3,
payeeIdentificationIssuer3,
payeeIdentificationCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_PAYE',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payeeSameAsSwiftBenCust,
n.payeeFullName,
n.payeeAddr,
n.payeeSuburb,
n.payeeState,
n.payeePostcode,
n.payeeCountry,
n.payeeAccountBSB1,
n.payeeAccountNumber1,
n.payeeAccountBSB2,
n.payeeAccountNumber2,
n.payeeAccountBSB3,
n.payeeAccountNumber3,
n.payeeIdentificationType1,
n.payeeIdentificationTypeOther1,
n.payeeIdentificationNumber1,
n.payeeIdentificationIssuer1,
n.payeeIdentificationCountry1,
n.payeeIdentificationType2,
n.payeeIdentificationTypeOther2,
n.payeeIdentificationNumber2,
n.payeeIdentificationIssuer2,
n.payeeIdentificationCountry2,
n.payeeIdentificationType3,
n.payeeIdentificationTypeOther3,
n.payeeIdentificationNumber3,
n.payeeIdentificationIssuer3,
n.payeeIdentificationCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_CORR
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receiversCorrespondentCode1,
receiversCorrespondentName1,
receiversCorrespondentCity1,
receiversCorrespondentCountry1,
receiversCorrespondentCode2,
receiversCorrespondentName2,
receiversCorrespondentCity2,
receiversCorrespondentCountry2,
receiversCorrespondentCode3,
receiversCorrespondentName3,
receiversCorrespondentCity3,
receiversCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_CORR',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receiversCorrespondentCode1,
n.receiversCorrespondentName1,
n.receiversCorrespondentCity1,
n.receiversCorrespondentCountry1,
n.receiversCorrespondentCode2,
n.receiversCorrespondentName2,
n.receiversCorrespondentCity2,
n.receiversCorrespondentCountry2,
n.receiversCorrespondentCode3,
n.receiversCorrespondentName3,
n.receiversCorrespondentCity3,
n.receiversCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_SQLState_Cds_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_SQLState_Cds_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_SQLState_Cds
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_SQLState_Cds_Hist
  (
     SQLStateCd
    ,ClassCd
    ,SubClassCd
    ,TDErrorCd
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     o.SQLStateCd
    ,o.ClassCd
    ,o.SubClassCd
    ,o.TDErrorCd
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_TS
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Step_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Step_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Step
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Step_Hist
  (
     Cand_Id
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Exec_Param_KVP
    ,Executable_User
    ,Executable_TDPID
    ,Executable_BT_ET_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Id
    ,o.Step_No
    ,o.Step_Desc
    ,o.Step_Type
    ,o.Executable_Nm
    ,o.Executable_Db
    ,o.Executable_Type
    ,o.Executable_Param
    ,o.Exec_Param_KVP
    ,o.Executable_User
    ,o.Executable_TDPID
    ,o.Executable_BT_ET_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYR_TRIG_U </sc-trigger> */


--update
--DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYR_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYR
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payerSameAsSwiftOrdCust,
PayerFullName,
payerAddr,
payerSuburb,
payerState,
payerPostcode,
payerCountry,
payerAccountBSB1,
payerAccountNumber1,
payerAccountBSB2,
payerAccountNumber2,
payerAccountBSB3,
payerAccountNumber3,
payerAbn,
payerAcn,
payerArbn,
payerIdentificationType1,
payerIdentificationTypeOther1,
payerIdentificationNumber1,
payerIdentificationIssuer1,
payerIdentificationCountry1,
payerIdentificationType2,
payerIdentificationTypeOther2,
payerIdentificationNumber2,
payerIdentificationIssuer2,
payerIdentificationCountry2,
payerIdentificationType3,
payerIdentificationTypeOther3,
payerIdentificationNumber3,
payerIdentificationIssuer3,
payerIdentificationCountry3,
payerCustNo,
payerIndividualDetailsDob,
payerDobSuburb,
payerDobState,
payerDobPostcode,
payerDobCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY 
)
VALUES
(
'IFTI_XML_REVW_PAYR',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payerSameAsSwiftOrdCust,
n.PayerFullName,
n.payerAddr,
n.payerSuburb,
n.payerState,
n.payerPostcode,
n.payerCountry,
n.payerAccountBSB1,
n.payerAccountNumber1,
n.payerAccountBSB2,
n.payerAccountNumber2,
n.payerAccountBSB3,
n.payerAccountNumber3,
n.payerAbn,
n.payerAcn,
n.payerArbn,
n.payerIdentificationType1,
n.payerIdentificationTypeOther1,
n.payerIdentificationNumber1,
n.payerIdentificationIssuer1,
n.payerIdentificationCountry1,
n.payerIdentificationType2,
n.payerIdentificationTypeOther2,
n.payerIdentificationNumber2,
n.payerIdentificationIssuer2,
n.payerIdentificationCountry2,
n.payerIdentificationType3,
n.payerIdentificationTypeOther3,
n.payerIdentificationNumber3,
n.payerIdentificationIssuer3,
n.payerIdentificationCountry3,
n.payerCustNo,
n.payerIndividualDetailsDob,
n.payerDobSuburb,
n.payerDobState,
n.payerDobPostcode,
n.payerDobCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY 
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_EXC_CMMT </sc-trigger> */
Create TRIGGER U_D_DSV_001_NIT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_EXC_CMMT 
AFTER UPDATE OF OTHR_CMMT ON U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_IDENTIFIER,
            EXC_OFCR,
            EXC_OFCR_DTTM,
            EXC_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_IDENTIFIER,
            COALESCE (NewRow.EXC_OFCR, NULL, CURRENT_USER),
            COALESCE (NewRow.EXC_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
            NewRow.OTHR_CMMT
            );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus_Hist
  (
     LogDate
    ,RowsReturned
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.LogDate
    ,o.RowsReturned
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_Hist
  (
     Cycl_Start_Ts
    ,Cycl_End_Ts
    ,Cycl_Stat
    ,Src_Sys
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cycl_Start_Ts
    ,n.Cycl_End_Ts
    ,n.Cycl_Stat
    ,n.Src_Sys
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYR_TRIG_I </sc-trigger> */

-----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYR
 --------------------------------------------------------------------- PAYR
 --DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYR_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYR_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYR
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payerSameAsSwiftOrdCust,
PayerFullName,
payerAddr,
payerSuburb,
payerState,
payerPostcode,
payerCountry,
payerAccountBSB1,
payerAccountNumber1,
payerAccountBSB2,
payerAccountNumber2,
payerAccountBSB3,
payerAccountNumber3,
payerAbn,
payerAcn,
payerArbn,
payerIdentificationType1,
payerIdentificationTypeOther1,
payerIdentificationNumber1,
payerIdentificationIssuer1,
payerIdentificationCountry1,
payerIdentificationType2,
payerIdentificationTypeOther2,
payerIdentificationNumber2,
payerIdentificationIssuer2,
payerIdentificationCountry2,
payerIdentificationType3,
payerIdentificationTypeOther3,
payerIdentificationNumber3,
payerIdentificationIssuer3,
payerIdentificationCountry3,
payerCustNo,
payerIndividualDetailsDob,
payerDobSuburb,
payerDobState,
payerDobPostcode,
payerDobCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY 
)
VALUES
(
'IFTI_XML_REVW_PAYR',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payerSameAsSwiftOrdCust,
n.PayerFullName,
n.payerAddr,
n.payerSuburb,
n.payerState,
n.payerPostcode,
n.payerCountry,
n.payerAccountBSB1,
n.payerAccountNumber1,
n.payerAccountBSB2,
n.payerAccountNumber2,
n.payerAccountBSB3,
n.payerAccountNumber3,
n.payerAbn,
n.payerAcn,
n.payerArbn,
n.payerIdentificationType1,
n.payerIdentificationTypeOther1,
n.payerIdentificationNumber1,
n.payerIdentificationIssuer1,
n.payerIdentificationCountry1,
n.payerIdentificationType2,
n.payerIdentificationTypeOther2,
n.payerIdentificationNumber2,
n.payerIdentificationIssuer2,
n.payerIdentificationCountry2,
n.payerIdentificationType3,
n.payerIdentificationTypeOther3,
n.payerIdentificationNumber3,
n.payerIdentificationIssuer3,
n.payerIdentificationCountry3,
n.payerCustNo,
n.payerIndividualDetailsDob,
n.payerDobSuburb,
n.payerDobState,
n.payerDobPostcode,
n.payerDobCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY 
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_BASE_TRIG_D </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_BASE_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_BASE
REFERENCING OLD ROW AS n
FOR EACH ROW
WHEN (n.UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
filename,
txnRefNo ,
interceptFlag ,
isSwiftOrStructured,
swiftMsg ,
transferDate,
direction,
currency ,
currencyAmount,
valueDate ,    
RPRT_STUS ,
REVW_MODF_BY,
REVW_DTTM ,
SUPV_MODF_BY,
SUPV_DTTM ,
CRET_BY,
CRET_DTTM ,
INDA_DRFT ,
INDA_REDY,
INDA_APRD,
INDA_RWRK,
INDA_NOT_RPRT,
EXC_REAS ,
ENUS_CMMT ,
SDS_MSG_ID ,
SDS_MSG_SEQ,
base_transferDate,
base_direction,
base_currency ,
base_currencyAmount,
base_valueDate , 
INDA_PAYR,
INDA_PAYE,
INDA_ORDR_INST,
INDA_SEND_INST,
INDA_SEND_CORR,
INDA_BENE_INST,
INDA_RECE_INST,
INDA_RECE_CORR,
INDA_ADDN_DETL,
LINK_KEY       
)
VALUES
(
'IFTI_XML_REVW_BASE',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.filename,
n.txnRefNo ,
n.interceptFlag ,
n.isSwiftOrStructured,
n.swiftMsg ,
n.transferDate,
n.direction,
n.currency ,
n.currencyAmount,
n.valueDate ,    
n.RPRT_STUS ,
n.REVW_MODF_BY,
n.REVW_DTTM ,
n.SUPV_MODF_BY,
n.SUPV_DTTM ,
n.CRET_BY,
n.CRET_DTTM ,
n.INDA_DRFT ,
n.INDA_REDY,
n.INDA_APRD,
n.INDA_RWRK,
n.INDA_NOT_RPRT,
n.EXC_REAS ,
n.ENUS_CMMT ,
n.SDS_MSG_ID ,
n.SDS_MSG_SEQ,
n.base_transferDate,
n.base_direction,
n.base_currency ,
n.base_currencyAmount,
n.base_valueDate , 
n.INDA_PAYR,
n.INDA_PAYE,
n.INDA_ORDR_INST,
n.INDA_SEND_INST,
n.INDA_SEND_CORR,
n.INDA_BENE_INST,
n.INDA_RECE_INST,
n.INDA_RECE_CORR,
n.INDA_ADDN_DETL,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space_Hist
  (
     LogDate
    ,PermSpace
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.LogDate
    ,n.PermSpace
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Scope_Stream_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Scope_Stream_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Scope_Stream
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Scope_Stream_Hist
  (
     Stream_Name
    ,Stream_Type
    ,Job_Name
    ,Enabled_Flag
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Stream_Name
    ,o.Stream_Type
    ,o.Job_Name
    ,o.Enabled_Flag
    ,o.Allow_Cnsldtn_Flag
    ,o.Allow_Partial_Flag
    ,o.Start_Ts
    ,o.End_Ts
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYR_TRIG_D </sc-trigger> */


--DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE_TRIG_D;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYR_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_PAYR
REFERENCING OLD ROW AS n
FOR EACH ROW
WHEN (n.UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payerSameAsSwiftOrdCust,
PayerFullName,
payerAddr,
payerSuburb,
payerState,
payerPostcode,
payerCountry,
payerAccountBSB1,
payerAccountNumber1,
payerAccountBSB2,
payerAccountNumber2,
payerAccountBSB3,
payerAccountNumber3,
payerAbn,
payerAcn,
payerArbn,
payerIdentificationType1,
payerIdentificationTypeOther1,
payerIdentificationNumber1,
payerIdentificationIssuer1,
payerIdentificationCountry1,
payerIdentificationType2,
payerIdentificationTypeOther2,
payerIdentificationNumber2,
payerIdentificationIssuer2,
payerIdentificationCountry2,
payerIdentificationType3,
payerIdentificationTypeOther3,
payerIdentificationNumber3,
payerIdentificationIssuer3,
payerIdentificationCountry3,
payerCustNo,
payerIndividualDetailsDob,
payerDobSuburb,
payerDobState,
payerDobPostcode,
payerDobCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY 
)
VALUES
(
'IFTI_XML_REVW_PAYR',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payerSameAsSwiftOrdCust,
n.PayerFullName,
n.payerAddr,
n.payerSuburb,
n.payerState,
n.payerPostcode,
n.payerCountry,
n.payerAccountBSB1,
n.payerAccountNumber1,
n.payerAccountBSB2,
n.payerAccountNumber2,
n.payerAccountBSB3,
n.payerAccountNumber3,
n.payerAbn,
n.payerAcn,
n.payerArbn,
n.payerIdentificationType1,
n.payerIdentificationTypeOther1,
n.payerIdentificationNumber1,
n.payerIdentificationIssuer1,
n.payerIdentificationCountry1,
n.payerIdentificationType2,
n.payerIdentificationTypeOther2,
n.payerIdentificationNumber2,
n.payerIdentificationIssuer2,
n.payerIdentificationCountry2,
n.payerIdentificationType3,
n.payerIdentificationTypeOther3,
n.payerIdentificationNumber3,
n.payerIdentificationIssuer3,
n.payerIdentificationCountry3,
n.payerCustNo,
n.payerIndividualDetailsDob,
n.payerDobSuburb,
n.payerDobState,
n.payerDobPostcode,
n.payerDobCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY 
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_VERF_CMMT </sc-trigger> */
Create TRIGGER U_D_DSV_001_NIT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_VERF_CMMT 
AFTER UPDATE OF VERF_OFCR_CMMT ON U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_IDENTIFIER,
            VERF_OFCR,
            VERF_OFCR_DTTM,
            VERF_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_IDENTIFIER,
            COALESCE (NewRow.VERF_OFCR, NULL, CURRENT_USER),
            COALESCE (NewRow.VERF_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
            NewRow.VERF_OFCR_CMMT
            );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_NM
    ,TDM_Key_Column
    ,Pros_Isac_DB_NM
    ,ODS_Insert_Col_Flag
    ,ODS_Update_Col_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Nm
    ,n.Db_Nm
    ,n.Tbl_NM
    ,n.TDM_Key_Column
    ,n.Pros_Isac_DB_NM
    ,n.ODS_Insert_Col_Flag
    ,n.ODS_Update_Col_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I_Src
    ,MAX_Pros_Key_I_Tgt
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Stream_Nm
    ,o.Db_Nm
    ,o.Tbl_Nm
    ,o.User_ID
    ,o.Max_Pros_Key_I_Src
    ,o.MAX_Pros_Key_I_Tgt
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST_TRIG_I </sc-trigger> */

-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST
 --------------------------------------------------------------------- 
 --DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_SEND_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendingInstnCode,
sendingInstnName,
sendingInstnCity,
sendingInstnCountry,
sendingInstnBranchBSB,
sendingInstnBranchId,
sendingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_INST',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendingInstnCode,
n.sendingInstnName,
n.sendingInstnCity,
n.sendingInstnCountry,
n.sendingInstnBranchBSB,
n.sendingInstnBranchId,
n.sendingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_D </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
additionalDetailsOfPayment,
additionalOtherDetails,
additionalInstnCode,
additionalInstnName,
additionalInstnCity,
additionalInstnCountry,
additionalInstnAccountBSB,
additionalInstnAccountNumber,
senderToReceiverInfo1,
senderToReceiverInfo2,
senderToReceiverInfo3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ADDN_DETL',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.additionalDetailsOfPayment,
n.additionalOtherDetails,
n.additionalInstnCode,
n.additionalInstnName,
n.additionalInstnCity,
n.additionalInstnCountry,
n.additionalInstnAccountBSB,
n.additionalInstnAccountNumber,
n.senderToReceiverInfo1,
n.senderToReceiverInfo2,
n.senderToReceiverInfo3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers_Hist
  (
     InfoData
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.InfoData
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_Hist
  (
     ExtJob_Name
    ,ExtJob_RunNo
    ,ExtJob_Type
    ,ExtJob_Stat
    ,ExtJob_Beg_Ts
    ,ExtJob_End_Ts
    ,ExtJob_Pri
    ,Prev_ExtJob_End_Ts
    ,ExtJob_ErrNo
    ,ExtJob_ErrMsg
    ,Prg_Stat_Cd
    ,Prg_Stat_Desc
    ,ExtSys_Name
    ,ExtSys_Mode
    ,ExtSys_Capture_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.ExtJob_Name
    ,n.ExtJob_RunNo
    ,n.ExtJob_Type
    ,n.ExtJob_Stat
    ,n.ExtJob_Beg_Ts
    ,n.ExtJob_End_Ts
    ,n.ExtJob_Pri
    ,n.Prev_ExtJob_End_Ts
    ,n.ExtJob_ErrNo
    ,n.ExtJob_ErrMsg
    ,n.Prg_Stat_Cd
    ,n.Prg_Stat_Desc
    ,n.ExtSys_Name
    ,n.ExtSys_Mode
    ,n.ExtSys_Capture_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST_TRIG_I </sc-trigger> */

-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST
 
 --------------------------------------------------------------------- 
 --DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_ORDR_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
orderingInstnCode,
orderingInstnName,
orderingInstnCity,
orderingInstnCountry,
orderingInstnBranchBSB,
orderingInstnBranchId,
orderingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ORDR_INST',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.orderingInstnCode,
n.orderingInstnName,
n.orderingInstnCity,
n.orderingInstnCountry,
n.orderingInstnBranchBSB,
n.orderingInstnBranchId,
n.orderingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST_TRIG_D </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
orderingInstnCode,
orderingInstnName,
orderingInstnCity,
orderingInstnCountry,
orderingInstnBranchBSB,
orderingInstnBranchId,
orderingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ORDR_INST',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.orderingInstnCode,
n.orderingInstnName,
n.orderingInstnCity,
n.orderingInstnCountry,
n.orderingInstnBranchBSB,
n.orderingInstnBranchId,
n.orderingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_Hist
  (
     Daemon_Name
    ,Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Enabled_Flag
    ,Loop_Delay_Secs
    ,Send_Alert_On_Error_Flag
    ,Autosys_User
    ,Autosys_Host
    ,Autosys_Host_IP
    ,Autosys_Stus_Restrict_Flag
    ,Autosys_Stus_Allowed_Set
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Daemon_Name
    ,n.Prog_Name
    ,n.Prog_Mode
    ,n.Prog_User
    ,n.Prog_Host
    ,n.Enabled_Flag
    ,n.Loop_Delay_Secs
    ,n.Send_Alert_On_Error_Flag
    ,n.Autosys_User
    ,n.Autosys_Host
    ,n.Autosys_Host_IP
    ,n.Autosys_Stus_Restrict_Flag
    ,n.Autosys_Stus_Allowed_Set
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_Hist
  (
     Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Prog_Vers
    ,Proc_Id
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Enabled_Flag
    ,Prog_Beg_Ts
    ,Prog_End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Prog_Name
    ,o.Prog_Mode
    ,o.Prog_User
    ,o.Prog_Host
    ,o.Prog_Vers
    ,o.Proc_Id
    ,o.Prog_Stat_Cd
    ,o.Prog_Stat_Desc
    ,o.Enabled_Flag
    ,o.Prog_Beg_Ts
    ,o.Prog_End_Ts
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.QTLR_RPRT_EXC_UI_TRIG_U </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NFT_1.QTLR_RPRT_EXC_UI_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.QTLR_RPRT_EXC_UI
REFERENCING NEW ROW AS N
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.QTLR_RPRT_EXC_AUDT
(
	 AUDT_TABL_M                   
	,MODF_USER_M                   
	,MODF_DTTS                     
	,OPER_TYPE  , 
	  UID ,
      CLYR ,
      QRTR ,
      NOTC ,
      REPORTING_ENTITY_NO ,
      REPORTING_ENTITY_NAME ,
      REPORT_NO ,
      TRN ,
      TRANSACTION_VALUE_DATE ,
      TRANSACTION_CURRENCY_CODE ,
      TRANSACTION_AMOUNT ,
      BENEFICIARY_CUSTOMER ,
      ORDERING_INSTITUTION ,
      HEADER_BLOCKS ,
      BSB_NO ,
      ACCOUNT_NO ,
      ACCOUNT_NAME ,
      SEQUENCE_NO ,
      FAMILY_NAME ,
      FIRST_GIVEN_NAME ,
      SECOND_GIVEN_NAME ,
      BIRTH_DATE ,
      BUSINESS_NAME ,
      ADDRESS_LINE_1 ,
      ADDRESS_LINE_2 ,
      SUBURB ,
      STATE ,
      POSTCODE ,
      ABN ,
      ACN ,
      EXCEPTIONS ,
      BENEFICIARY_INSTITUTION_NAME ,
      RELATED_REFERENCE ,
      RECEIVING_BANK ,
      PAYMENT_CURRENCY_CODE ,
      PAYMENT_AMOUNT ,
      CHANNEL ,
      TRANSFER_DATE ,
      HEAD_BLOK_1 ,
      HEAD_BLOK_2 ,
      SWF_CODE ,
      PROS_ENTY ,
      INDA_BWA_NON_AUD ,
      TRN_DATE ,
      TRN_NUMBER ,
      TRN_TIMESTAMP,
      PAY_DATE ,
      CAN_MEMO ,
      TRADER_CTRL ,
      SWIFT_ID ,
      CDT_WIR_ID ,
      CDT_SPC_INST1 ,
      SBK_REF_NUM ,
      MESSAGE_TEXT ,
      SOURCE_CD ,
      IN_TYPE_CD ,
      IN_SUBTYPE ,
      CDT_ADV_TYP ,
      CURRENCY_CODE ,
      AMOUNT ,
      DBT_ID ,
      DBT_NAME1 ,
      DBT_BNK_INF1 ,
      DBT_BNK_INF2 ,
      DBT_BNK_INF3 ,
      DBT_BNK_INF4 ,
      DBT_CURRENCY ,
      DBT_AMOUNT ,
      DBT_SEC_ACCTG_AMT ,
      BBK_ID ,
      BBK_IDTYPE ,
      BBK_NAME1 ,
      BNP_ID ,
      BNP_NAME1 ,
      CDT_ID ,
      CDT_NAME1 ,
      CDT_CURRENCY ,
      CDT_AMOUNT ,
      CANCELLED_FLAG ,
      INDA_RANK_REL_REFN,
      INDA_AMH_FLTR ,
      INDA_MTCH_CCP2P ,
      DIFF_TRAN_AMT ,
      DIFF_TRAN_DATE,
      INDA_OFFSYST_BSB ,
      BULK_BSB_NORM ,
      BULK_ACCT_NORM ,
      INDA_RETN ,
      INDA_REJT ,
      DE_BSB_NUMB ,
      DE_ACCT_NUMB ,
      DE_INDA_RTN ,
      INDA_MTCH_DE ,
      RESP_BENEFICIARY_INSTITUTION_NAME ,
      RESP_RELATED_REFERENCE ,
      RESP_RECEIVING_BANK ,
      RESP_PAYMENT_CURRENCY_CODE ,
      RESP_PAYMENT_AMOUNT ,
      CDT_ADV_TYP_CHNL ,
      RESP_CHANNEL ,
      RESP_TRANSFER_DATE ,
      INDA_DUPL_ACCT ,
      ACCT_I ,
      PATY_ACCT_REL_M ,
      PATY_I ,
      PATY_TYPE_C ,
      ADRS_I ,
      CLSE_D ,
      MAX_REL_EXPY_D ,
      MAX_ACCT_M_EXPY_D ,
      PATY_LIFE_CYCL_EXPY_D ,
      ACCT_NAME ,
      ACCOUNT_NAME_2 ,
      SEQN_NUMB,
      FRST_M ,
      SCND_M ,
      SRNM_M ,
      PATY_LIFE_CYCL_D ,
      BUSN_NAME ,
      ADRS_CATG_C ,
      ADRS_QUAL_C ,
      ADRS_LINE_1_X ,
      ADRS_LINE_2_X ,
      SURB_X ,
      CITY_X ,
      PCOD_C ,
      STAT_C ,
      IDNN_VALU_X_ABN ,
      IDNN_VALU_X_ACN ,
      INDA_CBA_ACCT ,
      BWA_SRCE_SYST_ACCT ,
      BWA_PATY_TYPE_C ,
      BWA_ACCT_I ,
      BWA_PATY_ACCT_REL_C ,
      BWA_ORGN_TYPE_X ,
      BWA_PATY_I ,
      BWA_ADRS_I ,
      BWA_ACCT_NAME ,
      BWA_SEQN_NUMB,
      BWA_SRNM_M ,
      BWA_FRST_M ,
      BWA_SCND_M ,
      BWA_PATY_LIFE_CYCL_D ,
      BWA_BUSN_NAME ,
      BWA_IDNN_VALU_X_ABN ,
      BWA_IDNN_VALU_X_ACN ,
      BWA_ADRS_LINE_1_X ,
      BWA_ADRS_LINE_2_X ,
      BWA_SURB_X ,
      BWA_STAT_C ,
      BWA_PCOD_C ,
      BWA_ISO_CNTY_C ,
      RESP_EXCEPTIONS ,
      RESP_BSB_NO ,
      RESP_ACCOUNT_NO ,
      RESP_ACCOUNT_NAME ,
      RESP_SEQUENCE_NO,
      RESP_FAMILY_NAME ,
      RESP_FIRST_GIVEN_NAME ,
      RESP_SECOND_GIVEN_NAME ,
      RESP_BIRTH_DATE ,
      RESP_BUSINESS_NAME ,
      RESP_ADDRESS_LINE_1 ,
      RESP_ADDRESS_LINE_2 ,
      RESP_SUBURB ,
      RESP_STATE_INMD ,
      RESP_STATE ,
      RESP_POSTCODE ,
      RESP_ABN ,
      RESP_ACN ,
      UID2 ,
      INDA_ONWD_TRAN ,
      INDA_MTCH_BWA_MANL_RQST ,
      INDA_MTCH_BWA_MANL_RESP ,
      ENUS_REVW_STUS ,
      ENUS_CMMT ,
      ENUS_EXC_REFN ,
      ENUS_MODF_BY ,
      ENUS_REVW_DTTM,
      SUPV_REVW_STUS ,
      SUPV_MODF_BY ,
      SUPV_REVW_DTTM
	  )
VALUES
(
	 'QTLR_RPRT_EXC_UI'
	,USER
	,CURRENT_TIMESTAMP(6)
	,'UPDATE' , 
		  N.UID ,
      N.CLYR ,
      N.QRTR ,
      N.NOTC ,
      N.REPORTING_ENTITY_NO ,
      N.REPORTING_ENTITY_NAME ,
      N.REPORT_NO ,
      N.TRN ,
      N.TRANSACTION_VALUE_DATE ,
      N.TRANSACTION_CURRENCY_CODE ,
      N.TRANSACTION_AMOUNT ,
      N.BENEFICIARY_CUSTOMER ,
      N.ORDERING_INSTITUTION ,
      N.HEADER_BLOCKS ,
      N.BSB_NO ,
      N.ACCOUNT_NO ,
      N.ACCOUNT_NAME ,
      N.SEQUENCE_NO ,
      N.FAMILY_NAME ,
      N.FIRST_GIVEN_NAME ,
      N.SECOND_GIVEN_NAME ,
      N.BIRTH_DATE ,
      N.BUSINESS_NAME ,
      N.ADDRESS_LINE_1 ,
      N.ADDRESS_LINE_2 ,
      N.SUBURB ,
      N.STATE ,
      N.POSTCODE ,
      N.ABN ,
      N.ACN ,
      N.EXCEPTIONS ,
      N.BENEFICIARY_INSTITUTION_NAME ,
      N.RELATED_REFERENCE ,
      N.RECEIVING_BANK ,
      N.PAYMENT_CURRENCY_CODE ,
      N.PAYMENT_AMOUNT ,
      N.CHANNEL ,
      N.TRANSFER_DATE ,
      N.HEAD_BLOK_1 ,
      N.HEAD_BLOK_2 ,
      N.SWF_CODE ,
      N.PROS_ENTY ,
      N.INDA_BWA_NON_AUD ,
      N.TRN_DATE ,
      N.TRN_NUMBER ,
      N.TRN_TIMESTAMP,
      N.PAY_DATE ,
      N.CAN_MEMO ,
      N.TRADER_CTRL ,
      N.SWIFT_ID ,
      N.CDT_WIR_ID ,
      N.CDT_SPC_INST1 ,
      N.SBK_REF_NUM ,
      N.MESSAGE_TEXT ,
      N.SOURCE_CD ,
      N.IN_TYPE_CD ,
      N.IN_SUBTYPE ,
      N.CDT_ADV_TYP ,
      N.CURRENCY_CODE ,
      N.AMOUNT ,
      N.DBT_ID ,
      N.DBT_NAME1 ,
      N.DBT_BNK_INF1 ,
      N.DBT_BNK_INF2 ,
      N.DBT_BNK_INF3 ,
      N.DBT_BNK_INF4 ,
      N.DBT_CURRENCY ,
      N.DBT_AMOUNT ,
      N.DBT_SEC_ACCTG_AMT ,
      N.BBK_ID ,
      N.BBK_IDTYPE ,
      N.BBK_NAME1 ,
      N.BNP_ID ,
      N.BNP_NAME1 ,
      N.CDT_ID ,
      N.CDT_NAME1 ,
      N.CDT_CURRENCY ,
      N.CDT_AMOUNT ,
      N.CANCELLED_FLAG ,
      N.INDA_RANK_REL_REFN,
      N.INDA_AMH_FLTR ,
      N.INDA_MTCH_CCP2P ,
      N.DIFF_TRAN_AMT ,
      N.DIFF_TRAN_DATE,
      N.INDA_OFFSYST_BSB ,
      N.BULK_BSB_NORM ,
      N.BULK_ACCT_NORM ,
      N.INDA_RETN ,
      N.INDA_REJT ,
      N.DE_BSB_NUMB ,
      N.DE_ACCT_NUMB ,
      N.DE_INDA_RTN ,
      N.INDA_MTCH_DE ,
      N.RESP_BENEFICIARY_INSTITUTION_NAME ,
      N.RESP_RELATED_REFERENCE ,
      N.RESP_RECEIVING_BANK ,
      N.RESP_PAYMENT_CURRENCY_CODE ,
      N.RESP_PAYMENT_AMOUNT ,
      N.CDT_ADV_TYP_CHNL ,
      N.RESP_CHANNEL ,
      N.RESP_TRANSFER_DATE ,
      N.INDA_DUPL_ACCT ,
      N.ACCT_I ,
      N.PATY_ACCT_REL_M ,
      N.PATY_I ,
      N.PATY_TYPE_C ,
      N.ADRS_I ,
      N.CLSE_D ,
      N.MAX_REL_EXPY_D ,
      N.MAX_ACCT_M_EXPY_D ,
      N.PATY_LIFE_CYCL_EXPY_D ,
      N.ACCT_NAME ,
      N.ACCOUNT_NAME_2 ,
      N.SEQN_NUMB,
      N.FRST_M ,
      N.SCND_M ,
      N.SRNM_M ,
      N.PATY_LIFE_CYCL_D ,
      N.BUSN_NAME ,
      N.ADRS_CATG_C ,
      N.ADRS_QUAL_C ,
      N.ADRS_LINE_1_X ,
      N.ADRS_LINE_2_X ,
      N.SURB_X ,
      N.CITY_X ,
      N.PCOD_C ,
      N.STAT_C ,
      N.IDNN_VALU_X_ABN ,
      N.IDNN_VALU_X_ACN ,
      N.INDA_CBA_ACCT ,
      N.BWA_SRCE_SYST_ACCT ,
      N.BWA_PATY_TYPE_C ,
      N.BWA_ACCT_I ,
      N.BWA_PATY_ACCT_REL_C ,
      N.BWA_ORGN_TYPE_X ,
      N.BWA_PATY_I ,
      N.BWA_ADRS_I ,
      N.BWA_ACCT_NAME ,
      N.BWA_SEQN_NUMB,
      N.BWA_SRNM_M ,
      N.BWA_FRST_M ,
      N.BWA_SCND_M ,
      N.BWA_PATY_LIFE_CYCL_D ,
      N.BWA_BUSN_NAME ,
      N.BWA_IDNN_VALU_X_ABN ,
      N.BWA_IDNN_VALU_X_ACN ,
      N.BWA_ADRS_LINE_1_X ,
      N.BWA_ADRS_LINE_2_X ,
      N.BWA_SURB_X ,
      N.BWA_STAT_C ,
      N.BWA_PCOD_C ,
      N.BWA_ISO_CNTY_C ,
      N.RESP_EXCEPTIONS ,
      N.RESP_BSB_NO ,
      N.RESP_ACCOUNT_NO ,
      N.RESP_ACCOUNT_NAME ,
      N.RESP_SEQUENCE_NO,
      N.RESP_FAMILY_NAME ,
      N.RESP_FIRST_GIVEN_NAME ,
      N.RESP_SECOND_GIVEN_NAME ,
      N.RESP_BIRTH_DATE ,
      N.RESP_BUSINESS_NAME ,
      N.RESP_ADDRESS_LINE_1 ,
      N.RESP_ADDRESS_LINE_2 ,
      N.RESP_SUBURB ,
      N.RESP_STATE_INMD ,
      N.RESP_STATE ,
      N.RESP_POSTCODE ,
      N.RESP_ABN ,
      N.RESP_ACN ,
      N.UID2 ,
      N.INDA_ONWD_TRAN ,
      N.INDA_MTCH_BWA_MANL_RQST ,
      N.INDA_MTCH_BWA_MANL_RESP ,
      N.ENUS_REVW_STUS ,
      N.ENUS_CMMT ,
      N.ENUS_EXC_REFN ,
      N.ENUS_MODF_BY ,
      N.ENUS_REVW_DTTM,
      N.SUPV_REVW_STUS ,
      N.SUPV_MODF_BY ,
      N.SUPV_REVW_DTTM
	  );

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYR_TRIG_U </sc-trigger> */


--update
--DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_BASE_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYR_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_PAYR
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
payerSameAsSwiftOrdCust,
PayerFullName,
payerAddr,
payerSuburb,
payerState,
payerPostcode,
payerCountry,
payerAccountBSB1,
payerAccountNumber1,
payerAccountBSB2,
payerAccountNumber2,
payerAccountBSB3,
payerAccountNumber3,
payerAbn,
payerAcn,
payerArbn,
payerIdentificationType1,
payerIdentificationTypeOther1,
payerIdentificationNumber1,
payerIdentificationIssuer1,
payerIdentificationCountry1,
payerIdentificationType2,
payerIdentificationTypeOther2,
payerIdentificationNumber2,
payerIdentificationIssuer2,
payerIdentificationCountry2,
payerIdentificationType3,
payerIdentificationTypeOther3,
payerIdentificationNumber3,
payerIdentificationIssuer3,
payerIdentificationCountry3,
payerCustNo,
payerIndividualDetailsDob,
payerDobSuburb,
payerDobState,
payerDobPostcode,
payerDobCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY 
)
VALUES
(
'IFTI_XML_REVW_PAYR',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.payerSameAsSwiftOrdCust,
n.PayerFullName,
n.payerAddr,
n.payerSuburb,
n.payerState,
n.payerPostcode,
n.payerCountry,
n.payerAccountBSB1,
n.payerAccountNumber1,
n.payerAccountBSB2,
n.payerAccountNumber2,
n.payerAccountBSB3,
n.payerAccountNumber3,
n.payerAbn,
n.payerAcn,
n.payerArbn,
n.payerIdentificationType1,
n.payerIdentificationTypeOther1,
n.payerIdentificationNumber1,
n.payerIdentificationIssuer1,
n.payerIdentificationCountry1,
n.payerIdentificationType2,
n.payerIdentificationTypeOther2,
n.payerIdentificationNumber2,
n.payerIdentificationIssuer2,
n.payerIdentificationCountry2,
n.payerIdentificationType3,
n.payerIdentificationTypeOther3,
n.payerIdentificationNumber3,
n.payerIdentificationIssuer3,
n.payerIdentificationCountry3,
n.payerCustNo,
n.payerIndividualDetailsDob,
n.payerDobSuburb,
n.payerDobState,
n.payerDobPostcode,
n.payerDobCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY 
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_Hist
  (
     Cycl_Start_Ts
    ,Cycl_End_Ts
    ,Cycl_Stat
    ,Src_Sys
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cycl_Start_Ts
    ,n.Cycl_End_Ts
    ,n.Cycl_Stat
    ,n.Src_Sys
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_Hist
  (
     Log_Date
    ,Log_TS
    ,The_Count
    ,DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Gen_RI_SQL_Text
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Log_Date
    ,n.Log_TS
    ,n.The_Count
    ,n.DB_Name
    ,n.Obj_Name
    ,n.Col_1_Name
    ,n.Col_2_Name
    ,n.Join_Col_1_Name
    ,n.Join_Col_2_Name
    ,n.Predicate_Clause_Text
    ,n.Join_Type
    ,n.Rltd_DB_Name
    ,n.Rltd_Obj_Name
    ,n.Rltd_Col_1_Name
    ,n.Rltd_Col_2_Name
    ,n.Rltd_Join_Col_1_Name
    ,n.Rltd_Join_Col_2_Name
    ,n.Rltd_Predicate_Clause_Text
    ,n.Gen_RI_SQL_Text
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */
Replace TRIGGER U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_AUDT
(stmt_line_sk
,unique_id
,rprt_name
,acct_no
,acct_name
,acct_cncy
,acct_baln_pool
,date_tag_created
,value_date
,amt
,txn_type
,internal_ref
,external_ref
,description
,swf_file_ref
,mtch_type
,team_resp
,investigation_stus
,tran_catg
,is_reviewed
,othr_cmmt
,busn_days
,ifti_rprt
,subm_date
,subm_file_name
,subm_rcpt_no
,imt_mtch_grup_id
,imt_refn_1
,imt_refn_2
,imt_refn_3
,pms_ref_no
,pms_dbt_name
,pms_sbk_ref_no
,wss_srce
,wss_portfolio
,wss_audt_note
,wss_net_info
,wss_area
,itc_case_no
,wss_extl_deal_numb
,action_date
,EXC_OFCR
,EXC_OFCR_DTTM
,VERF_OFCR_CMMT
,VERF_OFCR
,VERF_OFCR_DTTM
,QA_OFCR_CMMT
,QA_OFCR
,QA_OFCR_DTTM
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(n.stmt_line_sk
,n.unique_id
,n.rprt_name
,n.acct_no
,n.acct_name
,n.acct_cncy
,n.acct_baln_pool
,n.date_tag_created
,n.value_date
,n.amt
,n.txn_type
,n.internal_ref
,n.external_ref
,n.description
,n.swf_file_ref
,n.mtch_type
,n.team_resp
,n.investigation_stus
,n.tran_catg
,n.is_reviewed
,n.othr_cmmt
,n.busn_days
,n.ifti_rprt
,n.subm_date
,n.subm_file_name
,n.subm_rcpt_no
,n.imt_mtch_grup_id
,n.imt_refn_1
,n.imt_refn_2
,n.imt_refn_3
,n.pms_ref_no
,n.pms_dbt_name
,n.pms_sbk_ref_no
,n.wss_srce
,n.wss_portfolio
,n.wss_audt_note
,n.wss_net_info
,n.wss_area
,n.itc_case_no
,n.wss_extl_deal_numb
,n.action_date
,n.EXC_OFCR
,n.EXC_OFCR_DTTM
,n.VERF_OFCR_CMMT
,n.VERF_OFCR
,n.VERF_OFCR_DTTM
,n.QA_OFCR_CMMT
,n.QA_OFCR
,n.QA_OFCR_DTTM
,USER
,Current_Timestamp(6)
,'UPDATE' 
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.NOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'INSERT'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers_Hist
  (
     InfoData
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.InfoData
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Scope_Stream_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Scope_Stream_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Scope_Stream
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Scope_Stream_Hist
  (
     Stream_Name
    ,Stream_Type
    ,Job_Name
    ,Enabled_Flag
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Name
    ,n.Stream_Type
    ,n.Job_Name
    ,n.Enabled_Flag
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Start_Ts
    ,n.End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */








-- Bankwest Tigger Delete
--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_BWA_CR_TRIG_D
replace trigger U_D_DSV_001_NFT_1.NOST_BWA_CR_EXC_RPRT_ACCS_TRIG_D
after delete on U_D_DSV_001_NFT_1.NOST_BWA_CR_EXC_RPRT_ACCESS
referencing old row as o
for each row
insert into U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
values
(
 'NOST_BWA_CR_EXC_RPRT_ACCESS'
,user
,current_timestamp(6)
,'DELETE'  
,o.unique_identifier             
,o.match_type                    
,o.Nostro_acct                   
,o.CURR_NOSTRO                   
,o.Group_area                    
,o.Acct_Name                     
,o.external_party_bic            
,o.value_date                    
,o.AMOUNT                        
,o.Message_Type                  
,o.message_type_desc             
,o.tag_61_stmt_line              
,o.internal_ref                  
,o.external_ref                  
,o.description                   
,o.department                    
,o.Investigation_Flag            
,o.WS_flag                       
,o.deal_number                   
,o.cust_number                   
,o.broker_name                   
,o.product_cat                   
,o.Department_Responsible        
,o.Reason                        
,o.Other_Comment                 
,o.Escalate                      
,o.IFTI_Reportable               
,o.Payor_Bank                    
,o.Payor_Details                 
,o.Payee_Bank                    
,o.Payee_Details                 
,o.Investigation_Status          
,o.MatchID                       
,o.deal_num_list                 
,o.Beneficiary                   
,o.Remark                        
,o.Settle                        
,o.Category_Correct              
,o.Updated                       
,o.msg_id                        
,o.MSG_TAG_SEQ                   
,o.CBA_BIC                       
,o.currency_tag62                
,o.date_tag_created              
,o.amount_matched                
,o.Buckets_f                     
,o.Bucket                        
,o.WS_GDW_source                 
,o.cust_code                     
,o.cust_type                     
,o.ext_ref_nr                    
,o.portfolio_no                  
,o.Acc                           
,o.Additional_Field              
,o.Pay_Meth                      
,o.swift_50                      
,o.Remittance_info               
,o.Enough_Data_For_IFTI          
,o.swf_File_Ref                  
,o.compliance_exclusion
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_QA_CMMT </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.TRI_MT202_EXC_RPRT_ACCS_UPDATE_QA_CMMT 
AFTER UPDATE OF QA_OFCR_CMMT ON U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NIT_1.MT202_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_IDENTIFIER,
            QA_OFCR,
            QA_OFCR_DTTM,
            QA_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_IDENTIFIER,
--            COALESCE (NewRow.QA_OFCR, NULL, CURRENT_USER),
			(CASE 
			WHEN NewRow.QA_OFCR IS NULL THEN CURRENT_USER
			WHEN NewRow.QA_OFCR = '' THEN CURRENT_USER
			ELSE NewRow.QA_OFCR
			END),
            COALESCE (NewRow.QA_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
/*			(CASE
			WHEN NewRow.QA_OFCR_DTTM IS NULL THEN CURRENT_TIMESTAMP
			WHEN NewRow.QA_OFCR_DTTM = '' THEN CURRENT_TIMESTAMP
			ELSE NewRow.QA_OFCR_DTTM
			END),*/
			NewRow.QA_OFCR_CMMT
            );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_Hist
  (
     Log_Date
    ,Log_TS
    ,The_Count
    ,DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Gen_RI_SQL_Text
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Log_Date
    ,n.Log_TS
    ,n.The_Count
    ,n.DB_Name
    ,n.Obj_Name
    ,n.Col_1_Name
    ,n.Col_2_Name
    ,n.Join_Col_1_Name
    ,n.Join_Col_2_Name
    ,n.Predicate_Clause_Text
    ,n.Join_Type
    ,n.Rltd_DB_Name
    ,n.Rltd_Obj_Name
    ,n.Rltd_Col_1_Name
    ,n.Rltd_Col_2_Name
    ,n.Rltd_Join_Col_1_Name
    ,n.Rltd_Join_Col_2_Name
    ,n.Rltd_Predicate_Clause_Text
    ,n.Gen_RI_SQL_Text
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id_Hist
  (
     Fixed_Value
    ,Next_Job_Id
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Fixed_Value
    ,o.Next_Job_Id
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */



--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_CBA_DR_TRIG_U
REPLACE TRIGGER U_D_DSV_001_NFT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.NOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'UPDATE'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_I </sc-trigger> */
-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL
 
 --------------------------------------------------------------------- 
 --DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
additionalDetailsOfPayment,
additionalOtherDetails,
additionalInstnCode,
additionalInstnName,
additionalInstnCity,
additionalInstnCountry,
additionalInstnAccountBSB,
additionalInstnAccountNumber,
senderToReceiverInfo1,
senderToReceiverInfo2,
senderToReceiverInfo3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ADDN_DETL',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.additionalDetailsOfPayment,
n.additionalOtherDetails,
n.additionalInstnCode,
n.additionalInstnName,
n.additionalInstnCity,
n.additionalInstnCountry,
n.additionalInstnAccountBSB,
n.additionalInstnAccountNumber,
n.senderToReceiverInfo1,
n.senderToReceiverInfo2,
n.senderToReceiverInfo3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_SQLState_Cds_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_SQLState_Cds_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_SQLState_Cds
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_SQLState_Cds_Hist
  (
     SQLStateCd
    ,ClassCd
    ,SubClassCd
    ,TDErrorCd
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     n.SQLStateCd
    ,n.ClassCd
    ,n.SubClassCd
    ,n.TDErrorCd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_TS
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_TS
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_Hist
  (
     ExtJob_Name
    ,ExtJob_RunNo
    ,ExtJob_Type
    ,ExtJob_Stat
    ,ExtJob_Beg_Ts
    ,ExtJob_End_Ts
    ,ExtJob_Pri
    ,Prev_ExtJob_End_Ts
    ,ExtJob_ErrNo
    ,ExtJob_ErrMsg
    ,Prg_Stat_Cd
    ,Prg_Stat_Desc
    ,ExtSys_Name
    ,ExtSys_Mode
    ,ExtSys_Capture_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.ExtJob_Name
    ,n.ExtJob_RunNo
    ,n.ExtJob_Type
    ,n.ExtJob_Stat
    ,n.ExtJob_Beg_Ts
    ,n.ExtJob_End_Ts
    ,n.ExtJob_Pri
    ,n.Prev_ExtJob_End_Ts
    ,n.ExtJob_ErrNo
    ,n.ExtJob_ErrMsg
    ,n.Prg_Stat_Cd
    ,n.Prg_Stat_Desc
    ,n.ExtSys_Name
    ,n.ExtSys_Mode
    ,n.ExtSys_Capture_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */



--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_CBA_CR_TRIG_U
REPLACE TRIGGER U_D_DSV_001_NFT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.NOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'UPDATE'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_ORDR_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
orderingInstnCode,
orderingInstnName,
orderingInstnCity,
orderingInstnCountry,
orderingInstnBranchBSB,
orderingInstnBranchId,
orderingInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ORDR_INST',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.orderingInstnCode,
n.orderingInstnName,
n.orderingInstnCity,
n.orderingInstnCountry,
n.orderingInstnBranchBSB,
n.orderingInstnBranchId,
n.orderingInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Alrt_Dest_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Alrt_Dest_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Alrt_Dest
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Alrt_Dest_Hist
  (
     Destination
    ,DestinationDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Destination
    ,n.DestinationDesc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Cand
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Hist
  (
     Cand_Id
    ,Description
    ,Trtmnt_Cd
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Priority
    ,Cand_Source
    ,Source_Id
    ,Stream_End_TS
    ,Enabled_Flag
    ,Num_Retries
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Description
    ,n.Trtmnt_Cd
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Priority
    ,n.Cand_Source
    ,n.Source_Id
    ,n.Stream_End_TS
    ,n.Enabled_Flag
    ,n.Num_Retries
    ,n.TDM_Job_Priority
    ,n.TDM_Job_Type
    ,n.TDM_Job_Source
    ,n.TDM_Job_Description
    ,n.TDM_Source_TDPID
    ,n.TDM_Source_User
    ,n.TDM_Source_Passwd_Encrypted
    ,n.TDM_Source_Logon_Mech
    ,n.TDM_Source_Logon_Mech_Data
    ,n.TDM_Source_Userid_Pool
    ,n.TDM_Source_Account_Id
    ,n.TDM_Source_Sessions
    ,n.TDM_Target_TDPID
    ,n.TDM_Target_User
    ,n.TDM_Target_Passwd_Encrypted
    ,n.TDM_Target_Logon_Mech
    ,n.TDM_Target_Logon_Mech_Data
    ,n.TDM_Target_Userid_Pool
    ,n.TDM_Target_Account_Id
    ,n.TDM_Target_Sessions
    ,n.TDM_Data_Streams
    ,n.TDM_Response_Timeout
    ,n.TDM_Overwrite_Existing_Obj
    ,n.TDM_Force_Utility
    ,n.TDM_Log_Level
    ,n.TDM_Online_Archive
    ,n.TDM_Freeze_Job_Steps
    ,n.TDM_Max_Agents_Per_Task
    ,n.TDM_Additional_ARC_Parameters
    ,n.TDM_Query_Band_Str
    ,n.TDM_Sync_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR_TRIG_I </sc-trigger> */

-----------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------
 
 --show table U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR
 
 --------------------------------------------------------------------- 
 --DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR_TRIG_I; 
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_RECE_CORR
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receiversCorrespondentCode1,
receiversCorrespondentName1,
receiversCorrespondentCity1,
receiversCorrespondentCountry1,
receiversCorrespondentCode2,
receiversCorrespondentName2,
receiversCorrespondentCity2,
receiversCorrespondentCountry2,
receiversCorrespondentCode3,
receiversCorrespondentName3,
receiversCorrespondentCity3,
receiversCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_CORR',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receiversCorrespondentCode1,
n.receiversCorrespondentName1,
n.receiversCorrespondentCity1,
n.receiversCorrespondentCountry1,
n.receiversCorrespondentCode2,
n.receiversCorrespondentName2,
n.receiversCorrespondentCity2,
n.receiversCorrespondentCountry2,
n.receiversCorrespondentCode3,
n.receiversCorrespondentName3,
n.receiversCorrespondentCity3,
n.receiversCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_ADMT_TRIG_I </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_ADMT_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.IFTI_XML_ADMT
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_ADMT_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
FILENAME,
RPRT_CNT,
IFTI_XML,
FILE_COUR,
CRET_BY,
CRET_DTTM,
FILE_RNAM_BY,
FILE_RNAM_DTTM,
APRV_BY,
APRV_DTTM,
RCPT_N,
AUSTC_REFN,
SUBM_DT,
ENUS_CMMT,
REVER_OFCR_CMMT,
REVER_OFCR,
REVER_OFCR_DTTM,
VERF_OFCR_CMMT,
VERF_OFCR,
VERF_OFCR_DTTM,
QA_OFCR_CMMT,
QA_OFCR,
QA_OFCR_DTTM,
IS_REVIEWED
)
VALUES
(
'IFTI_XML_ADMT',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.FILENAME,
n.RPRT_CNT,
n.IFTI_XML,
n.FILE_COUR,
n.CRET_BY,
n.CRET_DTTM,
n.FILE_RNAM_BY,
n.FILE_RNAM_DTTM,
n.APRV_BY,
n.APRV_DTTM,
n.RCPT_N,
n.AUSTC_REFN,
n.SUBM_DT,
n.ENUS_CMMT,
n.REVER_OFCR_CMMT,
n.REVER_OFCR,
n.REVER_OFCR_DTTM,
n.VERF_OFCR_CMMT,
n.VERF_OFCR,
n.VERF_OFCR_DTTM,
n.QA_OFCR_CMMT,
n.QA_OFCR,
n.QA_OFCR_DTTM,
n.IS_REVIEWED
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Limit_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Limit_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Cand_Limit
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Limit_Hist
  (
     Limit_No
    ,Candidates_Per_Iteration
    ,Obj_Per_Iteration
    ,MB_Per_Iteration
    ,Candidates_Per_Hour
    ,Obj_Per_Hour
    ,MB_Per_Hour
    ,Num_Running_DM_Jobs
    ,Num_Running_Tbl
    ,Num_Staged_Jobs
    ,Num_Staged_Tbl
    ,Num_Total_Jobs
    ,Num_Total_Tbl
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Limit_No
    ,n.Candidates_Per_Iteration
    ,n.Obj_Per_Iteration
    ,n.MB_Per_Iteration
    ,n.Candidates_Per_Hour
    ,n.Obj_Per_Hour
    ,n.MB_Per_Hour
    ,n.Num_Running_DM_Jobs
    ,n.Num_Running_Tbl
    ,n.Num_Staged_Jobs
    ,n.Num_Staged_Tbl
    ,n.Num_Total_Jobs
    ,n.Num_Total_Tbl
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Obj_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Obj_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDM_Job_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Hist
  (
     Job_Cd
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Db_Nm
    ,n.Obj_Nm
    ,n.Obj_Type
    ,n.Copy_Stat
    ,n.Compare_DDL
    ,n.Where_Clause
    ,n.Key_Column
    ,n.Query_Band_Str
    ,n.Job_Desc
    ,n.Staging_Database_Nm
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */



--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_BWA_CR_TRIG_U
REPLACE TRIGGER U_D_DSV_001_NFT_1.VOST_CBA_CR_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.VOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.VOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE  ,                   
Unique_Identifier,
Msg_ID,
Msg_Tag_Seq,
Tag_61_Stmt_Line,
Header_Msg_Type,
Vostro_Acct,
Vostro_Name,
External_Party_BIC,
Amount,
currency_tag62,
Message_Type,
Date_Tag_Created,
Value_Date,
Internal_Ref,
External_Ref,
Description,
Match_Type,
Investigations,
DBT_Name,
SBK_Ref_Num,
Other_Flags,
Investigation_Status,
Department_Responsible,
Reason,
IFTI_Reportable,
Enough_Data_For_IFTI,
Escalate,
Other_Comment,
Updated
)
VALUES
(
 'VOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'UPDATE'  ,
n.Unique_Identifier,
n.Msg_ID,
n.Msg_Tag_Seq,
n.Tag_61_Stmt_Line,
n.Header_Msg_Type,
n.Vostro_Acct,
n.Vostro_Name,
n.External_Party_BIC,
n.Amount,
n.currency_tag62,
n.Message_Type,
n.Date_Tag_Created,
n.Value_Date,
n.Internal_Ref,
n.External_Ref,
n.Description,
n.Match_Type,
n.Investigations,
n.DBT_Name,
n.SBK_Ref_Num,
n.Other_Flags,
n.Investigation_Status,
n.Department_Responsible,
n.Reason,
n.IFTI_Reportable,
n.Enough_Data_For_IFTI,
n.Escalate,
n.Other_Comment,
n.Updated
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_ADMT_TRIG_D </sc-trigger> */

Replace TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_ADMT_TRIG_D
AFTER DELETE ON U_D_DSV_001_NIT_1.IFTI_XML_ADMT
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_ADMT_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
FILENAME,
RPRT_CNT,
IFTI_XML,
FILE_COUR,
CRET_BY,
CRET_DTTM,
FILE_RNAM_BY,
FILE_RNAM_DTTM,
APRV_BY,
APRV_DTTM,
RCPT_N,
AUSTC_REFN,
SUBM_DT,
ENUS_CMMT,
REVER_OFCR_CMMT,
REVER_OFCR,
REVER_OFCR_DTTM,
VERF_OFCR_CMMT,
VERF_OFCR,
VERF_OFCR_DTTM,
QA_OFCR_CMMT,
QA_OFCR,
QA_OFCR_DTTM,
IS_REVIEWED
)
VALUES
(
'IFTI_XML_ADMT',
USER,
CURRENT_TIMESTAMP(6),
'DELETE',     
n.FILENAME,
n.RPRT_CNT,
n.IFTI_XML,
n.FILE_COUR,
n.CRET_BY,
n.CRET_DTTM,
n.FILE_RNAM_BY,
n.FILE_RNAM_DTTM,
n.APRV_BY,
n.APRV_DTTM,
n.RCPT_N,
n.AUSTC_REFN,
n.SUBM_DT,
n.ENUS_CMMT,
n.REVER_OFCR_CMMT,
n.REVER_OFCR,
n.REVER_OFCR_DTTM,
n.VERF_OFCR_CMMT,
n.VERF_OFCR,
n.VERF_OFCR_DTTM,
n.QA_OFCR_CMMT,
n.QA_OFCR,
n.QA_OFCR_DTTM,
n.IS_REVIEWED
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_ADT </sc-trigger> */
-- Replace triggers 

REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_Hist
  (
     Db_Nm
    ,Obj_Nm
    ,Job_Cd
    ,UOWId
    ,JobName
    ,Source_TDPID
    ,Export_Completion_TS
    ,Export_Completion_Date
    ,Copy_Completion_TS
    ,Copy_Completion_Date
    ,Utility_Name
    ,EventMsg
    ,Row_Count
    ,Byte_Count
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Db_Nm
    ,o.Obj_Nm
    ,o.Job_Cd
    ,o.UOWId
    ,o.JobName
    ,o.Source_TDPID
    ,o.Export_Completion_TS
    ,o.Export_Completion_Date
    ,o.Copy_Completion_TS
    ,o.Copy_Completion_Date
    ,o.Utility_Name
    ,o.EventMsg
    ,o.Row_Count
    ,o.Byte_Count
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_Hist
  (
     Cand_Id
    ,Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Id
    ,o.Job_Cd
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_D </sc-trigger> */
Replace TRIGGER U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_D
AFTER DELETE ON U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_ACCESS
REFERENCING OLD ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.Nost_Vost_EXC_RPRT_AUDT
(stmt_line_sk
,unique_id
,rprt_name
,acct_no
,acct_name
,acct_cncy
,acct_baln_pool
,date_tag_created
,value_date
,amt
,txn_type
,internal_ref
,external_ref
,description
,swf_file_ref
,mtch_type
,team_resp
,investigation_stus
,tran_catg
,is_reviewed
,othr_cmmt
,busn_days
,ifti_rprt
,subm_date
,subm_file_name
,subm_rcpt_no
,imt_mtch_grup_id
,imt_refn_1
,imt_refn_2
,imt_refn_3
,pms_ref_no
,pms_dbt_name
,pms_sbk_ref_no
,wss_srce
,wss_portfolio
,wss_audt_note
,wss_net_info
,wss_area
,itc_case_no
,wss_extl_deal_numb
,action_date
,EXC_OFCR
,EXC_OFCR_DTTM
,VERF_OFCR_CMMT
,VERF_OFCR
,VERF_OFCR_DTTM
,QA_OFCR_CMMT
,QA_OFCR
,QA_OFCR_DTTM
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(n.stmt_line_sk
,n.unique_id
,n.rprt_name
,n.acct_no
,n.acct_name
,n.acct_cncy
,n.acct_baln_pool
,n.date_tag_created
,n.value_date
,n.amt
,n.txn_type
,n.internal_ref
,n.external_ref
,n.description
,n.swf_file_ref
,n.mtch_type
,n.team_resp
,n.investigation_stus
,n.tran_catg
,n.is_reviewed
,n.othr_cmmt
,n.busn_days
,n.ifti_rprt
,n.subm_date
,n.subm_file_name
,n.subm_rcpt_no
,n.imt_mtch_grup_id
,n.imt_refn_1
,n.imt_refn_2
,n.imt_refn_3
,n.pms_ref_no
,n.pms_dbt_name
,n.pms_sbk_ref_no
,n.wss_srce
,n.wss_portfolio
,n.wss_audt_note
,n.wss_net_info
,n.wss_area
,n.itc_case_no
,n.wss_extl_deal_numb
,n.action_date
,n.EXC_OFCR
,n.EXC_OFCR_DTTM
,n.VERF_OFCR_CMMT
,n.VERF_OFCR
,n.VERF_OFCR_DTTM
,n.QA_OFCR_CMMT
,n.QA_OFCR
,n.QA_OFCR_DTTM
,USER
,Current_Timestamp(6)
,'DELETE' 
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_SEND_CORR
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
sendersCorrespondentCode1,
sendersCorrespondentName1,
sendersCorrespondentCity1,
sendersCorrespondentCountry1,
sendersCorrespondentCode2,
sendersCorrespondentName2,
sendersCorrespondentCity2,
sendersCorrespondentCountry2,
sendersCorrespondentCode3,
sendersCorrespondentName3,
sendersCorrespondentCity3,
sendersCorrespondentCountry3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_SEND_CORR',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.sendersCorrespondentCode1,
n.sendersCorrespondentName1,
n.sendersCorrespondentCity1,
n.sendersCorrespondentCountry1,
n.sendersCorrespondentCode2,
n.sendersCorrespondentName2,
n.sendersCorrespondentCity2,
n.sendersCorrespondentCountry2,
n.sendersCorrespondentCode3,
n.sendersCorrespondentName3,
n.sendersCorrespondentCity3,
n.sendersCorrespondentCountry3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg_Hist
  (
     DataBaseName
    ,TableName
    ,Version
    ,CreatorName
    ,CreateTimeStamp
    ,LastAlterName
    ,LastAlterTimeStamp
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DataBaseName
    ,n.TableName
    ,n.Version
    ,n.CreatorName
    ,n.CreateTimeStamp
    ,n.LastAlterName
    ,n.LastAlterTimeStamp
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_Hist
  (
     Cand_Obj_Stus_Type_Cd
    ,Cand_Obj_Stus_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Stus_Type_Cd
    ,n.Cand_Obj_Stus_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE_TRIG_U </sc-trigger> */


--update
--DROP TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NFT_1.IFTI_XML_REVW_BASE
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
filename,
txnRefNo ,
interceptFlag ,
isSwiftOrStructured,
swiftMsg ,
transferDate,
direction,
currency ,
currencyAmount,
valueDate ,    
RPRT_STUS ,
REVW_MODF_BY,
REVW_DTTM ,
SUPV_MODF_BY,
SUPV_DTTM ,
CRET_BY,
CRET_DTTM ,
INDA_DRFT ,
INDA_REDY,
INDA_APRD,
INDA_RWRK,
INDA_NOT_RPRT,
EXC_REAS ,
ENUS_CMMT ,
SDS_MSG_ID ,
SDS_MSG_SEQ,
base_transferDate,
base_direction,
base_currency ,
base_currencyAmount,
base_valueDate , 
INDA_PAYR,
INDA_PAYE,
INDA_ORDR_INST,
INDA_SEND_INST,
INDA_SEND_CORR,
INDA_BENE_INST,
INDA_RECE_INST,
INDA_RECE_CORR,
INDA_ADDN_DETL,
LINK_KEY       
)
VALUES
(
'IFTI_XML_REVW_BASE',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.filename,
n.txnRefNo ,
n.interceptFlag ,
n.isSwiftOrStructured,
n.swiftMsg ,
n.transferDate,
n.direction,
n.currency ,
n.currencyAmount,
n.valueDate ,    
n.RPRT_STUS ,
n.REVW_MODF_BY,
n.REVW_DTTM ,
n.SUPV_MODF_BY,
n.SUPV_DTTM ,
n.CRET_BY,
n.CRET_DTTM ,
n.INDA_DRFT ,
n.INDA_REDY,
n.INDA_APRD,
n.INDA_RWRK,
n.INDA_NOT_RPRT,
n.EXC_REAS ,
n.ENUS_CMMT ,
n.SDS_MSG_ID ,
n.SDS_MSG_SEQ,
n.base_transferDate,
n.base_direction,
n.base_currency ,
n.base_currencyAmount,
n.base_valueDate , 
n.INDA_PAYR,
n.INDA_PAYE,
n.INDA_ORDR_INST,
n.INDA_SEND_INST,
n.INDA_SEND_CORR,
n.INDA_BENE_INST,
n.INDA_RECE_INST,
n.INDA_RECE_CORR,
n.INDA_ADDN_DETL,
n.LINK_KEY
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_RECE_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
receivingInstnCode,
receivingInstnName,
receivingInstnCity,
receivingInstnCountry,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_RECE_INST',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.receivingInstnCode,
n.receivingInstnName,
n.receivingInstnCity,
n.receivingInstnCountry,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Job_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Job_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Job
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Job_Hist
  (
     Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj_Hist
  (
     Job_Id
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Db_Nm
    ,n.Obj_Nm
    ,n.Obj_Type
    ,n.Copy_Stat
    ,n.Compare_DDL
    ,n.Where_Clause
    ,n.Key_Column
    ,n.Query_Band_Str
    ,n.Job_Desc
    ,n.Staging_Database_Nm
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.MT202_EXC_RPRT_ACCS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.MT202_EXC_RPRT_AUDT
(
REPORT                        
,SWIFT_MESSAGE_TEXT            
,UNIQUE_IDENTIFIER             
,MSG_ID                        
,MSG_SEQ                       
,MSG_TYPE                      
,DB_DATE                       
,SOURCE                        
,BRAND                         
,CBA_BIC                       
,EXTERNAL_PARTY_BIC            
,INP_OUT_IDENTIFIER            
,AMOUNT                        
,CURRENCY                      
,VAL_DATE                      
,REMITTER                      
,BENEFICIARY                   
,FIELD_20                      
,FIELD_21                      
,CREATED_DATE                  
,CDT_ID                        
,CDT_NAME                      
,DBT_ID                        
,DBT_NAME                      
,TAG_52                        
,TAG_58                        
,TAG_72                        
,SWF_FILE_REF                  
,TRN_PROC_PMS                  
,BUSN_DAYS                     
,IS_REVIEWED                   
,TEAM_RESP                     
,INVESTIGATION_STUS            
,TRAN_CATG                     
,PRODUCT                       
,OTHR_CMMT                     
,VERF_BY_IFTI_MNGR             
,IFTI_RPRT                     
,SUBM_FILE_NAME                
,SUBM_DATE                     
,SUBM_RCPT_NO                  
,INSERTION_DATE                
,EXC_OFCR                      
,EXC_OFCR_DTTM                 
,VERF_OFCR_CMMT                
,VERF_OFCR                     
,VERF_OFCR_DTTM                
,QA_OFCR_CMMT                  
,QA_OFCR                       
,QA_OFCR_DTTM                  
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(
n.REPORT                        
,n.SWIFT_MESSAGE_TEXT            
,n.UNIQUE_IDENTIFIER             
,n.MSG_ID                        
,n.MSG_SEQ                       
,n.MSG_TYPE                      
,n.DB_DATE                       
,n.SOURCE                        
,n.BRAND                         
,n.CBA_BIC                       
,n.EXTERNAL_PARTY_BIC            
,n.INP_OUT_IDENTIFIER            
,n.AMOUNT                        
,n.CURRENCY                      
,n.VAL_DATE                      
,n.REMITTER                      
,n.BENEFICIARY                   
,n.FIELD_20                      
,n.FIELD_21                      
,n.CREATED_DATE                  
,n.CDT_ID                        
,n.CDT_NAME                      
,n.DBT_ID                        
,n.DBT_NAME                      
,n.TAG_52                        
,n.TAG_58                        
,n.TAG_72                        
,n.SWF_FILE_REF                  
,n.TRN_PROC_PMS                  
,n.BUSN_DAYS                     
,n.IS_REVIEWED                   
,n.TEAM_RESP                     
,n.INVESTIGATION_STUS            
,n.TRAN_CATG                     
,n.PRODUCT                       
,n.OTHR_CMMT                     
,n.VERF_BY_IFTI_MNGR             
,n.IFTI_RPRT                     
,n.SUBM_FILE_NAME                
,n.SUBM_DATE                     
,n.SUBM_RCPT_NO                  
,n.INSERTION_DATE                
,n.EXC_OFCR                      
,n.EXC_OFCR_DTTM                 
,n.VERF_OFCR_CMMT                
,n.VERF_OFCR                     
,n.VERF_OFCR_DTTM                
,n.QA_OFCR_CMMT                  
,n.QA_OFCR                       
,n.QA_OFCR_DTTM                  
,USER
,Current_Timestamp(6)
,'INSERT' 
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_ADMT_TRIG_U </sc-trigger> */
Replace TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_ADMT_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.IFTI_XML_ADMT
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_ADMT_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
FILENAME,
RPRT_CNT,
IFTI_XML,
FILE_COUR,
CRET_BY,
CRET_DTTM,
FILE_RNAM_BY,
FILE_RNAM_DTTM,
APRV_BY,
APRV_DTTM,
RCPT_N,
AUSTC_REFN,
SUBM_DT,
ENUS_CMMT,
REVER_OFCR_CMMT,
REVER_OFCR,
REVER_OFCR_DTTM,
VERF_OFCR_CMMT,
VERF_OFCR,
VERF_OFCR_DTTM,
QA_OFCR_CMMT,
QA_OFCR,
QA_OFCR_DTTM,
IS_REVIEWED
)
VALUES
(
'IFTI_XML_ADMT',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.FILENAME,
n.RPRT_CNT,
n.IFTI_XML,
n.FILE_COUR,
n.CRET_BY,
n.CRET_DTTM,
n.FILE_RNAM_BY,
n.FILE_RNAM_DTTM,
n.APRV_BY,
n.APRV_DTTM,
n.RCPT_N,
n.AUSTC_REFN,
n.SUBM_DT,
n.ENUS_CMMT,
n.REVER_OFCR_CMMT,
n.REVER_OFCR,
n.REVER_OFCR_DTTM,
n.VERF_OFCR_CMMT,
n.VERF_OFCR,
n.VERF_OFCR_DTTM,
n.QA_OFCR_CMMT,
n.QA_OFCR,
n.QA_OFCR_DTTM,
n.IS_REVIEWED
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Id_Step_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Id_Step_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDM_Job_Id_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Id_Step_Hist
  (
     Job_Id
    ,Step_No
    ,Step_Start_Ts
    ,Step_End_Ts
    ,Step_Stus_Cd
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Final_Step_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Step_No
    ,n.Step_Start_Ts
    ,n.Step_End_Ts
    ,n.Step_Stus_Cd
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Executable_User
    ,n.Executable_Platform_Name
    ,n.Executable_BT_ET_Flag
    ,n.Enabled_Flag
    ,n.Final_Step_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Step_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Step_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDM_Job_Step
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Step_Hist
  (
     Job_Cd
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.Step_No
    ,o.Step_Desc
    ,o.Step_Type
    ,o.Executable_Nm
    ,o.Executable_Db
    ,o.Executable_Type
    ,o.Executable_Param
    ,o.Executable_User
    ,o.Executable_Platform_Name
    ,o.Executable_BT_ET_Flag
    ,o.Enabled_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> U_D_DSV_001_NFT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_I </sc-trigger> */
-- cba  debit Insert Trigger
--replace trigger U_D_DSV_001_NFT_1.EXC_RPRT_ACCS_CBA_CR_TRIG_I
REPLACE TRIGGER U_D_DSV_001_NFT_1.NOST_CBA_CR_EXC_RPRT_ACCS_TRIG_I
AFTER INSERT ON U_D_DSV_001_NFT_1.NOST_CBA_CR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NFT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_CR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'INSERT'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> U_D_DSV_001_NIT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_QA_CMMT </sc-trigger> */
Create TRIGGER U_D_DSV_001_NIT_1.TRI_NOST_VOST_EXC_RPRT_ACCS_UPDATE_QA_CMMT 
AFTER UPDATE OF QA_OFCR_CMMT ON U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCESS
    REFERENCING OLD AS OldRow NEW as NewRow
    FOR EACH ROW
        INSERT U_D_DSV_001_NIT_1.NOST_VOST_EXC_RPRT_ACCS_CMMT_HIS
            (
            UNIQUE_ID,
            QA_OFCR,
            QA_OFCR_DTTM,
            QA_OFCR_CMMT
            )
        VALUES
            (
            NewRow.UNIQUE_ID,

   COALESCE (NewRow.QA_OFCR, NULL, CURRENT_USER),
   COALESCE (NewRow.QA_OFCR_DTTM, NULL, CURRENT_TIMESTAMP),
   NewRow.QA_OFCR_CMMT
            );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Cand_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Hist
  (
     Cand_Id
    ,Trtmnt_Cd
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,TDM_Obj_Type
    ,TDM_Copy_Stat
    ,TDM_Compare_DDL
    ,TDM_Where_Clause
    ,TDM_Key_Column
    ,TDM_Query_Band_Str
    ,TDM_Job_Desc
    ,TDM_Staging_Database_Nm
    ,TDM_Job_Cd
    ,Force_Flag
    ,Tbl_Size
    ,Job_Cd
    ,UOWId
    ,num_Tbl
    ,running_flag
    ,failed_flag
    ,exists_flag
    ,Tbl_flag
    ,changed_flag
    ,Rule_No
    ,Limit_Desc
    ,dedupe_cd
    ,Enabled_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Trtmnt_Cd
    ,n.TDM_Database_Nm
    ,n.TDM_Obj_Nm
    ,n.TDM_Obj_Type
    ,n.TDM_Copy_Stat
    ,n.TDM_Compare_DDL
    ,n.TDM_Where_Clause
    ,n.TDM_Key_Column
    ,n.TDM_Query_Band_Str
    ,n.TDM_Job_Desc
    ,n.TDM_Staging_Database_Nm
    ,n.TDM_Job_Cd
    ,n.Force_Flag
    ,n.Tbl_Size
    ,n.Job_Cd
    ,n.UOWId
    ,n.num_Tbl
    ,n.running_flag
    ,n.failed_flag
    ,n.exists_flag
    ,n.Tbl_flag
    ,n.changed_flag
    ,n.Rule_No
    ,n.Limit_Desc
    ,n.dedupe_cd
    ,n.Enabled_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Limit_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Limit_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Limit
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Limit_Hist
  (
     Limit_No
    ,Candidates_Per_Iteration
    ,Obj_Per_Iteration
    ,MB_Per_Iteration
    ,Candidates_Per_Hour
    ,Obj_Per_Hour
    ,MB_Per_Hour
    ,Num_Running_DM_Jobs
    ,Num_Running_Tbl
    ,Num_Staged_Jobs
    ,Num_Staged_Tbl
    ,Num_Total_Jobs
    ,Num_Total_Tbl
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Limit_No
    ,n.Candidates_Per_Iteration
    ,n.Obj_Per_Iteration
    ,n.MB_Per_Iteration
    ,n.Candidates_Per_Hour
    ,n.Obj_Per_Hour
    ,n.MB_Per_Hour
    ,n.Num_Running_DM_Jobs
    ,n.Num_Running_Tbl
    ,n.Num_Staged_Jobs
    ,n.Num_Staged_Tbl
    ,n.Num_Total_Jobs
    ,n.Num_Total_Tbl
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_U </sc-trigger> */


--update
-- DROP TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_U;
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_ADDN_DETL
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
additionalDetailsOfPayment,
additionalOtherDetails,
additionalInstnCode,
additionalInstnName,
additionalInstnCity,
additionalInstnCountry,
additionalInstnAccountBSB,
additionalInstnAccountNumber,
senderToReceiverInfo1,
senderToReceiverInfo2,
senderToReceiverInfo3,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_ADDN_DETL',
USER,
CURRENT_TIMESTAMP(6),
'UPDATE',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.additionalDetailsOfPayment,
n.additionalOtherDetails,
n.additionalInstnCode,
n.additionalInstnName,
n.additionalInstnCity,
n.additionalInstnCountry,
n.additionalInstnAccountBSB,
n.additionalInstnAccountNumber,
n.senderToReceiverInfo1,
n.senderToReceiverInfo2,
n.senderToReceiverInfo3,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_NM
    ,TDM_Key_Column
    ,Pros_Isac_DB_NM
    ,ODS_Insert_Col_Flag
    ,ODS_Update_Col_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Nm
    ,n.Db_Nm
    ,n.Tbl_NM
    ,n.TDM_Key_Column
    ,n.Pros_Isac_DB_NM
    ,n.ODS_Insert_Col_Flag
    ,n.ODS_Update_Col_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Sys_Name
    ,Update_Tally
    ,Elapsed_Secs
    ,Key_Type1
    ,Key_Type2
    ,Key_Type3
    ,Key_Value
    ,Key_Where_Flag
    ,Key_Where_Val
    ,Key_Where_Cond
    ,Key_Partition_Flag
    ,Key_Partition_Val
    ,Key_Partition_Col
    ,Prev_Create_Date
    ,Prev_Create_Ts
    ,Is_Current_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Sys_Name
    ,n.Update_Tally
    ,n.Elapsed_Secs
    ,n.Key_Type1
    ,n.Key_Type2
    ,n.Key_Type3
    ,n.Key_Value
    ,n.Key_Where_Flag
    ,n.Key_Where_Val
    ,n.Key_Where_Cond
    ,n.Key_Partition_Flag
    ,n.Key_Partition_Val
    ,n.Key_Partition_Col
    ,n.Prev_Create_Date
    ,n.Prev_Create_Ts
    ,n.Is_Current_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> U_D_DSV_001_NIT_1.IFTI_XML_REVW_BENE_INST_TRIG_I </sc-trigger> */
REPLACE TRIGGER U_D_DSV_001_NIT_1.IFTI_XML_REVW_BENE_INST_TRIG_I
AFTER INSERT ON U_D_DSV_001_NIT_1.IFTI_XML_REVW_BENE_INST
REFERENCING NEW ROW AS n
FOR EACH ROW
--WHEN (UID IS NOT NULL)
INSERT INTO U_D_DSV_001_NIT_1.IFTI_XML_AUDT
(
AUDT_TABL_M ,
MODF_USER_M ,
MODF_DTTS ,
OPER_TYPE ,     
UID,
txnRefNo,
isSwiftOrStructured,
transferDate,
direction,
currency,
currencyAmount,
valueDate,
beneficiaryInstnCode,
beneficiaryInstnName,
beneficiaryInstnCity,
beneficiaryInstnCountry,
beneficiaryInstnBranchBSB,
beneficiaryInstnBranchId,
beneficiaryInstnBranchName,
RPRT_STUS,
REVW_MODF_BY,
REVW_DTTM,
LINK_KEY
)
VALUES
(
'IFTI_XML_REVW_BENE_INST',
USER,
CURRENT_TIMESTAMP(6),
'INSERT',     
n.UID,
n.txnRefNo,
n.isSwiftOrStructured,
n.transferDate,
n.direction,
n.currency,
n.currencyAmount,
n.valueDate,
n.beneficiaryInstnCode,
n.beneficiaryInstnName,
n.beneficiaryInstnCity,
n.beneficiaryInstnCountry,
n.beneficiaryInstnBranchBSB,
n.beneficiaryInstnBranchId,
n.beneficiaryInstnBranchName,
n.RPRT_STUS,
n.REVW_MODF_BY,
n.REVW_DTTM,
n.LINK_KEY
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Rule_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Rule_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Cand_Rule
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Rule_Hist
  (
     Rule_No
    ,Cand_Source
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,Priority
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Rule_No
    ,n.Cand_Source
    ,n.TDM_Database_Nm
    ,n.TDM_Obj_Nm
    ,n.Priority
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Step_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Step_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Step_Hist
  (
     Cand_Id
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Exec_Param_KVP
    ,Executable_User
    ,Executable_TDPID
    ,Executable_BT_ET_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Step_No
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Exec_Param_KVP
    ,n.Executable_User
    ,n.Executable_TDPID
    ,n.Executable_BT_ET_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> U_D_DSV_001_NIT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */



--replace trigger U_D_DSV_001_NIT_1.EXC_RPRT_ACCS_CBA_DR_TRIG_U
REPLACE TRIGGER U_D_DSV_001_NIT_1.NOST_CBA_DR_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.NOST_CBA_DR_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.NOST_EXC_RPRT_AUDT
(
 AUDT_TABL_M                   
,MODF_USER_M                   
,MODF_DTTS                     
,OPER_TYPE                     
,unique_identifier             
,match_type                    
,Nostro_acct                   
,CURR_NOSTRO                   
,Group_area                    
,Acct_Name                     
,external_party_bic            
,value_date                    
,AMOUNT                        
,Message_Type                  
,message_type_desc             
,tag_61_stmt_line              
,internal_ref                  
,external_ref                  
,description                   
,department                    
,Investigation_Flag            
,WS_flag                       
,deal_number                   
,cust_number                   
,broker_name                   
,product_cat                   
,Department_Responsible        
,Reason                        
,Other_Comment                 
,Escalate                      
,IFTI_Reportable               
,Payor_Bank                    
,Payor_Details                 
,Payee_Bank                    
,Payee_Details                 
,Investigation_Status          
,MatchID                       
,deal_num_list                 
,Beneficiary                   
,Remark                        
,Settle                        
,Category_Correct              
,Updated                       
,msg_id                        
,MSG_TAG_SEQ                   
,CBA_BIC                       
,currency_tag62                
,date_tag_created              
,amount_matched                
,Buckets_f                     
,Bucket                        
,WS_GDW_source                 
,cust_code                     
,cust_type                     
,ext_ref_nr                    
,portfolio_no                  
,Acc                           
,Additional_Field              
,Pay_Meth                      
,swift_50                      
,Remittance_info               
,Enough_Data_For_IFTI          
,swf_File_Ref                  
,compliance_exclusion          
)
VALUES
(
 'NOST_CBA_DR_EXC_RPRT_ACCESS'
,USER
,CURRENT_TIMESTAMP(6)
,'UPDATE'  
,n.unique_identifier             
,n.match_type                    
,n.Nostro_acct                   
,n.CURR_NOSTRO                   
,n.Group_area                    
,n.Acct_Name                     
,n.external_party_bic            
,n.value_date                    
,n.AMOUNT                        
,n.Message_Type                  
,n.message_type_desc             
,n.tag_61_stmt_line              
,n.internal_ref                  
,n.external_ref                  
,n.description                   
,n.department                    
,n.Investigation_Flag            
,n.WS_flag                       
,n.deal_number                   
,n.cust_number                   
,n.broker_name                   
,n.product_cat                   
,n.Department_Responsible        
,n.Reason                        
,n.Other_Comment                 
,n.Escalate                      
,n.IFTI_Reportable               
,n.Payor_Bank                    
,n.Payor_Details                 
,n.Payee_Bank                    
,n.Payee_Details                 
,n.Investigation_Status          
,n.MatchID                       
,n.deal_num_list                 
,n.Beneficiary                   
,n.Remark                        
,n.Settle                        
,n.Category_Correct              
,n.Updated                       
,n.msg_id                        
,n.MSG_TAG_SEQ                   
,n.CBA_BIC                       
,n.currency_tag62                
,n.date_tag_created              
,n.amount_matched                
,n.Buckets_f                     
,n.Bucket                        
,n.WS_GDW_source                 
,n.cust_code                     
,n.cust_type                     
,n.ext_ref_nr                    
,n.portfolio_no                  
,n.Acc                           
,n.Additional_Field              
,n.Pay_Meth                      
,n.swift_50                      
,n.Remittance_info               
,n.Enough_Data_For_IFTI          
,n.swf_File_Ref                  
,n.compliance_exclusion
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Alrt_Dest_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Alrt_Dest_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Alrt_Dest
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Alrt_Dest_Hist
  (
     Destination
    ,DestinationDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Destination
    ,o.DestinationDesc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_Hist
  (
     Stream_Name
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Name
    ,n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_U </sc-trigger> */

Replace TRIGGER U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCS_TRIG_U
AFTER UPDATE ON U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_ACCESS
REFERENCING NEW ROW AS n
FOR EACH ROW
INSERT INTO U_D_DSV_001_NIT_1.Nost_Vost_EXC_RPRT_AUDT
(stmt_line_sk
,unique_id
,rprt_name
,acct_no
,acct_name
,acct_cncy
,acct_baln_pool
,date_tag_created
,value_date
,amt
,txn_type
,internal_ref
,external_ref
,description
,swf_file_ref
,mtch_type
,team_resp
,investigation_stus
,tran_catg
,is_reviewed
,othr_cmmt
,busn_days
,ifti_rprt
,subm_date
,subm_file_name
,subm_rcpt_no
,imt_mtch_grup_id
,imt_refn_1
,imt_refn_2
,imt_refn_3
,pms_ref_no
,pms_dbt_name
,pms_sbk_ref_no
,wss_srce
,wss_portfolio
,wss_audt_note
,wss_net_info
,wss_area
,itc_case_no
,wss_extl_deal_numb
,action_date
,EXC_OFCR
,EXC_OFCR_DTTM
,VERF_OFCR_CMMT
,VERF_OFCR
,VERF_OFCR_DTTM
,QA_OFCR_CMMT
,QA_OFCR
,QA_OFCR_DTTM
,MODF_BY                       
,MODF_DTTS                     
,OPER_TYPE                     
)
VALUES
(n.stmt_line_sk
,n.unique_id
,n.rprt_name
,n.acct_no
,n.acct_name
,n.acct_cncy
,n.acct_baln_pool
,n.date_tag_created
,n.value_date
,n.amt
,n.txn_type
,n.internal_ref
,n.external_ref
,n.description
,n.swf_file_ref
,n.mtch_type
,n.team_resp
,n.investigation_stus
,n.tran_catg
,n.is_reviewed
,n.othr_cmmt
,n.busn_days
,n.ifti_rprt
,n.subm_date
,n.subm_file_name
,n.subm_rcpt_no
,n.imt_mtch_grup_id
,n.imt_refn_1
,n.imt_refn_2
,n.imt_refn_3
,n.pms_ref_no
,n.pms_dbt_name
,n.pms_sbk_ref_no
,n.wss_srce
,n.wss_portfolio
,n.wss_audt_note
,n.wss_net_info
,n.wss_area
,n.itc_case_no
,n.wss_extl_deal_numb
,n.action_date
,n.EXC_OFCR
,n.EXC_OFCR_DTTM
,n.VERF_OFCR_CMMT
,n.VERF_OFCR
,n.VERF_OFCR_DTTM
,n.QA_OFCR_CMMT
,n.QA_OFCR
,n.QA_OFCR_DTTM
,USER
,Current_Timestamp(6)
,'UPDATE' 
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers_Hist
  (
     ParameterValue
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.ParameterValue
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Hist
  (
     Cand_Id
    ,Trtmnt_Cd
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,TDM_Obj_Type
    ,TDM_Copy_Stat
    ,TDM_Compare_DDL
    ,TDM_Where_Clause
    ,TDM_Key_Column
    ,TDM_Query_Band_Str
    ,TDM_Job_Desc
    ,TDM_Staging_Database_Nm
    ,TDM_Job_Cd
    ,Force_Flag
    ,Tbl_Size
    ,Job_Cd
    ,UOWId
    ,num_Tbl
    ,running_flag
    ,failed_flag
    ,exists_flag
    ,Tbl_flag
    ,changed_flag
    ,Rule_No
    ,Limit_Desc
    ,dedupe_cd
    ,Enabled_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Trtmnt_Cd
    ,n.TDM_Database_Nm
    ,n.TDM_Obj_Nm
    ,n.TDM_Obj_Type
    ,n.TDM_Copy_Stat
    ,n.TDM_Compare_DDL
    ,n.TDM_Where_Clause
    ,n.TDM_Key_Column
    ,n.TDM_Query_Band_Str
    ,n.TDM_Job_Desc
    ,n.TDM_Staging_Database_Nm
    ,n.TDM_Job_Cd
    ,n.Force_Flag
    ,n.Tbl_Size
    ,n.Job_Cd
    ,n.UOWId
    ,n.num_Tbl
    ,n.running_flag
    ,n.failed_flag
    ,n.exists_flag
    ,n.Tbl_flag
    ,n.changed_flag
    ,n.Rule_No
    ,n.Limit_Desc
    ,n.dedupe_cd
    ,n.Enabled_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_Hist
  (
     ExtJob_Name
    ,ExtJob_RunNo
    ,ExtJob_Type
    ,ExtJob_Stat
    ,ExtJob_Beg_Ts
    ,ExtJob_End_Ts
    ,ExtJob_Pri
    ,Prev_ExtJob_End_Ts
    ,ExtJob_ErrNo
    ,ExtJob_ErrMsg
    ,Prg_Stat_Cd
    ,Prg_Stat_Desc
    ,ExtSys_Name
    ,ExtSys_Mode
    ,ExtSys_Capture_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.ExtJob_Name
    ,n.ExtJob_RunNo
    ,n.ExtJob_Type
    ,n.ExtJob_Stat
    ,n.ExtJob_Beg_Ts
    ,n.ExtJob_End_Ts
    ,n.ExtJob_Pri
    ,n.Prev_ExtJob_End_Ts
    ,n.ExtJob_ErrNo
    ,n.ExtJob_ErrMsg
    ,n.Prg_Stat_Cd
    ,n.Prg_Stat_Desc
    ,n.ExtSys_Name
    ,n.ExtSys_Mode
    ,n.ExtSys_Capture_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_Hist
  (
     Obj
    ,Is_Current_Flag
    ,Is_Enabled_Flag
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Key_Type
    ,Key_Name
    ,Key_Sql
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Obj
    ,n.Is_Current_Flag
    ,n.Is_Enabled_Flag
    ,n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Key_Type
    ,n.Key_Name
    ,n.Key_Sql
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Limit_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Limit_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Cand_Limit
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Limit_Hist
  (
     Limit_No
    ,Candidates_Per_Iteration
    ,Obj_Per_Iteration
    ,MB_Per_Iteration
    ,Candidates_Per_Hour
    ,Obj_Per_Hour
    ,MB_Per_Hour
    ,Num_Running_DM_Jobs
    ,Num_Running_Tbl
    ,Num_Staged_Jobs
    ,Num_Staged_Tbl
    ,Num_Total_Jobs
    ,Num_Total_Tbl
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Limit_No
    ,o.Candidates_Per_Iteration
    ,o.Obj_Per_Iteration
    ,o.MB_Per_Iteration
    ,o.Candidates_Per_Hour
    ,o.Obj_Per_Hour
    ,o.MB_Per_Hour
    ,o.Num_Running_DM_Jobs
    ,o.Num_Running_Tbl
    ,o.Num_Staged_Jobs
    ,o.Num_Staged_Tbl
    ,o.Num_Total_Jobs
    ,o.Num_Total_Tbl
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Step_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Step_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Cand_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Step_Hist
  (
     Cand_Id
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Exec_Param_KVP
    ,Executable_User
    ,Executable_TDPID
    ,Executable_BT_ET_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Step_No
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Exec_Param_KVP
    ,n.Executable_User
    ,n.Executable_TDPID
    ,n.Executable_BT_ET_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Rule_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Rule_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Cand_Rule
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Rule_Hist
  (
     Rule_No
    ,Cand_Source
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,Priority
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Rule_No
    ,n.Cand_Source
    ,n.TDM_Database_Nm
    ,n.TDM_Obj_Nm
    ,n.Priority
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Rule_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Rule_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Rule
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Rule_Hist
  (
     Rule_No
    ,Cand_Source
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,Priority
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Rule_No
    ,n.Cand_Source
    ,n.TDM_Database_Nm
    ,n.TDM_Obj_Nm
    ,n.Priority
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Next_Job_Id_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Next_Job_Id_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDM_Next_Job_Id
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Next_Job_Id_Hist
  (
     Fixed_Value
    ,Next_Job_Id
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Fixed_Value
    ,n.Next_Job_Id
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_Hist
  (
     Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Prog_Vers
    ,Proc_Id
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Enabled_Flag
    ,Prog_Beg_Ts
    ,Prog_End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Prog_Name
    ,n.Prog_Mode
    ,n.Prog_User
    ,n.Prog_Host
    ,n.Prog_Vers
    ,n.Proc_Id
    ,n.Prog_Stat_Cd
    ,n.Prog_Stat_Desc
    ,n.Enabled_Flag
    ,n.Prog_Beg_Ts
    ,n.Prog_End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Id_Step_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Id_Step_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDM_Job_Id_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Id_Step_Hist
  (
     Job_Id
    ,Step_No
    ,Step_Start_Ts
    ,Step_End_Ts
    ,Step_Stus_Cd
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Final_Step_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Step_No
    ,n.Step_Start_Ts
    ,n.Step_End_Ts
    ,n.Step_Stus_Cd
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Executable_User
    ,n.Executable_Platform_Name
    ,n.Executable_BT_ET_Flag
    ,n.Enabled_Flag
    ,n.Final_Step_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Rule_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Rule_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Cand_Rule
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Rule_Hist
  (
     Rule_No
    ,Cand_Source
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,Priority
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Rule_No
    ,n.Cand_Source
    ,n.TDM_Database_Nm
    ,n.TDM_Obj_Nm
    ,n.Priority
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_Hist
  (
     Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Nm
    ,n.Tbl_Nm
    ,n.User_ID
    ,n.Max_Pros_Key_I
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_Hist
  (
     DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DB_Name
    ,n.Obj_Name
    ,n.Col_1_Name
    ,n.Col_2_Name
    ,n.Join_Col_1_Name
    ,n.Join_Col_2_Name
    ,n.Predicate_Clause_Text
    ,n.Join_Type
    ,n.Rltd_DB_Name
    ,n.Rltd_Obj_Name
    ,n.Rltd_Col_1_Name
    ,n.Rltd_Col_2_Name
    ,n.Rltd_Join_Col_1_Name
    ,n.Rltd_Join_Col_2_Name
    ,n.Rltd_Predicate_Clause_Text
    ,n.Is_Enabled
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_Pros_Stus_Hist
  (
     LogDate
    ,NumRows
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.LogDate
    ,o.NumRows
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_Hist
  (
     Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Nm
    ,n.Tbl_Nm
    ,n.User_ID
    ,n.Max_Pros_Key_I
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Data_Rtntn_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Data_Rtntn_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Data_Rtntn
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Data_Rtntn_Hist
  (
     DB_Name
    ,Tbl_Name
    ,Col_Name
    ,Ret_Period
    ,Impacted_Obj_DB_Name
    ,Impacted_Obj_Name
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.DB_Name
    ,o.Tbl_Name
    ,o.Col_Name
    ,o.Ret_Period
    ,o.Impacted_Obj_DB_Name
    ,o.Impacted_Obj_Name
    ,o.Is_Enabled
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest_Hist
  (
     Destination
    ,DestinationDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Destination
    ,n.DestinationDesc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_Hist
  (
     Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Prog_Vers
    ,Proc_Id
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Enabled_Flag
    ,Prog_Beg_Ts
    ,Prog_End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Prog_Name
    ,n.Prog_Mode
    ,n.Prog_User
    ,n.Prog_Host
    ,n.Prog_Vers
    ,n.Proc_Id
    ,n.Prog_Stat_Cd
    ,n.Prog_Stat_Desc
    ,n.Enabled_Flag
    ,n.Prog_Beg_Ts
    ,n.Prog_End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Next_Job_Id_Hist
  (
     Fixed_Value
    ,Next_Job_Id
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Fixed_Value
    ,n.Next_Job_Id
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_SQLState_Class_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_SQLState_Class_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_SQLState_Class
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_SQLState_Class_Hist
  (
     ClassCode
    ,ClassDefinition
    ,ClassNote
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     o.ClassCode
    ,o.ClassDefinition
    ,o.ClassNote
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_TS
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Limit_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Limit_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Cand_Limit
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Limit_Hist
  (
     Limit_No
    ,Candidates_Per_Iteration
    ,Obj_Per_Iteration
    ,MB_Per_Iteration
    ,Candidates_Per_Hour
    ,Obj_Per_Hour
    ,MB_Per_Hour
    ,Num_Running_DM_Jobs
    ,Num_Running_Tbl
    ,Num_Staged_Jobs
    ,Num_Staged_Tbl
    ,Num_Total_Jobs
    ,Num_Total_Tbl
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Limit_No
    ,n.Candidates_Per_Iteration
    ,n.Obj_Per_Iteration
    ,n.MB_Per_Iteration
    ,n.Candidates_Per_Hour
    ,n.Obj_Per_Hour
    ,n.MB_Per_Hour
    ,n.Num_Running_DM_Jobs
    ,n.Num_Running_Tbl
    ,n.Num_Staged_Jobs
    ,n.Num_Staged_Tbl
    ,n.Num_Total_Jobs
    ,n.Num_Total_Tbl
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Step_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Step_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Cand_Step
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Step_Hist
  (
     Cand_Id
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Exec_Param_KVP
    ,Executable_User
    ,Executable_TDPID
    ,Executable_BT_ET_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Id
    ,o.Step_No
    ,o.Step_Desc
    ,o.Step_Type
    ,o.Executable_Nm
    ,o.Executable_Db
    ,o.Executable_Type
    ,o.Executable_Param
    ,o.Exec_Param_KVP
    ,o.Executable_User
    ,o.Executable_TDPID
    ,o.Executable_BT_ET_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Id_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Id_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDM_Job_Id
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Id_Hist
  (
     Job_Id
    ,Job_Stus_Cd
    ,Job_Start_Ts
    ,Job_End_Ts
    ,Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Job_Stus_Cd
    ,n.Job_Start_Ts
    ,n.Job_End_Ts
    ,n.Job_Cd
    ,n.UOW_Id
    ,n.Job_Priority
    ,n.Job_Type
    ,n.Job_Source
    ,n.Job_Description
    ,n.Source_TDPID
    ,n.Source_User
    ,n.Source_Passwd_Encrypted
    ,n.Source_Logon_Mechanism
    ,n.Source_Logon_Mechanism_Data
    ,n.Source_Userid_Pool
    ,n.Source_Account_Id
    ,n.Source_Sessions
    ,n.Target_TDPID
    ,n.Target_User
    ,n.Target_Passwd_Encrypted
    ,n.Target_Logon_Mechanism
    ,n.Target_Logon_Mechanism_Data
    ,n.Target_Userid_Pool
    ,n.Target_Account_Id
    ,n.Target_Sessions
    ,n.Data_Streams
    ,n.Response_Timeout
    ,n.Overwrite_Existing_Obj
    ,n.Force_Utility
    ,n.Log_Level
    ,n.Online_Archive
    ,n.Freeze_Job_Steps
    ,n.Max_Agents_Per_Task
    ,n.Additional_ARC_Parameters
    ,n.Query_Band_Str
    ,n.Sync_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Scope_Stream_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Scope_Stream_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Scope_Stream
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Scope_Stream_Hist
  (
     Stream_Name
    ,Stream_Type
    ,Job_Name
    ,Enabled_Flag
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Stream_Name
    ,o.Stream_Type
    ,o.Job_Name
    ,o.Enabled_Flag
    ,o.Allow_Cnsldtn_Flag
    ,o.Allow_Partial_Flag
    ,o.Start_Ts
    ,o.End_Ts
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Cand
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Hist
  (
     Cand_Id
    ,Description
    ,Trtmnt_Cd
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Priority
    ,Cand_Source
    ,Source_Id
    ,Stream_End_TS
    ,Enabled_Flag
    ,Num_Retries
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Id
    ,o.Description
    ,o.Trtmnt_Cd
    ,o.Allow_Cnsldtn_Flag
    ,o.Allow_Partial_Flag
    ,o.Priority
    ,o.Cand_Source
    ,o.Source_Id
    ,o.Stream_End_TS
    ,o.Enabled_Flag
    ,o.Num_Retries
    ,o.TDM_Job_Priority
    ,o.TDM_Job_Type
    ,o.TDM_Job_Source
    ,o.TDM_Job_Description
    ,o.TDM_Source_TDPID
    ,o.TDM_Source_User
    ,o.TDM_Source_Passwd_Encrypted
    ,o.TDM_Source_Logon_Mech
    ,o.TDM_Source_Logon_Mech_Data
    ,o.TDM_Source_Userid_Pool
    ,o.TDM_Source_Account_Id
    ,o.TDM_Source_Sessions
    ,o.TDM_Target_TDPID
    ,o.TDM_Target_User
    ,o.TDM_Target_Passwd_Encrypted
    ,o.TDM_Target_Logon_Mech
    ,o.TDM_Target_Logon_Mech_Data
    ,o.TDM_Target_Userid_Pool
    ,o.TDM_Target_Account_Id
    ,o.TDM_Target_Sessions
    ,o.TDM_Data_Streams
    ,o.TDM_Response_Timeout
    ,o.TDM_Overwrite_Existing_Obj
    ,o.TDM_Force_Utility
    ,o.TDM_Log_Level
    ,o.TDM_Online_Archive
    ,o.TDM_Freeze_Job_Steps
    ,o.TDM_Max_Agents_Per_Task
    ,o.TDM_Additional_ARC_Parameters
    ,o.TDM_Query_Band_Str
    ,o.TDM_Sync_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_Hist
  (
     DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DB_Name
    ,n.Obj_Name
    ,n.Col_1_Name
    ,n.Col_2_Name
    ,n.Join_Col_1_Name
    ,n.Join_Col_2_Name
    ,n.Predicate_Clause_Text
    ,n.Join_Type
    ,n.Rltd_DB_Name
    ,n.Rltd_Obj_Name
    ,n.Rltd_Col_1_Name
    ,n.Rltd_Col_2_Name
    ,n.Rltd_Join_Col_1_Name
    ,n.Rltd_Join_Col_2_Name
    ,n.Rltd_Predicate_Clause_Text
    ,n.Is_Enabled
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Cand
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Hist
  (
     Cand_Id
    ,Description
    ,Trtmnt_Cd
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Priority
    ,Cand_Source
    ,Source_Id
    ,Stream_End_TS
    ,Enabled_Flag
    ,Num_Retries
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Description
    ,n.Trtmnt_Cd
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Priority
    ,n.Cand_Source
    ,n.Source_Id
    ,n.Stream_End_TS
    ,n.Enabled_Flag
    ,n.Num_Retries
    ,n.TDM_Job_Priority
    ,n.TDM_Job_Type
    ,n.TDM_Job_Source
    ,n.TDM_Job_Description
    ,n.TDM_Source_TDPID
    ,n.TDM_Source_User
    ,n.TDM_Source_Passwd_Encrypted
    ,n.TDM_Source_Logon_Mech
    ,n.TDM_Source_Logon_Mech_Data
    ,n.TDM_Source_Userid_Pool
    ,n.TDM_Source_Account_Id
    ,n.TDM_Source_Sessions
    ,n.TDM_Target_TDPID
    ,n.TDM_Target_User
    ,n.TDM_Target_Passwd_Encrypted
    ,n.TDM_Target_Logon_Mech
    ,n.TDM_Target_Logon_Mech_Data
    ,n.TDM_Target_Userid_Pool
    ,n.TDM_Target_Account_Id
    ,n.TDM_Target_Sessions
    ,n.TDM_Data_Streams
    ,n.TDM_Response_Timeout
    ,n.TDM_Overwrite_Existing_Obj
    ,n.TDM_Force_Utility
    ,n.TDM_Log_Level
    ,n.TDM_Online_Archive
    ,n.TDM_Freeze_Job_Steps
    ,n.TDM_Max_Agents_Per_Task
    ,n.TDM_Additional_ARC_Parameters
    ,n.TDM_Query_Band_Str
    ,n.TDM_Sync_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg_Hist
  (
     DBName
    ,TBName
    ,ColName
    ,PPI_Status
    ,PPI_START_DATE
    ,PPI_END_DATE
    ,ADD_START_DATE
    ,ADD_END_DATE
    ,DROP_START_DATE
    ,DROP_END_DATE
    ,Ret_Period
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DBName
    ,n.TBName
    ,n.ColName
    ,n.PPI_Status
    ,n.PPI_START_DATE
    ,n.PPI_END_DATE
    ,n.ADD_START_DATE
    ,n.ADD_END_DATE
    ,n.DROP_START_DATE
    ,n.DROP_END_DATE
    ,n.Ret_Period
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Alrt_Action_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Alrt_Action_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Alrt_Action
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Alrt_Action_Hist
  (
     ActionCode
    ,ActionDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.ActionCode
    ,o.ActionDesc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_Hist
  (
     Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Db_Nm
    ,o.Tbl_Nm
    ,o.User_ID
    ,o.Max_Pros_Key_I
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log_Hist
  (
     Purge_Db
    ,Purge_Tbl
    ,Log_Date
    ,Start_Time
    ,Stop_Time
    ,Rows_Returned
    ,Status
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Purge_Db
    ,o.Purge_Tbl
    ,o.Log_Date
    ,o.Start_Time
    ,o.Stop_Time
    ,o.Rows_Returned
    ,o.Status
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Step_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Step_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDM_Job_Step
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Step_Hist
  (
     Job_Cd
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.Step_No
    ,o.Step_Desc
    ,o.Step_Type
    ,o.Executable_Nm
    ,o.Executable_Db
    ,o.Executable_Type
    ,o.Executable_Param
    ,o.Executable_User
    ,o.Executable_Platform_Name
    ,o.Executable_BT_ET_Flag
    ,o.Enabled_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds_Hist
  (
     SQLStateCd
    ,ClassCd
    ,SubClassCd
    ,TDErrorCd
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     n.SQLStateCd
    ,n.ClassCd
    ,n.SubClassCd
    ,n.TDErrorCd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_TS
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_TS
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD_Hist
  (
     DB_Vers
    ,Queryplan_Path
    ,Queryplan_Len
    ,Queryplan_XSD
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.DB_Vers
    ,o.Queryplan_Path
    ,o.Queryplan_Len
    ,o.Queryplan_XSD
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_Stat_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_Stat_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_JA_Stat
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_Stat_Hist
  (
     Submit_TS
    ,No_Cands
    ,No_Obj
    ,No_MB
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Submit_TS
    ,n.No_Cands
    ,n.No_Obj
    ,n.No_MB
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Cfg_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Cfg_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Cand_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Cfg_Hist
  (
    Cand_Source
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
    n.Cand_Source
    ,n.TDM_Job_Priority
    ,n.TDM_Job_Type
    ,n.TDM_Job_Source
    ,n.TDM_Job_Description
    ,n.TDM_Source_TDPID
    ,n.TDM_Source_User
    ,n.TDM_Source_Passwd_Encrypted
    ,n.TDM_Source_Logon_Mech
    ,n.TDM_Source_Logon_Mech_Data
    ,n.TDM_Source_Userid_Pool
    ,n.TDM_Source_Account_Id
    ,n.TDM_Source_Sessions
    ,n.TDM_Target_TDPID
    ,n.TDM_Target_User
    ,n.TDM_Target_Passwd_Encrypted
    ,n.TDM_Target_Logon_Mech
    ,n.TDM_Target_Logon_Mech_Data
    ,n.TDM_Target_Userid_Pool
    ,n.TDM_Target_Account_Id
    ,n.TDM_Target_Sessions
    ,n.TDM_Data_Streams
    ,n.TDM_Response_Timeout
    ,n.TDM_Overwrite_Existing_Obj
    ,n.TDM_Force_Utility
    ,n.TDM_Log_Level
    ,n.TDM_Online_Archive
    ,n.TDM_Freeze_Job_Steps
    ,n.TDM_Max_Agents_Per_Task
    ,n.TDM_Additional_ARC_Parameters
    ,n.TDM_Query_Band_Str
    ,n.TDM_Sync_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_Hist
  (
     Cand_Obj_Source_Type_Cd
    ,Cand_Obj_Source_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Obj_Source_Type_Cd
    ,o.Cand_Obj_Source_Type_Desc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_Hist
  (
     Db_Nm
    ,Obj_Nm
    ,Job_Cd
    ,UOWId
    ,JobName
    ,Source_TDPID
    ,Export_Completion_TS
    ,Export_Completion_Date
    ,Copy_Completion_TS
    ,Copy_Completion_Date
    ,Utility_Name
    ,EventMsg
    ,Row_Count
    ,Byte_Count
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Nm
    ,n.Obj_Nm
    ,n.Job_Cd
    ,n.UOWId
    ,n.JobName
    ,n.Source_TDPID
    ,n.Export_Completion_TS
    ,n.Export_Completion_Date
    ,n.Copy_Completion_TS
    ,n.Copy_Completion_Date
    ,n.Utility_Name
    ,n.EventMsg
    ,n.Row_Count
    ,n.Byte_Count
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Stus_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Stus_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDM_Job_Stus
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Stus_Hist
  (
     Job_Cd
    ,User_Stus_Cd
    ,System_Stus_Cd
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.User_Stus_Cd
    ,o.System_Stus_Cd
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_Hist
  (
     Cycl_Start_Ts
    ,Cycl_End_Ts
    ,Cycl_Stat
    ,Src_Sys
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cycl_Start_Ts
    ,o.Cycl_End_Ts
    ,o.Cycl_Stat
    ,o.Src_Sys
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_Hist
  (
     Db_Nm
    ,Obj_Nm
    ,Job_Cd
    ,UOWId
    ,JobName
    ,Source_TDPID
    ,Export_Completion_TS
    ,Export_Completion_Date
    ,Copy_Completion_TS
    ,Copy_Completion_Date
    ,Utility_Name
    ,EventMsg
    ,Row_Count
    ,Byte_Count
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Nm
    ,n.Obj_Nm
    ,n.Job_Cd
    ,n.UOWId
    ,n.JobName
    ,n.Source_TDPID
    ,n.Export_Completion_TS
    ,n.Export_Completion_Date
    ,n.Copy_Completion_TS
    ,n.Copy_Completion_Date
    ,n.Utility_Name
    ,n.EventMsg
    ,n.Row_Count
    ,n.Byte_Count
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_SQLState_Class_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_SQLState_Class_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_SQLState_Class
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_SQLState_Class_Hist
  (
     ClassCode
    ,ClassDefinition
    ,ClassNote
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     n.ClassCode
    ,n.ClassDefinition
    ,n.ClassNote
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_TS
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_TS
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_Hist
  (
     Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Db_Nm
    ,o.Tbl_Nm
    ,o.User_ID
    ,o.Max_Pros_Key_I
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Obj_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Obj_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDM_Job_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Obj_Hist
  (
     Job_Cd
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Db_Nm
    ,n.Obj_Nm
    ,n.Obj_Type
    ,n.Copy_Stat
    ,n.Compare_DDL
    ,n.Where_Clause
    ,n.Key_Column
    ,n.Query_Band_Str
    ,n.Job_Desc
    ,n.Staging_Database_Nm
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_Hist
  (
     Cand_Obj_Id
    ,Related_Cand_Obj_Id
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Id
    ,n.Related_Cand_Obj_Id
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus_Hist
  (
     LogDate
    ,RowsReturned
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.LogDate
    ,n.RowsReturned
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_Hist
  (
     Log_Id
    ,Prog_Host
    ,Prog_Name
    ,Func_Name
    ,Proc_Id
    ,Mesg_Type
    ,Mesg_Body
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Prog_Mode
    ,Prog_Vers
    ,Send_Alert_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Log_Id
    ,o.Prog_Host
    ,o.Prog_Name
    ,o.Func_Name
    ,o.Proc_Id
    ,o.Mesg_Type
    ,o.Mesg_Body
    ,o.Prog_Stat_Cd
    ,o.Prog_Stat_Desc
    ,o.Prog_Mode
    ,o.Prog_Vers
    ,o.Send_Alert_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_Hist
  (
     Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Prog_Vers
    ,Proc_Id
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Enabled_Flag
    ,Prog_Beg_Ts
    ,Prog_End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Prog_Name
    ,o.Prog_Mode
    ,o.Prog_User
    ,o.Prog_Host
    ,o.Prog_Vers
    ,o.Proc_Id
    ,o.Prog_Stat_Cd
    ,o.Prog_Stat_Desc
    ,o.Enabled_Flag
    ,o.Prog_Beg_Ts
    ,o.Prog_End_Ts
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_Hist
  (
     Cand_Obj_Stus_Type_Cd
    ,Cand_Obj_Stus_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Obj_Stus_Type_Cd
    ,o.Cand_Obj_Stus_Type_Desc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_SQLState_Cds_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_SQLState_Cds_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_SQLState_Cds
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_SQLState_Cds_Hist
  (
     SQLStateCd
    ,ClassCd
    ,SubClassCd
    ,TDErrorCd
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     n.SQLStateCd
    ,n.ClassCd
    ,n.SubClassCd
    ,n.TDErrorCd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_TS
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_TS
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Hist
  (
     Cand_Id
    ,Trtmnt_Cd
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,TDM_Obj_Type
    ,TDM_Copy_Stat
    ,TDM_Compare_DDL
    ,TDM_Where_Clause
    ,TDM_Key_Column
    ,TDM_Query_Band_Str
    ,TDM_Job_Desc
    ,TDM_Staging_Database_Nm
    ,TDM_Job_Cd
    ,Force_Flag
    ,Tbl_Size
    ,Job_Cd
    ,UOWId
    ,num_Tbl
    ,running_flag
    ,failed_flag
    ,exists_flag
    ,Tbl_flag
    ,changed_flag
    ,Rule_No
    ,Limit_Desc
    ,dedupe_cd
    ,Enabled_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Trtmnt_Cd
    ,n.TDM_Database_Nm
    ,n.TDM_Obj_Nm
    ,n.TDM_Obj_Type
    ,n.TDM_Copy_Stat
    ,n.TDM_Compare_DDL
    ,n.TDM_Where_Clause
    ,n.TDM_Key_Column
    ,n.TDM_Query_Band_Str
    ,n.TDM_Job_Desc
    ,n.TDM_Staging_Database_Nm
    ,n.TDM_Job_Cd
    ,n.Force_Flag
    ,n.Tbl_Size
    ,n.Job_Cd
    ,n.UOWId
    ,n.num_Tbl
    ,n.running_flag
    ,n.failed_flag
    ,n.exists_flag
    ,n.Tbl_flag
    ,n.changed_flag
    ,n.Rule_No
    ,n.Limit_Desc
    ,n.dedupe_cd
    ,n.Enabled_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_Hist
  (
     Cand_Obj_Source_Type_Cd
    ,Cand_Obj_Source_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Obj_Source_Type_Cd
    ,o.Cand_Obj_Source_Type_Desc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Id_Obj_Hist
  (
     Job_Id
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Db_Nm
    ,n.Obj_Nm
    ,n.Obj_Type
    ,n.Copy_Stat
    ,n.Compare_DDL
    ,n.Where_Clause
    ,n.Key_Column
    ,n.Query_Band_Str
    ,n.Job_Desc
    ,n.Staging_Database_Nm
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD_Hist
  (
     DB_Vers
    ,Queryplan_Path
    ,Queryplan_Len
    ,Queryplan_XSD
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DB_Vers
    ,n.Queryplan_Path
    ,n.Queryplan_Len
    ,n.Queryplan_XSD
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_Hist
  (
     Obj
    ,Is_Current_Flag
    ,Is_Enabled_Flag
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Key_Type
    ,Key_Name
    ,Key_Sql
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Obj
    ,n.Is_Current_Flag
    ,n.Is_Enabled_Flag
    ,n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Key_Type
    ,n.Key_Name
    ,n.Key_Sql
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Metric_Snap_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Sys_Name
    ,Update_Tally
    ,Elapsed_Secs
    ,Key_Type1
    ,Key_Type2
    ,Key_Type3
    ,Key_Value
    ,Key_Where_Flag
    ,Key_Where_Val
    ,Key_Where_Cond
    ,Key_Partition_Flag
    ,Key_Partition_Val
    ,Key_Partition_Col
    ,Prev_Create_Date
    ,Prev_Create_Ts
    ,Is_Current_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Sys_Name
    ,n.Update_Tally
    ,n.Elapsed_Secs
    ,n.Key_Type1
    ,n.Key_Type2
    ,n.Key_Type3
    ,n.Key_Value
    ,n.Key_Where_Flag
    ,n.Key_Where_Val
    ,n.Key_Where_Cond
    ,n.Key_Partition_Flag
    ,n.Key_Partition_Val
    ,n.Key_Partition_Col
    ,n.Prev_Create_Date
    ,n.Prev_Create_Ts
    ,n.Is_Current_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I_Src
    ,Max_Pros_Key_I_Tgt
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Nm
    ,n.Db_Nm
    ,n.Tbl_Nm
    ,n.User_ID
    ,n.Max_Pros_Key_I_Src
    ,n.Max_Pros_Key_I_Tgt
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_DM_Stus_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_DM_Stus_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDM_DM_Stus
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_DM_Stus_Hist
  (
     ResourceId
    ,UOWId
    ,Job_CD
    ,Job_Stus_Cd
    ,Start_TS
    ,End_TS
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.ResourceId
    ,o.UOWId
    ,o.Job_CD
    ,o.Job_Stus_Cd
    ,o.Start_TS
    ,o.End_TS
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Obj_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Obj_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDM_Job_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Hist
  (
     Job_Cd
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Db_Nm
    ,n.Obj_Nm
    ,n.Obj_Type
    ,n.Copy_Stat
    ,n.Compare_DDL
    ,n.Where_Clause
    ,n.Key_Column
    ,n.Query_Band_Str
    ,n.Job_Desc
    ,n.Staging_Database_Nm
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_Hist
  (
     DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.DB_Name
    ,o.Obj_Name
    ,o.Col_1_Name
    ,o.Col_2_Name
    ,o.Join_Col_1_Name
    ,o.Join_Col_2_Name
    ,o.Predicate_Clause_Text
    ,o.Join_Type
    ,o.Rltd_DB_Name
    ,o.Rltd_Obj_Name
    ,o.Rltd_Col_1_Name
    ,o.Rltd_Col_2_Name
    ,o.Rltd_Join_Col_1_Name
    ,o.Rltd_Join_Col_2_Name
    ,o.Rltd_Predicate_Clause_Text
    ,o.Is_Enabled
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step_Hist
  (
     Job_Id
    ,Step_No
    ,Step_Start_Ts
    ,Step_End_Ts
    ,Step_Stus_Cd
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Final_Step_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Id
    ,o.Step_No
    ,o.Step_Start_Ts
    ,o.Step_End_Ts
    ,o.Step_Stus_Cd
    ,o.Step_Desc
    ,o.Step_Type
    ,o.Executable_Nm
    ,o.Executable_Db
    ,o.Executable_Type
    ,o.Executable_Param
    ,o.Executable_User
    ,o.Executable_Platform_Name
    ,o.Executable_BT_ET_Flag
    ,o.Enabled_Flag
    ,o.Final_Step_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_User_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I_Src
    ,Max_Pros_Key_I_Tgt
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Nm
    ,n.Db_Nm
    ,n.Tbl_Nm
    ,n.User_ID
    ,n.Max_Pros_Key_I_Src
    ,n.Max_Pros_Key_I_Tgt
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Repl_Type_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Repl_Type_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Repl_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Repl_Type_Hist
  (
     Repl_Type_Cd
    ,Repl_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Repl_Type_Cd
    ,n.Repl_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers_Hist
  (
     ParameterValue
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.ParameterValue
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_SQLState_Class_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_SQLState_Class_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_SQLState_Class
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_SQLState_Class_Hist
  (
     ClassCode
    ,ClassDefinition
    ,ClassNote
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     n.ClassCode
    ,n.ClassDefinition
    ,n.ClassNote
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_TS
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_TS
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Stus_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Stus_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDM_Job_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Stus_Hist
  (
     Job_Cd
    ,User_Stus_Cd
    ,System_Stus_Cd
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.User_Stus_Cd
    ,n.System_Stus_Cd
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Rule_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Rule_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Obj_Rule
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Rule_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Enabled_Flag
    ,Repl_Type_Cd
    ,Predicate_Clause_Text
    ,Force_Flag
    ,Key_Column
    ,Obj_Size_Type_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Enabled_Flag
    ,n.Repl_Type_Cd
    ,n.Predicate_Clause_Text
    ,n.Force_Flag
    ,n.Key_Column
    ,n.Obj_Size_Type_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_Stat_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_Stat_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_JA_Stat
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_Stat_Hist
  (
     Submit_TS
    ,No_Cands
    ,No_Obj
    ,No_MB
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Submit_TS
    ,n.No_Cands
    ,n.No_Obj
    ,n.No_MB
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type_Hist
  (
     Obj_Size_Type_Cd
    ,Obj_Size_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Obj_Size_Type_Cd
    ,o.Obj_Size_Type_Desc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Scope_Stream_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Scope_Stream_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Scope_Stream
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Scope_Stream_Hist
  (
     Stream_Name
    ,Stream_Type
    ,Job_Name
    ,Enabled_Flag
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Name
    ,n.Stream_Type
    ,n.Job_Name
    ,n.Enabled_Flag
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Start_Ts
    ,n.End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Data_Purge_Log_Hist
  (
     Purge_Db
    ,Purge_Tbl
    ,Log_Date
    ,Start_Time
    ,Stop_Time
    ,Rows_Returned
    ,Status
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Purge_Db
    ,n.Purge_Tbl
    ,n.Log_Date
    ,n.Start_Time
    ,n.Stop_Time
    ,n.Rows_Returned
    ,n.Status
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Cfg_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Cfg_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Cand_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Cfg_Hist
  (
    Cand_Source
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
    n.Cand_Source
    ,n.TDM_Job_Priority
    ,n.TDM_Job_Type
    ,n.TDM_Job_Source
    ,n.TDM_Job_Description
    ,n.TDM_Source_TDPID
    ,n.TDM_Source_User
    ,n.TDM_Source_Passwd_Encrypted
    ,n.TDM_Source_Logon_Mech
    ,n.TDM_Source_Logon_Mech_Data
    ,n.TDM_Source_Userid_Pool
    ,n.TDM_Source_Account_Id
    ,n.TDM_Source_Sessions
    ,n.TDM_Target_TDPID
    ,n.TDM_Target_User
    ,n.TDM_Target_Passwd_Encrypted
    ,n.TDM_Target_Logon_Mech
    ,n.TDM_Target_Logon_Mech_Data
    ,n.TDM_Target_Userid_Pool
    ,n.TDM_Target_Account_Id
    ,n.TDM_Target_Sessions
    ,n.TDM_Data_Streams
    ,n.TDM_Response_Timeout
    ,n.TDM_Overwrite_Existing_Obj
    ,n.TDM_Force_Utility
    ,n.TDM_Log_Level
    ,n.TDM_Online_Archive
    ,n.TDM_Freeze_Job_Steps
    ,n.TDM_Max_Agents_Per_Task
    ,n.TDM_Additional_ARC_Parameters
    ,n.TDM_Query_Band_Str
    ,n.TDM_Sync_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Alrt_Dest_Hist
  (
     Destination
    ,DestinationDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Destination
    ,n.DestinationDesc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_Hist
  (
     Db_Nm
    ,Obj_Nm
    ,Job_Cd
    ,UOWId
    ,JobName
    ,Source_TDPID
    ,Export_Completion_TS
    ,Export_Completion_Date
    ,Copy_Completion_TS
    ,Copy_Completion_Date
    ,Utility_Name
    ,EventMsg
    ,Row_Count
    ,Byte_Count
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Nm
    ,n.Obj_Nm
    ,n.Job_Cd
    ,n.UOWId
    ,n.JobName
    ,n.Source_TDPID
    ,n.Export_Completion_TS
    ,n.Export_Completion_Date
    ,n.Copy_Completion_TS
    ,n.Copy_Completion_Date
    ,n.Utility_Name
    ,n.EventMsg
    ,n.Row_Count
    ,n.Byte_Count
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type_Hist
  (
     Obj_Size_Type_Cd
    ,Obj_Size_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Obj_Size_Type_Cd
    ,n.Obj_Size_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Obj_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Obj_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDM_Job_Obj
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Obj_Hist
  (
     Job_Cd
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.Db_Nm
    ,o.Obj_Nm
    ,o.Obj_Type
    ,o.Copy_Stat
    ,o.Compare_DDL
    ,o.Where_Clause
    ,o.Key_Column
    ,o.Query_Band_Str
    ,o.Job_Desc
    ,o.Staging_Database_Nm
    ,o.Enabled_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Stream_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Stream_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Cand_Stream
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Stream_Hist
  (
     Stream_Id
    ,Stream_Name
    ,Stream_RunNo
    ,Stream_Type
    ,Job_Name
    ,Job_Stat
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Id
    ,n.Stream_Name
    ,n.Stream_RunNo
    ,n.Stream_Type
    ,n.Job_Name
    ,n.Job_Stat
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Start_Ts
    ,n.End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Step_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Step_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDM_Job_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Step_Hist
  (
     Job_Cd
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Step_No
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Executable_User
    ,n.Executable_Platform_Name
    ,n.Executable_BT_ET_Flag
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Id_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Id_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDM_Job_Id
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Id_Hist
  (
     Job_Id
    ,Job_Stus_Cd
    ,Job_Start_Ts
    ,Job_End_Ts
    ,Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Job_Stus_Cd
    ,n.Job_Start_Ts
    ,n.Job_End_Ts
    ,n.Job_Cd
    ,n.UOW_Id
    ,n.Job_Priority
    ,n.Job_Type
    ,n.Job_Source
    ,n.Job_Description
    ,n.Source_TDPID
    ,n.Source_User
    ,n.Source_Passwd_Encrypted
    ,n.Source_Logon_Mechanism
    ,n.Source_Logon_Mechanism_Data
    ,n.Source_Userid_Pool
    ,n.Source_Account_Id
    ,n.Source_Sessions
    ,n.Target_TDPID
    ,n.Target_User
    ,n.Target_Passwd_Encrypted
    ,n.Target_Logon_Mechanism
    ,n.Target_Logon_Mechanism_Data
    ,n.Target_Userid_Pool
    ,n.Target_Account_Id
    ,n.Target_Sessions
    ,n.Data_Streams
    ,n.Response_Timeout
    ,n.Overwrite_Existing_Obj
    ,n.Force_Utility
    ,n.Log_Level
    ,n.Online_Archive
    ,n.Freeze_Job_Steps
    ,n.Max_Agents_Per_Task
    ,n.Additional_ARC_Parameters
    ,n.Query_Band_Str
    ,n.Sync_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Limit_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Limit_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Cand_Limit
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Limit_Hist
  (
     Limit_No
    ,Candidates_Per_Iteration
    ,Obj_Per_Iteration
    ,MB_Per_Iteration
    ,Candidates_Per_Hour
    ,Obj_Per_Hour
    ,MB_Per_Hour
    ,Num_Running_DM_Jobs
    ,Num_Running_Tbl
    ,Num_Staged_Jobs
    ,Num_Staged_Tbl
    ,Num_Total_Jobs
    ,Num_Total_Tbl
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Limit_No
    ,n.Candidates_Per_Iteration
    ,n.Obj_Per_Iteration
    ,n.MB_Per_Iteration
    ,n.Candidates_Per_Hour
    ,n.Obj_Per_Hour
    ,n.MB_Per_Hour
    ,n.Num_Running_DM_Jobs
    ,n.Num_Running_Tbl
    ,n.Num_Staged_Jobs
    ,n.Num_Staged_Tbl
    ,n.Num_Total_Jobs
    ,n.Num_Total_Tbl
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Repl_Cycl_Log_Hist
  (
     Cycl_Start_Ts
    ,Cycl_End_Ts
    ,Cycl_Stat
    ,Src_Sys
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cycl_Start_Ts
    ,o.Cycl_End_Ts
    ,o.Cycl_Stat
    ,o.Src_Sys
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_DDL_Chg_Hist
  (
     DataBaseName
    ,TableName
    ,Version
    ,CreatorName
    ,CreateTimeStamp
    ,LastAlterName
    ,LastAlterTimeStamp
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.DataBaseName
    ,o.TableName
    ,o.Version
    ,o.CreatorName
    ,o.CreateTimeStamp
    ,o.LastAlterName
    ,o.LastAlterTimeStamp
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_SQLState_Cds_Hist
  (
     SQLStateCd
    ,ClassCd
    ,SubClassCd
    ,TDErrorCd
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     n.SQLStateCd
    ,n.ClassCd
    ,n.SubClassCd
    ,n.TDErrorCd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_TS
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_TS
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_Hist
  (
     Obj
    ,Is_Current_Flag
    ,Is_Enabled_Flag
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Key_Type
    ,Key_Name
    ,Key_Sql
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Obj
    ,o.Is_Current_Flag
    ,o.Is_Enabled_Flag
    ,o.Db_Name
    ,o.Obj_Name
    ,o.Obj_Type
    ,o.Key_Type
    ,o.Key_Name
    ,o.Key_Sql
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_Hist
  (
     Db_Nm
    ,Obj_Nm
    ,Job_Cd
    ,UOWId
    ,JobName
    ,Source_TDPID
    ,Export_Completion_TS
    ,Export_Completion_Date
    ,Copy_Completion_TS
    ,Copy_Completion_Date
    ,Utility_Name
    ,EventMsg
    ,Row_Count
    ,Byte_Count
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Nm
    ,n.Obj_Nm
    ,n.Job_Cd
    ,n.UOWId
    ,n.JobName
    ,n.Source_TDPID
    ,n.Export_Completion_TS
    ,n.Export_Completion_Date
    ,n.Copy_Completion_TS
    ,n.Copy_Completion_Date
    ,n.Utility_Name
    ,n.EventMsg
    ,n.Row_Count
    ,n.Byte_Count
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Alrt_Action_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Alrt_Action_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Alrt_Action
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Alrt_Action_Hist
  (
     ActionCode
    ,ActionDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.ActionCode
    ,n.ActionDesc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Repl_Type_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Repl_Type_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Repl_Type
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Repl_Type_Hist
  (
     Repl_Type_Cd
    ,Repl_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Repl_Type_Cd
    ,o.Repl_Type_Desc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_Hist
  (
     Cand_Obj_Id
    ,Related_Cand_Obj_Id
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Obj_Id
    ,o.Related_Cand_Obj_Id
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn_Hist
  (
     DB_Name
    ,Tbl_Name
    ,Col_Name
    ,Ret_Period
    ,Impacted_Obj_DB_Name
    ,Impacted_Obj_Name
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DB_Name
    ,n.Tbl_Name
    ,n.Col_Name
    ,n.Ret_Period
    ,n.Impacted_Obj_DB_Name
    ,n.Impacted_Obj_Name
    ,n.Is_Enabled
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_Queryplan_XSD_Hist
  (
     DB_Vers
    ,Queryplan_Path
    ,Queryplan_Len
    ,Queryplan_XSD
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DB_Vers
    ,n.Queryplan_Path
    ,n.Queryplan_Len
    ,n.Queryplan_XSD
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDM_Job
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Hist
  (
     Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.UOW_Id
    ,n.Job_Priority
    ,n.Job_Type
    ,n.Job_Source
    ,n.Job_Description
    ,n.Source_TDPID
    ,n.Source_User
    ,n.Source_Passwd_Encrypted
    ,n.Source_Logon_Mechanism
    ,n.Source_Logon_Mechanism_Data
    ,n.Source_Userid_Pool
    ,n.Source_Account_Id
    ,n.Source_Sessions
    ,n.Target_TDPID
    ,n.Target_User
    ,n.Target_Passwd_Encrypted
    ,n.Target_Logon_Mechanism
    ,n.Target_Logon_Mechanism_Data
    ,n.Target_Userid_Pool
    ,n.Target_Account_Id
    ,n.Target_Sessions
    ,n.Data_Streams
    ,n.Response_Timeout
    ,n.Overwrite_Existing_Obj
    ,n.Force_Utility
    ,n.Log_Level
    ,n.Online_Archive
    ,n.Freeze_Job_Steps
    ,n.Max_Agents_Per_Task
    ,n.Additional_ARC_Parameters
    ,n.Query_Band_Str
    ,n.Sync_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Type_Hist
  (
     Cand_Obj_Stus_Type_Cd
    ,Cand_Obj_Stus_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Obj_Stus_Type_Cd
    ,o.Cand_Obj_Stus_Type_Desc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Stat_Hist
  (
     Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Prog_Vers
    ,Proc_Id
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Enabled_Flag
    ,Prog_Beg_Ts
    ,Prog_End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Prog_Name
    ,n.Prog_Mode
    ,n.Prog_User
    ,n.Prog_Host
    ,n.Prog_Vers
    ,n.Proc_Id
    ,n.Prog_Stat_Cd
    ,n.Prog_Stat_Desc
    ,n.Enabled_Flag
    ,n.Prog_Beg_Ts
    ,n.Prog_End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Id_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Id_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDM_Job_Id
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Id_Hist
  (
     Job_Id
    ,Job_Stus_Cd
    ,Job_Start_Ts
    ,Job_End_Ts
    ,Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Job_Stus_Cd
    ,n.Job_Start_Ts
    ,n.Job_End_Ts
    ,n.Job_Cd
    ,n.UOW_Id
    ,n.Job_Priority
    ,n.Job_Type
    ,n.Job_Source
    ,n.Job_Description
    ,n.Source_TDPID
    ,n.Source_User
    ,n.Source_Passwd_Encrypted
    ,n.Source_Logon_Mechanism
    ,n.Source_Logon_Mechanism_Data
    ,n.Source_Userid_Pool
    ,n.Source_Account_Id
    ,n.Source_Sessions
    ,n.Target_TDPID
    ,n.Target_User
    ,n.Target_Passwd_Encrypted
    ,n.Target_Logon_Mechanism
    ,n.Target_Logon_Mechanism_Data
    ,n.Target_Userid_Pool
    ,n.Target_Account_Id
    ,n.Target_Sessions
    ,n.Data_Streams
    ,n.Response_Timeout
    ,n.Overwrite_Existing_Obj
    ,n.Force_Utility
    ,n.Log_Level
    ,n.Online_Archive
    ,n.Freeze_Job_Steps
    ,n.Max_Agents_Per_Task
    ,n.Additional_ARC_Parameters
    ,n.Query_Band_Str
    ,n.Sync_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Stus_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Stus_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDM_Job_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Stus_Hist
  (
     Job_Cd
    ,User_Stus_Cd
    ,System_Stus_Cd
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.User_Stus_Cd
    ,n.System_Stus_Cd
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Rules_Hist
  (
     DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DB_Name
    ,n.Obj_Name
    ,n.Col_1_Name
    ,n.Col_2_Name
    ,n.Join_Col_1_Name
    ,n.Join_Col_2_Name
    ,n.Predicate_Clause_Text
    ,n.Join_Type
    ,n.Rltd_DB_Name
    ,n.Rltd_Obj_Name
    ,n.Rltd_Col_1_Name
    ,n.Rltd_Col_2_Name
    ,n.Rltd_Join_Col_1_Name
    ,n.Rltd_Join_Col_2_Name
    ,n.Rltd_Predicate_Clause_Text
    ,n.Is_Enabled
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Size_Type_Hist
  (
     Obj_Size_Type_Cd
    ,Obj_Size_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Obj_Size_Type_Cd
    ,n.Obj_Size_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Pros_Stus_Hist
  (
     LogDate
    ,RowsReturned
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.LogDate
    ,n.RowsReturned
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_Hist
  (
     Cand_Id
    ,Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Job_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_TD_Vers_Hist
  (
     InfoData
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.InfoData
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Alrt_Action_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Alrt_Action_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Alrt_Action
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Alrt_Action_Hist
  (
     ActionCode
    ,ActionDesc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.ActionCode
    ,n.ActionDesc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Next_Job_Id_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Next_Job_Id_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDM_Next_Job_Id
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Next_Job_Id_Hist
  (
     Fixed_Value
    ,Next_Job_Id
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Fixed_Value
    ,o.Next_Job_Id
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_Hist
  (
     Log_Date
    ,Log_TS
    ,The_Count
    ,DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Gen_RI_SQL_Text
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Log_Date
    ,n.Log_TS
    ,n.The_Count
    ,n.DB_Name
    ,n.Obj_Name
    ,n.Col_1_Name
    ,n.Col_2_Name
    ,n.Join_Col_1_Name
    ,n.Join_Col_2_Name
    ,n.Predicate_Clause_Text
    ,n.Join_Type
    ,n.Rltd_DB_Name
    ,n.Rltd_Obj_Name
    ,n.Rltd_Col_1_Name
    ,n.Rltd_Col_2_Name
    ,n.Rltd_Join_Col_1_Name
    ,n.Rltd_Join_Col_2_Name
    ,n.Rltd_Predicate_Clause_Text
    ,n.Gen_RI_SQL_Text
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_Vers_Hist
  (
     ParameterValue
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.ParameterValue
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_Hist
  (
     Daemon_Name
    ,Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Enabled_Flag
    ,Loop_Delay_Secs
    ,Send_Alert_On_Error_Flag
    ,Autosys_User
    ,Autosys_Host
    ,Autosys_Host_IP
    ,Autosys_Stus_Restrict_Flag
    ,Autosys_Stus_Allowed_Set
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Daemon_Name
    ,n.Prog_Name
    ,n.Prog_Mode
    ,n.Prog_User
    ,n.Prog_Host
    ,n.Enabled_Flag
    ,n.Loop_Delay_Secs
    ,n.Send_Alert_On_Error_Flag
    ,n.Autosys_User
    ,n.Autosys_Host
    ,n.Autosys_Host_IP
    ,n.Autosys_Stus_Restrict_Flag
    ,n.Autosys_Stus_Allowed_Set
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_Hist
  (
     Cand_Id
    ,Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Job_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_Hist
  (
     Stream_Name
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Stream_Name
    ,o.Db_Name
    ,o.Obj_Name
    ,o.Obj_Type
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Rule_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Rule_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Obj_Rule
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Rule_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Enabled_Flag
    ,Repl_Type_Cd
    ,Predicate_Clause_Text
    ,Force_Flag
    ,Key_Column
    ,Obj_Size_Type_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Db_Name
    ,o.Obj_Name
    ,o.Obj_Type
    ,o.Enabled_Flag
    ,o.Repl_Type_Cd
    ,o.Predicate_Clause_Text
    ,o.Force_Flag
    ,o.Key_Column
    ,o.Obj_Size_Type_Cd
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_NM
    ,TDM_Key_Column
    ,Pros_Isac_DB_NM
    ,ODS_Insert_Col_Flag
    ,ODS_Update_Col_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Stream_Nm
    ,o.Db_Nm
    ,o.Tbl_NM
    ,o.TDM_Key_Column
    ,o.Pros_Isac_DB_NM
    ,o.ODS_Insert_Col_Flag
    ,o.ODS_Update_Col_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_Stat_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_Stat_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_JA_Stat
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_Stat_Hist
  (
     Submit_TS
    ,No_Cands
    ,No_Obj
    ,No_MB
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Submit_TS
    ,n.No_Cands
    ,n.No_Obj
    ,n.No_MB
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_Stat_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_Stat_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_JA_Stat
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_Stat_Hist
  (
     Submit_TS
    ,No_Cands
    ,No_Obj
    ,No_MB
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Submit_TS
    ,o.No_Cands
    ,o.No_Obj
    ,o.No_MB
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands_Hist
  (
     Cand_Id
    ,Cand_Trtmnt_Cd
    ,Job_Cd
    ,Job_Stus_Cd
    ,Created_Ts
    ,TargetTable
    ,Business_Date
    ,Business_Date_Cycle_Start_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Cand_Trtmnt_Cd
    ,n.Job_Cd
    ,n.Job_Stus_Cd
    ,n.Created_Ts
    ,n.TargetTable
    ,n.Business_Date
    ,n.Business_Date_Cycle_Start_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg_Hist
  (
    Cand_Source
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
    n.Cand_Source
    ,n.TDM_Job_Priority
    ,n.TDM_Job_Type
    ,n.TDM_Job_Source
    ,n.TDM_Job_Description
    ,n.TDM_Source_TDPID
    ,n.TDM_Source_User
    ,n.TDM_Source_Passwd_Encrypted
    ,n.TDM_Source_Logon_Mech
    ,n.TDM_Source_Logon_Mech_Data
    ,n.TDM_Source_Userid_Pool
    ,n.TDM_Source_Account_Id
    ,n.TDM_Source_Sessions
    ,n.TDM_Target_TDPID
    ,n.TDM_Target_User
    ,n.TDM_Target_Passwd_Encrypted
    ,n.TDM_Target_Logon_Mech
    ,n.TDM_Target_Logon_Mech_Data
    ,n.TDM_Target_Userid_Pool
    ,n.TDM_Target_Account_Id
    ,n.TDM_Target_Sessions
    ,n.TDM_Data_Streams
    ,n.TDM_Response_Timeout
    ,n.TDM_Overwrite_Existing_Obj
    ,n.TDM_Force_Utility
    ,n.TDM_Log_Level
    ,n.TDM_Online_Archive
    ,n.TDM_Freeze_Job_Steps
    ,n.TDM_Max_Agents_Per_Task
    ,n.TDM_Additional_ARC_Parameters
    ,n.TDM_Query_Band_Str
    ,n.TDM_Sync_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Cand
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Hist
  (
     Cand_Id
    ,Description
    ,Trtmnt_Cd
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Priority
    ,Cand_Source
    ,Source_Id
    ,Stream_End_TS
    ,Enabled_Flag
    ,Num_Retries
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Exec_Param_KVP    
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Id
    ,o.Description
    ,o.Trtmnt_Cd
    ,o.Allow_Cnsldtn_Flag
    ,o.Allow_Partial_Flag
    ,o.Priority
    ,o.Cand_Source
    ,o.Source_Id
    ,o.Stream_End_TS
    ,o.Enabled_Flag
    ,o.Num_Retries
    ,o.TDM_Job_Priority
    ,o.TDM_Job_Type
    ,o.TDM_Job_Source
    ,o.TDM_Job_Description
    ,o.TDM_Source_TDPID
    ,o.TDM_Source_User
    ,o.TDM_Source_Passwd_Encrypted
    ,o.TDM_Source_Logon_Mech
    ,o.TDM_Source_Logon_Mech_Data
    ,o.TDM_Source_Userid_Pool
    ,o.TDM_Source_Account_Id
    ,o.TDM_Source_Sessions
    ,o.TDM_Target_TDPID
    ,o.TDM_Target_User
    ,o.TDM_Target_Passwd_Encrypted
    ,o.TDM_Target_Logon_Mech
    ,o.TDM_Target_Logon_Mech_Data
    ,o.TDM_Target_Userid_Pool
    ,o.TDM_Target_Account_Id
    ,o.TDM_Target_Sessions
    ,o.TDM_Data_Streams
    ,o.TDM_Response_Timeout
    ,o.TDM_Overwrite_Existing_Obj
    ,o.TDM_Force_Utility
    ,o.TDM_Log_Level
    ,o.TDM_Online_Archive
    ,o.TDM_Freeze_Job_Steps
    ,o.TDM_Max_Agents_Per_Task
    ,o.TDM_Additional_ARC_Parameters
    ,o.TDM_Query_Band_Str
    ,o.TDM_Sync_Flag
    ,o.Exec_Param_KVP    
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,o.Update_User
    ,o.Update_Date
    ,o.Update_Ts
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Stus_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Stus_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDM_Job_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Stus_Hist
  (
     Job_Cd
    ,User_Stus_Cd
    ,System_Stus_Cd
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.User_Stus_Cd
    ,n.System_Stus_Cd
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_NM
    ,TDM_Key_Column
    ,Pros_Isac_DB_NM
    ,ODS_Insert_Col_Flag
    ,ODS_Update_Col_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Stream_Nm
    ,o.Db_Nm
    ,o.Tbl_NM
    ,o.TDM_Key_Column
    ,o.Pros_Isac_DB_NM
    ,o.ODS_Insert_Col_Flag
    ,o.ODS_Update_Col_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Stream_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Stream_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Stream
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Stream_Hist
  (
     Stream_Id
    ,Stream_Name
    ,Stream_RunNo
    ,Stream_Type
    ,Job_Name
    ,Job_Stat
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Stream_Id
    ,o.Stream_Name
    ,o.Stream_RunNo
    ,o.Stream_Type
    ,o.Job_Name
    ,o.Job_Stat
    ,o.Allow_Cnsldtn_Flag
    ,o.Allow_Partial_Flag
    ,o.Start_Ts
    ,o.End_Ts
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Data_Purge_Log_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Data_Purge_Log_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Data_Purge_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Data_Purge_Log_Hist
  (
     Purge_Db
    ,Purge_Tbl
    ,Log_Date
    ,Start_Time
    ,Stop_Time
    ,Rows_Returned
    ,Status
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Purge_Db
    ,n.Purge_Tbl
    ,n.Log_Date
    ,n.Start_Time
    ,n.Stop_Time
    ,n.Rows_Returned
    ,n.Status
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Job_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Job_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_Job
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Job_Hist
  (
     Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_Hist
  (
     Daemon_Name
    ,Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Enabled_Flag
    ,Loop_Delay_Secs
    ,Send_Alert_On_Error_Flag
    ,Autosys_User
    ,Autosys_Host
    ,Autosys_Host_IP
    ,Autosys_Stus_Restrict_Flag
    ,Autosys_Stus_Allowed_Set
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Daemon_Name
    ,o.Prog_Name
    ,o.Prog_Mode
    ,o.Prog_User
    ,o.Prog_Host
    ,o.Enabled_Flag
    ,o.Loop_Delay_Secs
    ,o.Send_Alert_On_Error_Flag
    ,o.Autosys_User
    ,o.Autosys_Host
    ,o.Autosys_Host_IP
    ,o.Autosys_Stus_Restrict_Flag
    ,o.Autosys_Stus_Allowed_Set
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Cfg_Hist
  (
     Daemon_Name
    ,Prog_Name
    ,Prog_Mode
    ,Prog_User
    ,Prog_Host
    ,Enabled_Flag
    ,Loop_Delay_Secs
    ,Send_Alert_On_Error_Flag
    ,Autosys_User
    ,Autosys_Host
    ,Autosys_Host_IP
    ,Autosys_Stus_Restrict_Flag
    ,Autosys_Stus_Allowed_Set
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Daemon_Name
    ,o.Prog_Name
    ,o.Prog_Mode
    ,o.Prog_User
    ,o.Prog_Host
    ,o.Enabled_Flag
    ,o.Loop_Delay_Secs
    ,o.Send_Alert_On_Error_Flag
    ,o.Autosys_User
    ,o.Autosys_Host
    ,o.Autosys_Host_IP
    ,o.Autosys_Stus_Restrict_Flag
    ,o.Autosys_Stus_Allowed_Set
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Cand
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Hist
  (
     Cand_Id
    ,Description
    ,Trtmnt_Cd
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Priority
    ,Cand_Source
    ,Source_Id
    ,Stream_End_TS
    ,Enabled_Flag
    ,Num_Retries
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Exec_Param_KVP    
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Description
    ,n.Trtmnt_Cd
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Priority
    ,n.Cand_Source
    ,n.Source_Id
    ,n.Stream_End_TS
    ,n.Enabled_Flag
    ,n.Num_Retries
    ,n.TDM_Job_Priority
    ,n.TDM_Job_Type
    ,n.TDM_Job_Source
    ,n.TDM_Job_Description
    ,n.TDM_Source_TDPID
    ,n.TDM_Source_User
    ,n.TDM_Source_Passwd_Encrypted
    ,n.TDM_Source_Logon_Mech
    ,n.TDM_Source_Logon_Mech_Data
    ,n.TDM_Source_Userid_Pool
    ,n.TDM_Source_Account_Id
    ,n.TDM_Source_Sessions
    ,n.TDM_Target_TDPID
    ,n.TDM_Target_User
    ,n.TDM_Target_Passwd_Encrypted
    ,n.TDM_Target_Logon_Mech
    ,n.TDM_Target_Logon_Mech_Data
    ,n.TDM_Target_Userid_Pool
    ,n.TDM_Target_Account_Id
    ,n.TDM_Target_Sessions
    ,n.TDM_Data_Streams
    ,n.TDM_Response_Timeout
    ,n.TDM_Overwrite_Existing_Obj
    ,n.TDM_Force_Utility
    ,n.TDM_Log_Level
    ,n.TDM_Online_Archive
    ,n.TDM_Freeze_Job_Steps
    ,n.TDM_Max_Agents_Per_Task
    ,n.TDM_Additional_ARC_Parameters
    ,n.TDM_Query_Band_Str
    ,n.TDM_Sync_Flag
    ,n.Exec_Param_KVP    
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Id_Step_Hist
  (
     Job_Id
    ,Step_No
    ,Step_Start_Ts
    ,Step_End_Ts
    ,Step_Stus_Cd
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Final_Step_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Step_No
    ,n.Step_Start_Ts
    ,n.Step_End_Ts
    ,n.Step_Stus_Cd
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Executable_User
    ,n.Executable_Platform_Name
    ,n.Executable_BT_ET_Flag
    ,n.Enabled_Flag
    ,n.Final_Step_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_Hist
  (
     Log_Id
    ,Prog_Host
    ,Prog_Name
    ,Func_Name
    ,Proc_Id
    ,Mesg_Type
    ,Mesg_Body
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Prog_Mode
    ,Prog_Vers
    ,Send_Alert_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Log_Id
    ,n.Prog_Host
    ,n.Prog_Name
    ,n.Func_Name
    ,n.Proc_Id
    ,n.Mesg_Type
    ,n.Mesg_Body
    ,n.Prog_Stat_Cd
    ,n.Prog_Stat_Desc
    ,n.Prog_Mode
    ,n.Prog_Vers
    ,n.Send_Alert_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_AIT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_AIT
  AFTER INSERT ON V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_PI_Delta_Cand_Hist
  (
     Stream_Nm
    ,Db_Nm
    ,Tbl_NM
    ,TDM_Key_Column
    ,Pros_Isac_DB_NM
    ,ODS_Insert_Col_Flag
    ,ODS_Update_Col_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Nm
    ,n.Db_Nm
    ,n.Tbl_NM
    ,n.TDM_Key_Column
    ,n.Pros_Isac_DB_NM
    ,n.ODS_Insert_Col_Flag
    ,n.ODS_Update_Col_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDM_Job
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Hist
  (
     Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.UOW_Id
    ,n.Job_Priority
    ,n.Job_Type
    ,n.Job_Source
    ,n.Job_Description
    ,n.Source_TDPID
    ,n.Source_User
    ,n.Source_Passwd_Encrypted
    ,n.Source_Logon_Mechanism
    ,n.Source_Logon_Mechanism_Data
    ,n.Source_Userid_Pool
    ,n.Source_Account_Id
    ,n.Source_Sessions
    ,n.Target_TDPID
    ,n.Target_User
    ,n.Target_Passwd_Encrypted
    ,n.Target_Logon_Mechanism
    ,n.Target_Logon_Mechanism_Data
    ,n.Target_Userid_Pool
    ,n.Target_Account_Id
    ,n.Target_Sessions
    ,n.Data_Streams
    ,n.Response_Timeout
    ,n.Overwrite_Existing_Obj
    ,n.Force_Utility
    ,n.Log_Level
    ,n.Online_Archive
    ,n.Freeze_Job_Steps
    ,n.Max_Agents_Per_Task
    ,n.Additional_ARC_Parameters
    ,n.Query_Band_Str
    ,n.Sync_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Cfg_Hist
  (
     Cand_Source
    ,TDM_Job_Priority
    ,TDM_Job_Type
    ,TDM_Job_Source
    ,TDM_Job_Description
    ,TDM_Source_TDPID
    ,TDM_Source_User
    ,TDM_Source_Passwd_Encrypted
    ,TDM_Source_Logon_Mech
    ,TDM_Source_Logon_Mech_Data
    ,TDM_Source_Userid_Pool
    ,TDM_Source_Account_Id
    ,TDM_Source_Sessions
    ,TDM_Target_TDPID
    ,TDM_Target_User
    ,TDM_Target_Passwd_Encrypted
    ,TDM_Target_Logon_Mech
    ,TDM_Target_Logon_Mech_Data
    ,TDM_Target_Userid_Pool
    ,TDM_Target_Account_Id
    ,TDM_Target_Sessions
    ,TDM_Data_Streams
    ,TDM_Response_Timeout
    ,TDM_Overwrite_Existing_Obj
    ,TDM_Force_Utility
    ,TDM_Log_Level
    ,TDM_Online_Archive
    ,TDM_Freeze_Job_Steps
    ,TDM_Max_Agents_Per_Task
    ,TDM_Additional_ARC_Parameters
    ,TDM_Query_Band_Str
    ,TDM_Sync_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Source
    ,o.TDM_Job_Priority
    ,o.TDM_Job_Type
    ,o.TDM_Job_Source
    ,o.TDM_Job_Description
    ,o.TDM_Source_TDPID
    ,o.TDM_Source_User
    ,o.TDM_Source_Passwd_Encrypted
    ,o.TDM_Source_Logon_Mech
    ,o.TDM_Source_Logon_Mech_Data
    ,o.TDM_Source_Userid_Pool
    ,o.TDM_Source_Account_Id
    ,o.TDM_Source_Sessions
    ,o.TDM_Target_TDPID
    ,o.TDM_Target_User
    ,o.TDM_Target_Passwd_Encrypted
    ,o.TDM_Target_Logon_Mech
    ,o.TDM_Target_Logon_Mech_Data
    ,o.TDM_Target_Userid_Pool
    ,o.TDM_Target_Account_Id
    ,o.TDM_Target_Sessions
    ,o.TDM_Data_Streams
    ,o.TDM_Response_Timeout
    ,o.TDM_Overwrite_Existing_Obj
    ,o.TDM_Force_Utility
    ,o.TDM_Log_Level
    ,o.TDM_Online_Archive
    ,o.TDM_Freeze_Job_Steps
    ,o.TDM_Max_Agents_Per_Task
    ,o.TDM_Additional_ARC_Parameters
    ,o.TDM_Query_Band_Str
    ,o.TDM_Sync_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Cand_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Hist
  (
     Cand_Id
    ,Trtmnt_Cd
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,TDM_Obj_Type
    ,TDM_Copy_Stat
    ,TDM_Compare_DDL
    ,TDM_Where_Clause
    ,TDM_Key_Column
    ,TDM_Query_Band_Str
    ,TDM_Job_Desc
    ,TDM_Staging_Database_Nm
    ,TDM_Job_Cd
    ,Force_Flag
    ,Tbl_Size
    ,Job_Cd
    ,UOWId
    ,num_Tbl
    ,running_flag
    ,failed_flag
    ,exists_flag
    ,Tbl_flag
    ,changed_flag
    ,Rule_No
    ,Limit_Desc
    ,dedupe_cd
    ,Enabled_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Id
    ,n.Trtmnt_Cd
    ,n.TDM_Database_Nm
    ,n.TDM_Obj_Nm
    ,n.TDM_Obj_Type
    ,n.TDM_Copy_Stat
    ,n.TDM_Compare_DDL
    ,n.TDM_Where_Clause
    ,n.TDM_Key_Column
    ,n.TDM_Query_Band_Str
    ,n.TDM_Job_Desc
    ,n.TDM_Staging_Database_Nm
    ,n.TDM_Job_Cd
    ,n.Force_Flag
    ,n.Tbl_Size
    ,n.Job_Cd
    ,n.UOWId
    ,n.num_Tbl
    ,n.running_flag
    ,n.failed_flag
    ,n.exists_flag
    ,n.Tbl_flag
    ,n.changed_flag
    ,n.Rule_No
    ,n.Limit_Desc
    ,n.dedupe_cd
    ,n.Enabled_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Data_Rtntn_Hist
  (
     DB_Name
    ,Tbl_Name
    ,Col_Name
    ,Ret_Period
    ,Impacted_Obj_DB_Name
    ,Impacted_Obj_Name
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DB_Name
    ,n.Tbl_Name
    ,n.Col_Name
    ,n.Ret_Period
    ,n.Impacted_Obj_DB_Name
    ,n.Impacted_Obj_Name
    ,n.Is_Enabled
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Rule_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Rule_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Cand_Rule
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Rule_Hist
  (
     Rule_No
    ,Cand_Source
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,Priority
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Rule_No
    ,o.Cand_Source
    ,o.TDM_Database_Nm
    ,o.TDM_Obj_Nm
    ,o.Priority
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_Job_Obj_Metrics_Hist
  (
     Db_Nm
    ,Obj_Nm
    ,Job_Cd
    ,UOWId
    ,JobName
    ,Source_TDPID
    ,Export_Completion_TS
    ,Export_Completion_Date
    ,Copy_Completion_TS
    ,Copy_Completion_Date
    ,Utility_Name
    ,EventMsg
    ,Row_Count
    ,Byte_Count
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Db_Nm
    ,o.Obj_Nm
    ,o.Job_Cd
    ,o.UOWId
    ,o.JobName
    ,o.Source_TDPID
    ,o.Export_Completion_TS
    ,o.Export_Completion_Date
    ,o.Copy_Completion_TS
    ,o.Copy_Completion_Date
    ,o.Utility_Name
    ,o.EventMsg
    ,o.Row_Count
    ,o.Byte_Count
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Stream_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Stream_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Cand_Stream
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Stream_Hist
  (
     Stream_Id
    ,Stream_Name
    ,Stream_RunNo
    ,Stream_Type
    ,Job_Name
    ,Job_Stat
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Id
    ,n.Stream_Name
    ,n.Stream_RunNo
    ,n.Stream_Type
    ,n.Job_Name
    ,n.Job_Stat
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Start_Ts
    ,n.End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_Hist
  (
     Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Nm
    ,n.Tbl_Nm
    ,n.User_ID
    ,n.Max_Pros_Key_I
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Job_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Job_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Job
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Job_Hist
  (
     Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDS_Cand_Obj
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Cand_Obj_Hist
  (
     Cand_Id
    ,Trtmnt_Cd
    ,TDM_Database_Nm
    ,TDM_Obj_Nm
    ,TDM_Obj_Type
    ,TDM_Copy_Stat
    ,TDM_Compare_DDL
    ,TDM_Where_Clause
    ,TDM_Key_Column
    ,TDM_Query_Band_Str
    ,TDM_Job_Desc
    ,TDM_Staging_Database_Nm
    ,TDM_Job_Cd
    ,Force_Flag
    ,Tbl_Size
    ,Job_Cd
    ,UOWId
    ,num_Tbl
    ,running_flag
    ,failed_flag
    ,exists_flag
    ,Tbl_flag
    ,changed_flag
    ,Rule_No
    ,Limit_Desc
    ,dedupe_cd
    ,Enabled_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Id
    ,o.Trtmnt_Cd
    ,o.TDM_Database_Nm
    ,o.TDM_Obj_Nm
    ,o.TDM_Obj_Type
    ,o.TDM_Copy_Stat
    ,o.TDM_Compare_DDL
    ,o.TDM_Where_Clause
    ,o.TDM_Key_Column
    ,o.TDM_Query_Band_Str
    ,o.TDM_Job_Desc
    ,o.TDM_Staging_Database_Nm
    ,o.TDM_Job_Cd
    ,o.Force_Flag
    ,o.Tbl_Size
    ,o.Job_Cd
    ,o.UOWId
    ,o.num_Tbl
    ,o.running_flag
    ,o.failed_flag
    ,o.exists_flag
    ,o.Tbl_flag
    ,o.changed_flag
    ,o.Rule_No
    ,o.Limit_Desc
    ,o.dedupe_cd
    ,o.Enabled_Flag
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_Hist
  (
     ExtJob_Name
    ,ExtJob_RunNo
    ,ExtJob_Type
    ,ExtJob_Stat
    ,ExtJob_Beg_Ts
    ,ExtJob_End_Ts
    ,ExtJob_Pri
    ,Prev_ExtJob_End_Ts
    ,ExtJob_ErrNo
    ,ExtJob_ErrMsg
    ,Prg_Stat_Cd
    ,Prg_Stat_Desc
    ,ExtSys_Name
    ,ExtSys_Mode
    ,ExtSys_Capture_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.ExtJob_Name
    ,n.ExtJob_RunNo
    ,n.ExtJob_Type
    ,n.ExtJob_Stat
    ,n.ExtJob_Beg_Ts
    ,n.ExtJob_End_Ts
    ,n.ExtJob_Pri
    ,n.Prev_ExtJob_End_Ts
    ,n.ExtJob_ErrNo
    ,n.ExtJob_ErrMsg
    ,n.Prg_Stat_Cd
    ,n.Prg_Stat_Desc
    ,n.ExtSys_Name
    ,n.ExtSys_Mode
    ,n.ExtSys_Capture_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_DM_Stus_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_DM_Stus_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDM_DM_Stus
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_DM_Stus_Hist
  (
     ResourceId
    ,UOWId
    ,Job_CD
    ,Job_Stus_Cd
    ,Start_TS
    ,End_TS
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.ResourceId
    ,n.UOWId
    ,n.Job_CD
    ,n.Job_Stus_Cd
    ,n.Start_TS
    ,n.End_TS
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Id_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Id_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDM_Job_Id
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Id_Hist
  (
     Job_Id
    ,Job_Stus_Cd
    ,Job_Start_Ts
    ,Job_End_Ts
    ,Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Job_Stus_Cd
    ,n.Job_Start_Ts
    ,n.Job_End_Ts
    ,n.Job_Cd
    ,n.UOW_Id
    ,n.Job_Priority
    ,n.Job_Type
    ,n.Job_Source
    ,n.Job_Description
    ,n.Source_TDPID
    ,n.Source_User
    ,n.Source_Passwd_Encrypted
    ,n.Source_Logon_Mechanism
    ,n.Source_Logon_Mechanism_Data
    ,n.Source_Userid_Pool
    ,n.Source_Account_Id
    ,n.Source_Sessions
    ,n.Target_TDPID
    ,n.Target_User
    ,n.Target_Passwd_Encrypted
    ,n.Target_Logon_Mechanism
    ,n.Target_Logon_Mechanism_Data
    ,n.Target_Userid_Pool
    ,n.Target_Account_Id
    ,n.Target_Sessions
    ,n.Data_Streams
    ,n.Response_Timeout
    ,n.Overwrite_Existing_Obj
    ,n.Force_Utility
    ,n.Log_Level
    ,n.Online_Archive
    ,n.Freeze_Job_Steps
    ,n.Max_Agents_Per_Task
    ,n.Additional_ARC_Parameters
    ,n.Query_Band_Str
    ,n.Sync_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'U'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDM_DM_Stus_ADT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDM_DM_Stus_ADT
  AFTER DELETE ON V_O01_D_TDS_001_STD_0.TDM_DM_Stus
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDM_DM_Stus_Hist
  (
     ResourceId
    ,UOWId
    ,Job_CD
    ,Job_Stus_Cd
    ,Start_TS
    ,End_TS
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.ResourceId
    ,o.UOWId
    ,o.Job_CD
    ,o.Job_Stus_Cd
    ,o.Start_TS
    ,o.End_TS
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Stream_Obj_Reln_Hist
  (
     Stream_Name
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Name
    ,n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> V_O01_D_TDS_001_STD_0.TDS_Obj_Rule_AUT </sc-trigger> */
REPLACE TRIGGER V_O01_D_TDS_001_STD_0.TDS_Obj_Rule_AUT
  AFTER UPDATE ON V_O01_D_TDS_001_STD_0.TDS_Obj_Rule
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO V_O01_D_TDS_001_STD_0.TDS_Obj_Rule_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Enabled_Flag
    ,Repl_Type_Cd
    ,Predicate_Clause_Text
    ,Force_Flag
    ,Key_Column
    ,Obj_Size_Type_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Enabled_Flag
    ,n.Repl_Type_Cd
    ,n.Predicate_Clause_Text
    ,n.Force_Flag
    ,n.Key_Column
    ,n.Obj_Size_Type_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_X_Prcs_TCF_Cands_Hist
  (
     Cand_Id
    ,Cand_Trtmnt_Cd
    ,Job_Cd
    ,Job_Stus_Cd
    ,Created_Ts
    ,TargetTable
    ,Business_Date
    ,Business_Date_Cycle_Start_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Id
    ,o.Cand_Trtmnt_Cd
    ,o.Job_Cd
    ,o.Job_Stus_Cd
    ,o.Created_Ts
    ,o.TargetTable
    ,o.Business_Date
    ,o.Business_Date_Cycle_Start_Ts
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,o.Update_User
    ,o.Update_Date
    ,o.Update_Ts
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_Autosys_Snap_Hist
  (
     ExtJob_Name
    ,ExtJob_RunNo
    ,ExtJob_Type
    ,ExtJob_Stat
    ,ExtJob_Beg_Ts
    ,ExtJob_End_Ts
    ,ExtJob_Pri
    ,Prev_ExtJob_End_Ts
    ,ExtJob_ErrNo
    ,ExtJob_ErrMsg
    ,Prg_Stat_Cd
    ,Prg_Stat_Desc
    ,ExtSys_Name
    ,ExtSys_Mode
    ,ExtSys_Capture_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.ExtJob_Name
    ,o.ExtJob_RunNo
    ,o.ExtJob_Type
    ,o.ExtJob_Stat
    ,o.ExtJob_Beg_Ts
    ,o.ExtJob_End_Ts
    ,o.ExtJob_Pri
    ,o.Prev_ExtJob_End_Ts
    ,o.ExtJob_ErrNo
    ,o.ExtJob_ErrMsg
    ,o.Prg_Stat_Cd
    ,o.Prg_Stat_Desc
    ,o.ExtSys_Name
    ,o.ExtSys_Mode
    ,o.ExtSys_Capture_Ts
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_PI_Max_Key_Hist
  (
     Db_Nm
    ,Tbl_Nm
    ,User_ID
    ,Max_Pros_Key_I
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Nm
    ,n.Tbl_Nm
    ,n.User_ID
    ,n.Max_Pros_Key_I
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_JA_Stat_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_JA_Stat_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_JA_Stat
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_JA_Stat_Hist
  (
     Submit_TS
    ,No_Cands
    ,No_Obj
    ,No_MB
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Submit_TS
    ,o.No_Cands
    ,o.No_Obj
    ,o.No_MB
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Scope_Stream_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Scope_Stream_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Scope_Stream
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Scope_Stream_Hist
  (
     Stream_Name
    ,Stream_Type
    ,Job_Name
    ,Enabled_Flag
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Name
    ,n.Stream_Type
    ,n.Job_Name
    ,n.Enabled_Flag
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Start_Ts
    ,n.End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Data_Purge_Log_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Data_Purge_Log_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Data_Purge_Log
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Data_Purge_Log_Hist
  (
     Purge_Db
    ,Purge_Tbl
    ,Log_Date
    ,Start_Time
    ,Stop_Time
    ,Rows_Returned
    ,Status
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Purge_Db
    ,o.Purge_Tbl
    ,o.Log_Date
    ,o.Start_Time
    ,o.Stop_Time
    ,o.Rows_Returned
    ,o.Status
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Rule_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Rule_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Obj_Rule
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Rule_Hist
  (
     Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Enabled_Flag
    ,Repl_Type_Cd
    ,Predicate_Clause_Text
    ,Force_Flag
    ,Key_Column
    ,Obj_Size_Type_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Enabled_Flag
    ,n.Repl_Type_Cd
    ,n.Predicate_Clause_Text
    ,n.Force_Flag
    ,n.Key_Column
    ,n.Obj_Size_Type_Cd
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Id_Obj_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Id_Obj_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDM_Job_Id_Obj
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Id_Obj_Hist
  (
     Job_Id
    ,Db_Nm
    ,Obj_Nm
    ,Obj_Type
    ,Copy_Stat
    ,Compare_DDL
    ,Where_Clause
    ,Key_Column
    ,Query_Band_Str
    ,Job_Desc
    ,Staging_Database_Nm
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Id
    ,n.Db_Nm
    ,n.Obj_Nm
    ,n.Obj_Type
    ,n.Copy_Stat
    ,n.Compare_DDL
    ,n.Where_Clause
    ,n.Key_Column
    ,n.Query_Band_Str
    ,n.Job_Desc
    ,n.Staging_Database_Nm
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_Perm_Space_Hist
  (
     LogDate
    ,PermSpace
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.LogDate
    ,n.PermSpace
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_Step_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_Step_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDM_Job_Step
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Step_Hist
  (
     Job_Cd
    ,Step_No
    ,Step_Desc
    ,Step_Type
    ,Executable_Nm
    ,Executable_Db
    ,Executable_Type
    ,Executable_Param
    ,Executable_User
    ,Executable_Platform_Name
    ,Executable_BT_ET_Flag
    ,Enabled_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     n.Job_Cd
    ,n.Step_No
    ,n.Step_Desc
    ,n.Step_Type
    ,n.Executable_Nm
    ,n.Executable_Db
    ,n.Executable_Type
    ,n.Executable_Param
    ,n.Executable_User
    ,n.Executable_Platform_Name
    ,n.Executable_BT_ET_Flag
    ,n.Enabled_Flag
    ,n.Create_TS
    ,n.Create_User
    ,n.Update_TS
    ,n.Update_User
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Size_Type_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Size_Type_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Obj_Size_Type
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Size_Type_Hist
  (
     Obj_Size_Type_Cd
    ,Obj_Size_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Obj_Size_Type_Cd
    ,o.Obj_Size_Type_Desc
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Alrt_Rqst_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Alrt_Rqst_AIT
AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Alrt_Rqst
REFERENCING NEW ROW AS n
FOR EACH ROW  (
INSERT INTO DBCMNGR.AlertRequest (
     ReqDate
    ,ReqTime
    ,JobName
    ,Description
    ,EventValue
    ,ActionCode
    ,RepeatPeriod
    ,Destination
    ,Message
  ) VALUES (
     n.ReqDate
    ,n.ReqTime
    ,n.JobName
    ,n.Description
    ,n.EventValue
    ,n.ActionCode
    ,n.RepeatPeriod
    ,n.Destination
    ,trim(n.Destination) || '_' || trim(n.Message)
  );
);

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Data_Rtntn_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Data_Rtntn_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Data_Rtntn
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Data_Rtntn_Hist
  (
     DB_Name
    ,Tbl_Name
    ,Col_Name
    ,Ret_Period
    ,Impacted_Obj_DB_Name
    ,Impacted_Obj_Name
    ,Is_Enabled
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DB_Name
    ,n.Tbl_Name
    ,n.Col_Name
    ,n.Ret_Period
    ,n.Impacted_Obj_DB_Name
    ,n.Impacted_Obj_Name
    ,n.Is_Enabled
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_Hist
  (
     Cand_Obj_Source_Type_Cd
    ,Cand_Obj_Source_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Source_Type_Cd
    ,n.Cand_Obj_Source_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Size_Type_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Size_Type_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Obj_Size_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Size_Type_Hist
  (
     Obj_Size_Type_Cd
    ,Obj_Size_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Obj_Size_Type_Cd
    ,n.Obj_Size_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Metric_Sql_Cfg_Hist
  (
     Obj
    ,Is_Current_Flag
    ,Is_Enabled_Flag
    ,Db_Name
    ,Obj_Name
    ,Obj_Type
    ,Key_Type
    ,Key_Name
    ,Key_Sql
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Obj
    ,n.Is_Current_Flag
    ,n.Is_Enabled_Flag
    ,n.Db_Name
    ,n.Obj_Name
    ,n.Obj_Type
    ,n.Key_Type
    ,n.Key_Name
    ,n.Key_Sql
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Stat_Reln_Hist
  (
     Cand_Obj_Id
    ,Related_Cand_Obj_Id
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Id
    ,n.Related_Cand_Obj_Id
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_Hist
  (
     Log_Date
    ,Log_TS
    ,The_Count
    ,DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Gen_RI_SQL_Text
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Log_Date
    ,n.Log_TS
    ,n.The_Count
    ,n.DB_Name
    ,n.Obj_Name
    ,n.Col_1_Name
    ,n.Col_2_Name
    ,n.Join_Col_1_Name
    ,n.Join_Col_2_Name
    ,n.Predicate_Clause_Text
    ,n.Join_Type
    ,n.Rltd_DB_Name
    ,n.Rltd_Obj_Name
    ,n.Rltd_Col_1_Name
    ,n.Rltd_Col_2_Name
    ,n.Rltd_Join_Col_1_Name
    ,n.Rltd_Join_Col_2_Name
    ,n.Rltd_Predicate_Clause_Text
    ,n.Gen_RI_SQL_Text
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Stream_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Stream_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Cand_Stream
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Stream_Hist
  (
     Stream_Id
    ,Stream_Name
    ,Stream_RunNo
    ,Stream_Type
    ,Job_Name
    ,Job_Stat
    ,Allow_Cnsldtn_Flag
    ,Allow_Partial_Flag
    ,Start_Ts
    ,End_Ts
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Stream_Id
    ,n.Stream_Name
    ,n.Stream_RunNo
    ,n.Stream_Type
    ,n.Job_Name
    ,n.Job_Stat
    ,n.Allow_Cnsldtn_Flag
    ,n.Allow_Partial_Flag
    ,n.Start_Ts
    ,n.End_Ts
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_ExtJob_AutosysD_Log_Hist
  (
     Log_Id
    ,Prog_Host
    ,Prog_Name
    ,Func_Name
    ,Proc_Id
    ,Mesg_Type
    ,Mesg_Body
    ,Prog_Stat_Cd
    ,Prog_Stat_Desc
    ,Prog_Mode
    ,Prog_Vers
    ,Send_Alert_Flag
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Log_Id
    ,n.Prog_Host
    ,n.Prog_Name
    ,n.Func_Name
    ,n.Proc_Id
    ,n.Mesg_Type
    ,n.Mesg_Body
    ,n.Prog_Stat_Cd
    ,n.Prog_Stat_Desc
    ,n.Prog_Mode
    ,n.Prog_Vers
    ,n.Send_Alert_Flag
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDM_Job_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDM_Job_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDM_Job
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDM_Job_Hist
  (
     Job_Cd
    ,UOW_Id
    ,Job_Priority
    ,Job_Type
    ,Job_Source
    ,Job_Description
    ,Source_TDPID
    ,Source_User
    ,Source_Passwd_Encrypted
    ,Source_Logon_Mechanism
    ,Source_Logon_Mechanism_Data
    ,Source_Userid_Pool
    ,Source_Account_Id
    ,Source_Sessions
    ,Target_TDPID
    ,Target_User
    ,Target_Passwd_Encrypted
    ,Target_Logon_Mechanism
    ,Target_Logon_Mechanism_Data
    ,Target_Userid_Pool
    ,Target_Account_Id
    ,Target_Sessions
    ,Data_Streams
    ,Response_Timeout
    ,Overwrite_Existing_Obj
    ,Force_Utility
    ,Log_Level
    ,Online_Archive
    ,Freeze_Job_Steps
    ,Max_Agents_Per_Task
    ,Additional_ARC_Parameters
    ,Query_Band_Str
    ,Sync_Flag
    ,Create_TS
    ,Create_User
    ,Update_TS
    ,Update_User
    ,Modify_Action
   ) VALUES (
     o.Job_Cd
    ,o.UOW_Id
    ,o.Job_Priority
    ,o.Job_Type
    ,o.Job_Source
    ,o.Job_Description
    ,o.Source_TDPID
    ,o.Source_User
    ,o.Source_Passwd_Encrypted
    ,o.Source_Logon_Mechanism
    ,o.Source_Logon_Mechanism_Data
    ,o.Source_Userid_Pool
    ,o.Source_Account_Id
    ,o.Source_Sessions
    ,o.Target_TDPID
    ,o.Target_User
    ,o.Target_Passwd_Encrypted
    ,o.Target_Logon_Mechanism
    ,o.Target_Logon_Mechanism_Data
    ,o.Target_Userid_Pool
    ,o.Target_Account_Id
    ,o.Target_Sessions
    ,o.Data_Streams
    ,o.Response_Timeout
    ,o.Overwrite_Existing_Obj
    ,o.Force_Utility
    ,o.Log_Level
    ,o.Online_Archive
    ,o.Freeze_Job_Steps
    ,o.Max_Agents_Per_Task
    ,o.Additional_ARC_Parameters
    ,o.Query_Band_Str
    ,o.Sync_Flag
    ,o.Create_TS
    ,o.Create_User
    ,CURRENT_TIMESTAMP(6)
    ,CURRENT_USER
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_Src_Type_Hist
  (
     Cand_Obj_Source_Type_Cd
    ,Cand_Obj_Source_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Cand_Obj_Source_Type_Cd
    ,n.Cand_Obj_Source_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg_AUT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg_AUT
  AFTER UPDATE ON P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.DBQLXMLPlan_PDCR_RP_Chg_Hist
  (
     DBName
    ,TBName
    ,ColName
    ,PPI_Status
    ,PPI_START_DATE
    ,PPI_END_DATE
    ,ADD_START_DATE
    ,ADD_END_DATE
    ,DROP_START_DATE
    ,DROP_END_DATE
    ,Ret_Period
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.DBName
    ,n.TBName
    ,n.ColName
    ,n.PPI_Status
    ,n.PPI_START_DATE
    ,n.PPI_END_DATE
    ,n.ADD_START_DATE
    ,n.ADD_END_DATE
    ,n.DROP_START_DATE
    ,n.DROP_END_DATE
    ,n.Ret_Period
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'U'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Cand_Obj_TDM_Job_Reln_Hist
  (
     Cand_Id
    ,Job_Cd
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Cand_Id
    ,o.Job_Cd
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_ADT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_ADT
  AFTER DELETE ON P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log
  REFERENCING OLD ROW AS o FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_HC_RI_Chk_Log_Hist
  (
     Log_Date
    ,Log_TS
    ,The_Count
    ,DB_Name
    ,Obj_Name
    ,Col_1_Name
    ,Col_2_Name
    ,Join_Col_1_Name
    ,Join_Col_2_Name
    ,Predicate_Clause_Text
    ,Join_Type
    ,Rltd_DB_Name
    ,Rltd_Obj_Name
    ,Rltd_Col_1_Name
    ,Rltd_Col_2_Name
    ,Rltd_Join_Col_1_Name
    ,Rltd_Join_Col_2_Name
    ,Rltd_Predicate_Clause_Text
    ,Gen_RI_SQL_Text
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     o.Log_Date
    ,o.Log_TS
    ,o.The_Count
    ,o.DB_Name
    ,o.Obj_Name
    ,o.Col_1_Name
    ,o.Col_2_Name
    ,o.Join_Col_1_Name
    ,o.Join_Col_2_Name
    ,o.Predicate_Clause_Text
    ,o.Join_Type
    ,o.Rltd_DB_Name
    ,o.Rltd_Obj_Name
    ,o.Rltd_Col_1_Name
    ,o.Rltd_Col_2_Name
    ,o.Rltd_Join_Col_1_Name
    ,o.Rltd_Join_Col_2_Name
    ,o.Rltd_Predicate_Clause_Text
    ,o.Gen_RI_SQL_Text
    ,o.Create_User
    ,o.Create_Date
    ,o.Create_Ts
    ,CURRENT_USER
    ,CURRENT_DATE
    ,CURRENT_TIMESTAMP(6)
    ,'D'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_Obj_Size_Type_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_Obj_Size_Type_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_Obj_Size_Type
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_Obj_Size_Type_Hist
  (
     Obj_Size_Type_Cd
    ,Obj_Size_Type_Desc
    ,Create_User
    ,Create_Date
    ,Create_Ts
    ,Update_User
    ,Update_Date
    ,Update_Ts
    ,Modify_Action
   ) VALUES (
     n.Obj_Size_Type_Cd
    ,n.Obj_Size_Type_Desc
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_Ts
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_Ts
    ,'I'
   );

--
/* <sc-trigger> P_D_TDS_001_STD_0.TDS_SQLState_Class_AIT </sc-trigger> */
REPLACE TRIGGER P_D_TDS_001_STD_0.TDS_SQLState_Class_AIT
  AFTER INSERT ON P_D_TDS_001_STD_0.TDS_SQLState_Class
  REFERENCING NEW ROW AS n FOR EACH ROW
  INSERT INTO P_D_TDS_001_STD_0.TDS_SQLState_Class_Hist
  (
     ClassCode
    ,ClassDefinition
    ,ClassNote
    ,Create_User
    ,Create_Date
    ,Create_TS
    ,Update_User
    ,Update_Date
    ,Update_TS
    ,Modify_Action
   ) VALUES (
     n.ClassCode
    ,n.ClassDefinition
    ,n.ClassNote
    ,n.Create_User
    ,n.Create_Date
    ,n.Create_TS
    ,n.Update_User
    ,n.Update_Date
    ,n.Update_TS
    ,'I'
   );

