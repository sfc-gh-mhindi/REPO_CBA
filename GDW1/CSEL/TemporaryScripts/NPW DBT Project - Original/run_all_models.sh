#!/bin/bash

# CBA NPW-DBT Complete Pipeline Execution
echo "======================================================"
echo "Running Complete CBA NPW-DBT Pipeline"
echo "======================================================"

# Set variables
VARS='{
  "stg_ctl_db": "NPD_D12_DMN_GDWMIG",
  "job_params": {
    "processrunstreamstatuscheck": {
      "ctl_schema": "TMP",
      "run_stream": "CSE_ICE_BUS_DEAL",
      "app_release": "CSEL4",
      "etl_process_dt": "20240101"
    },
    "ldtmp_appt_deptrmxfm": {
      "base_dir": "cba_app__CSEL4__CSEL4DEV",
      "run_stream": "CSE_COM_BUS_CCL_CHL_COM_APP",
      "gdw_stag_db": "PDDSTG",
      "tgt_table": "TMP_APPT_DEPT"
    },
    "dltappt_deptfrmtmp_appt_dept": {
      "base_dir": "cba_app__CSEL4__CSEL4DEV",
      "tgt_table": "appt_dept",
      "gdw_stag_db": "PDDSTG",
      "run_stream": "CSE_CCC_BUS_APP_PROD"
    },
    "ldapptdeptupd": {
      "base_dir": "cba_app__CSEL4__CSEL4DEV",
      "run_stream": "CSE_CCC_BUS_APP_PROD",
      "tgt_table_tbl": "appt_dept"
    }
  }
}'

echo "Using variables: $VARS"
echo ""

# Run all models in dependency order with single command
echo "Running all models in dependency order..."
dbt run --select \
  models/cse_dataload/24processmetadata/processrunstreamstatuscheck/ \
  models/cse_dataload/08extraction/cplbusapp/extpl_app/ \
  models/cse_dataload/12MappingTransformation/CplBusApp/xfmpl_appfrmext/ \
  models/cse_dataload/14loadtotemp/ldtmp_appt_deptrmxfm/ \
  models/cse_dataload/16transformdelta/dltappt_deptfrmtmp_appt_dept/ \
  models/cse_dataload/18loadtogdw/appt_dept/ldapptdeptupd/ \
  --vars "$VARS"

echo "======================================================"
echo "✅ Pipeline execution completed!"
echo "======================================================"
