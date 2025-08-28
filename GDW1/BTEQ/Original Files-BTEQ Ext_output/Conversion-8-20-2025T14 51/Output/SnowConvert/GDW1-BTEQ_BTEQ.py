#*** Generated code is based on the SnowConvert Python Helpers version 2.0.6 ***
 
import os
import sys
import snowconvert.helpers
from snowconvert.helpers import Export
from snowconvert.helpers import exec
from snowconvert.helpers import BeginLoading
con = None
def main():
  snowconvert.helpers.configure_log()
  con = snowconvert.helpers.log_on()
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '1' COLUMN '1' OF THE SOURCE CODE STARTING AT '{'. EXPECTED 'STATEMENT' GRAMMAR. **
  #--{
  #--    "name": "GDW1-BTEQ",
  #--    "guid": "a6c37c49-8600-4903-b05a-65e9c6d1619b",
  #--    "inputPath": "/Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/Original Files",
  #--    "outputPath": "/Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/SnowConvertRun2",
  #--    "projectInputPath": "",
  #--    "repointingPath": "",
  #--    "replatformingPath": "",
  #--    "connectionMappingsPath": "/var/folders/bj/vljj79ls735fp2mnpf5hh92c0000gn/T/snowconvert-connection-mappings/a6c37c49-8600-4903-b05a-65e9c6d1619b/connection-mappings.csv",
  #--    "migrationContextPath": "",
  #--    "platform": "SnowConvert",
  #--    "accesscode": "47468afc-7dc3-4145-9326-f0da6bfff91f",
  #--    "assessmentAccesscode": "",
  #--    "isDemo": false,
  #--    "steps": [
  #--        {
  #--            "name": "Project Creation",
  #--            "status": 1,
  #--            "active": false,
  #--            "disabled": false,
  #--            "steps": []
  #--        },
  #--        {
  #--            "name": "Extraction",
  #--            "status": 1,
  #--            "active": false,
  #--            "disabled": false,
  #--            "steps": []
  #--        },
  #--        {
  #--            "name": "Conversion",
  #--            "status": 1,
  #--            "active": true,
  #--            "disabled": false,
  #--            "steps": [
  #--                {
  #--                    "name": "Execution",
  #--                    "status": 1,
  #--                    "disabled": false,
  #--                    "active": true,
  #--                    "expandOptions": {
  #--                        "isExpandable": true,
  #--                        "expand": true
  #--                    }
  #--                },
  #--                {
  #--                    "name": "Results",
  #--                    "status": 0,
  #--                    "disabled": false,
  #--                    "active": false,
  #--                    "expandOptions": {
  #--                        "isExpandable": true,
  #--                        "expand": true
  #--                    }
  #--                }
  #--            ]
  #--        },
  #--        {
  #--            "name": "Deployment",
  #--            "status": 0,
  #--            "active": false,
  #--            "disabled": true,
  #--            "steps": []
  #--        },
  #--        {
  #--            "name": "Data Migration",
  #--            "status": 0,
  #--            "active": false,
  #--            "disabled": true,
  #--            "steps": [
  #--                {
  #--                    "name": "Execution",
  #--                    "status": 0,
  #--                    "disabled": true,
  #--                    "active": false
  #--                },
  #--                {
  #--                    "name": "Results",
  #--                    "status": 0,
  #--                    "disabled": true,
  #--                    "active": true
  #--                }
  #--            ]
  #--        },
  #--        {
  #--            "name": "Data Validation",
  #--            "status": 0,
  #--            "active": false,
  #--            "disabled": true,
  #--            "preview": true,
  #--            "steps": [
  #--                {
  #--                    "name": "Execution",
  #--                    "status": 0,
  #--                    "disabled": true,
  #--                    "active": false
  #--                },
  #--                {
  #--                    "name": "Results",
  #--                    "status": 0,
  #--                    "disabled": true,
  #--                    "active": true
  #--                }
  #--            ]
  #--        }
  #--    ],
  #--    "conversionSettings": {
  #--        "sourcePlatform": "teradata",
  #--        "conversionOptions": {
  #--            "commentIfMissingDependencies": false,
  #--            "generateStoredProcedureTags": false,
  #--            "splitPeriodDataType": false,
  #--            "disableTopologicalLevelReorder": false,
  #--            "useCollateForCaseSpecification": false,
  #--            "encoding": "65001",
  #--            "sessionMode": "Tera",
  #--            "customObjectName": "default",
  #--            "useExistingNameQualification": false,
  #--            "arrangeOption": {
  #--                "arrange": false
  #--            },
  #--            "characterToApproximateNumber": 10,
  #--            "defaultTimestampFormat": "YYYY/MM/DD HH:MI:SS",
  #--            "defaultDateFormat": "YYYY/MM/DD",
  #--            "defaultTimezoneFormat": "GMT-5",
  #--            "defaultTimeFormat": "HH:MI:SS",
  #--            "bteqTargetLanguage": "SnowScript",
  #--            "pLTargetLanguage": "SnowScript",
  #--            "targetLag": "TargetLag",
  #--            "targetLagNumber": "1",
  #--            "targetLagTime": "days",
  #--            "warehouse": "UPDATE_DUMMY_WAREHOUSE",
  #--            "conversionRateMode": "LoC"
  #--        }
  #--    },
  #--    "internalProcessInputPath": "",
  #--    "internalAssessmentOutputPath": "",
  #--    "internalConversionOutputPath": "/Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/SnowConvertRun2/Conversion-8-20-2025T11 36",
  #--    "customerData": {
  #--        "company": "Snowflake",
  #--        "email": "mazen.hindi@snowflake.com"
  #--    },
  #--    "conversionProgress": {
  #--        "isSuccessful": true
  #--    },
  #--    "customInputPaths": {
  #--        "assessment": "",
  #--        "conversion": ""
  #--    },
  #--    "projectRootPath": ""
  #--}
   
  snowconvert.helpers.quit_application()

if __name__ == "__main__":
  main()