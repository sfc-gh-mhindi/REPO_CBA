
CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_MIGRATE_TERADATA_TABLE("P_SOURCE_DATABASE_NAME" VARCHAR, "P_SOURCE_TABLE_NAME" VARCHAR, "P_TARGET_DATABASE_NAME" VARCHAR, "P_TARGET_SCHEMA_NAME" VARCHAR, "P_TARGET_TABLE_NAME" VARCHAR, "P_WITH_CHUNKING_YN" VARCHAR DEFAULT 'N', "P_CHUNKING_COLUMN" VARCHAR DEFAULT null, "P_CHUNKING_VALUE" VARCHAR DEFAULT null, "P_CHUNKING_DATA_TYPE" VARCHAR DEFAULT null, "P_SKIP_WAITING_FOR_MIGRATION_TASKS" VARCHAR DEFAULT 'N')
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS 
$$
DECLARE
    result_message STRING DEFAULT '';
    START_TIME              TIMESTAMP;
    MIGRATION_START_TIME    TIMESTAMP;
    running_count           INTEGER   DEFAULT 1;
    loop_counter            INTEGER DEFAULT 0;
    max_loops INTEGER       DEFAULT 1200;
    object_record_count     INTEGER DEFAULT 0;
    column_record_count     INTEGER DEFAULT 0;
    sync_json               STRING;
    checksum_json           STRING;
    target_table_count      INTEGER DEFAULT 0;
    target_tbl_sql          STRING;
    chunking_result         STRING DEFAULT '';
BEGIN
 
    result_message := '[' || CURRENT_TIMESTAMP()::STRING || '] Starting Teradata table migration procedure...\n';
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Source: ' || P_SOURCE_DATABASE_NAME || '.' || P_SOURCE_TABLE_NAME || '\n';
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Target: ' || P_TARGET_DATABASE_NAME || '.' || P_TARGET_SCHEMA_NAME || '.' || P_TARGET_TABLE_NAME || '\n';
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Chunking Enabled: ' || P_WITH_CHUNKING_YN || '\n';
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Skip Migration Task Waiting: ' || P_SKIP_WAITING_FOR_MIGRATION_TASKS || '\n';
   
        -- Validate P_SKIP_WAITING_FOR_MIGRATION_TASKS parameter
    IF (UPPER(P_SKIP_WAITING_FOR_MIGRATION_TASKS) NOT IN ('Y', 'N')) THEN
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] ERROR: P_SKIP_WAITING_FOR_MIGRATION_TASKS must be either Y or N. Provided value: ' || P_SKIP_WAITING_FOR_MIGRATION_TASKS || '\n';
        RETURN result_message;
    END IF;
    
    IF (P_WITH_CHUNKING_YN = 'Y') THEN
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Chunking Column: ' || COALESCE(P_CHUNKING_COLUMN, 'NULL') || '\n';
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Chunking Value: ' || COALESCE(P_CHUNKING_VALUE, 'NULL') || '\n';
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Chunking Data Type: ' || COALESCE(P_CHUNKING_DATA_TYPE, 'NULL') || '\n';
    END IF;
   
 
    result_message := '[' || CURRENT_TIMESTAMP()::STRING || '] Step 0: Checking if table has any existing records...\n';
    target_tbl_sql := 'SELECT COUNT(1) FROM "' || P_TARGET_DATABASE_NAME || '"."' || P_TARGET_SCHEMA_NAME || '"."' || P_TARGET_TABLE_NAME || '"';
   
    -- EXECUTE IMMEDIATE target_table_sql INTO target_table_count;
   
    -- EXECUTE IMMEDIATE 'SELECT COUNT(1) FROM ' || P_TARGET_DATABASE_NAME || '.' || P_TARGET_SCHEMA_NAME || '.' || P_TARGET_TABLE_NAME INTO target_table_count;
    LET count_result RESULTSET := (EXECUTE IMMEDIATE :target_tbl_sql);
    LET count_cursor CURSOR FOR count_result;
    OPEN count_cursor;
    FETCH count_cursor INTO target_table_count;
    CLOSE count_cursor;
   
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Target table has ' || target_table_count || ' rows...\n';
   
    IF (target_table_count > 0) THEN
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Going straight to migrating table\n';
    end if;
   
    IF (target_table_count = 0) THEN
   
        DELETE FROM DMVA_COLUMN_INFO WHERE COLUMN_ID IN (
            Select A.column_id
            from dmva_COLUMN_info A
                INNER JOIN DMVA_OBJECT_INFO B ON
                    A.OBJECT_ID = B.OBJECT_ID
            WHERE B.OBJECT_NAME = :P_SOURCE_TABLE_NAME
        );
        DELETE FROM DMVA_OBJECT_INFO WHERE OBJECT_NAME = :P_SOURCE_TABLE_NAME; --IN ('SANDPIT_DEMO_TBL001','SANDPIT_DEMO_TBL002');
       
       
       
        ///////////////////////////////////////////// Step 1 - Configure Mapping Rules /////////////////////////////////////////////
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 1: Configuring mapping rules...\n';
   
        DELETE FROM MIGRATION_TRACKING_V2.dmva_mapping_rules WHERE TARGET_OBJECT_NAME = :P_SOURCE_TABLE_NAME; -- IN ('SANDPIT_DEMO_TBL001','SANDPIT_DEMO_TBL002');
       
        INSERT INTO MIGRATION_TRACKING_V2.dmva_mapping_rules(
        mapping_rule_id,
        source_system_name,
        source_database_name,
        source_schema_name,
        source_object_name,
        target_system_name,
        target_database_name,
        target_schema_name,
        target_object_name,
        source_object_id,
        target_object_id,
        created_ts,
        updated_ts
        )
        SELECT
        NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.dmva_mapping_rule_id_seq.nextval mapping_rule_id,
        'teradata_source' source_system_name,
        'NA' source_database_name,
        :P_SOURCE_DATABASE_NAME source_schema_name,
        :P_SOURCE_TABLE_NAME source_object_name,
        'snowflake_target' target_system_name,
        :P_TARGET_DATABASE_NAME target_database_name,
        :P_TARGET_SCHEMA_NAME target_schema_name,
        :P_TARGET_TABLE_NAME target_object_name,
        NULL source_object_id,
        NULL target_object_id,
        current_timestamp() created_ts,
        current_timestamp() updated_ts;
       
        ///////////////////////////////////////////// Step 2 - Call Metadata Tasks /////////////////////////////////////////////
       
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 2: Starting metadata tasks...\n';
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] CALL: DMVA_GET_METADATA_TASKS(''teradata_source'', parse_json(''{"NA": {"' || :P_SOURCE_DATABASE_NAME || '": ["' || :P_SOURCE_TABLE_NAME || '"]}}'') )...\n';
        -- CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.DMVA_GET_METADATA_TASKS('teradata_source');
        CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.DMVA_GET_METADATA_TASKS('teradata_source', parse_json('{"NA": {"' || :P_SOURCE_DATABASE_NAME || '": ["' || :P_SOURCE_TABLE_NAME || '"]}}') );
        -- Capture start time for monitoring
        START_TIME := DATEADD('MINUTE',-1,sysdate());--CURRENT_TIMESTAMP();
       
        ///////////////////////////////////////////// Step 3 - Monitor Metadata Tasks /////////////////////////////////////////////
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 3: Monitoring metadata tasks...\n';
        -- Loop to monitor metadata tasks until completion
        running_count := 1;
        loop_counter := 0;
        max_loops := 1200; -- Maximum 1200 loops (100 minutes at 5 seconds each)
       
        WHILE (running_count > 0 AND loop_counter < max_loops) DO
       
            -- Check for running tasks
            SELECT COUNT(*)
            INTO running_count
            FROM dmva_tasks
            WHERE (status_cd = 'RUNNING' or status_cd is null)
            AND queue_ts >= :START_TIME;
           
            -- If still running, wait 2 seconds
            IF (running_count > 0) THEN
                result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Metadata Loop ' || loop_counter || ': Found ' || running_count || ' RUNNING tasks, waiting 1 seconds...\n';
                CALL SYSTEM$WAIT(1, 'SECONDS');
            END IF;
            loop_counter := loop_counter + 1;
        END WHILE;
       
        -- Final status report
        IF (running_count = 0) THEN
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] All metadata tasks completed successfully!\n';
        ELSE
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Maximum wait time reached for metadata tasks. Some tasks may still be running.\n';
        END IF;
       
        ///////////////////////////////////////////// Step 4 - Verify Metadata /////////////////////////////////////////////
       
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 4: Verifying metadata...\n';
       
        -- Verify object info was created
        SELECT COUNT(*) INTO object_record_count
        FROM dmva_object_info
        WHERE OBJECT_NAME = :P_SOURCE_TABLE_NAME;
       
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Object metadata count: ' || object_record_count || '\n';
       
        -- Stop execution if no object metadata found
        IF (object_record_count = 0) THEN
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] CRITICAL ERROR: No object metadata found for table ' || P_SOURCE_TABLE_NAME || '. Migration cannot proceed.\n';
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Please check if metadata tasks completed successfully and table exists in source system.\n';
            RETURN result_message;
        END IF;
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Object metadata validation: PASS\n';
       
        -- Verify column info was created
        SELECT COUNT(*) INTO column_record_count
        FROM dmva_COLUMN_info A
        INNER JOIN DMVA_OBJECT_INFO B ON A.OBJECT_ID = B.OBJECT_ID
        WHERE B.OBJECT_NAME = :P_SOURCE_TABLE_NAME;
       
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Column metadata count: ' || column_record_count || '\n';
       
        -- Stop execution if no column metadata found
        IF (column_record_count = 0) THEN
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] CRITICAL ERROR: No column metadata found for table ' || P_SOURCE_TABLE_NAME || '. Migration cannot proceed.\n';
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Please check if metadata tasks completed successfully and table structure is accessible.\n';
            RETURN result_message;
        END IF;
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Column metadata validation: PASS\n';
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 4 Validation PASSED: Metadata verification successful, proceeding with migration...\n';
        ///////////////////////////////////////////// Step 5 - Populate Metadata /////////////////////////////////////////////
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 5: Populating metadata...\n';
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] CALL: dmva_create_sync_tables(true,false,''teradata_source'', parse_json(''{"NA": [[ "' || :P_SOURCE_DATABASE_NAME || '", "' || :P_SOURCE_TABLE_NAME || '" ]]}''))...\n';
        
        CALL MIGRATION_TRACKING_V2.dmva_create_sync_tables(true,false,'teradata_source', parse_json('{"NA": {"' || :P_SOURCE_DATABASE_NAME || '": ["' || :P_SOURCE_TABLE_NAME || '"]}}'));
    END IF;  
        ///////////////////////////////////////////// Step 4.5 - Configure Chunking (Optional) /////////////////////////////////////////////
    
    IF (P_WITH_CHUNKING_YN = 'Y') THEN
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 4.5: Configuring chunking method...\n';
        
        -- Validate chunking parameters
        IF (P_CHUNKING_COLUMN IS NULL OR P_CHUNKING_VALUE IS NULL OR P_CHUNKING_DATA_TYPE IS NULL) THEN
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] ERROR: Chunking enabled but required parameters are missing:\n';
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] - chunking_column: ' || COALESCE(P_CHUNKING_COLUMN, 'NULL') || '\n';
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] - chunking_value: ' || COALESCE(P_CHUNKING_VALUE, 'NULL') || '\n';
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] - chunking_data_type: ' || COALESCE(P_CHUNKING_DATA_TYPE, 'NULL') || '\n';
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Please provide all required chunking parameters.\n ';
            RETURN result_message;
        END IF;
        
        -- Call the chunking procedure
        CALL P_SET_CHUNKING_KEY(:P_SOURCE_DATABASE_NAME,:P_SOURCE_TABLE_NAME,:P_CHUNKING_COLUMN,:P_CHUNKING_VALUE,:P_CHUNKING_DATA_TYPE)
        INTO chunking_result;
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Chunking configuration result:\n' || chunking_result || '\n';
        
        -- Check if chunking configuration was successful
        IF (chunking_result LIKE '%ERROR:%') THEN
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] CRITICAL ERROR: Chunking configuration failed. Migration cannot proceed.\n';
            RETURN result_message;
        END IF;
        
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 4.5 Completed: Chunking configuration successful, proceeding with migration...\n';
    ELSE
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 4.5: Skipping chunking configuration (P_WITH_CHUNKING_YN = N)...\n';
    END IF;
    ///////////////////////////////////////////// Step 6 - Run Migration /////////////////////////////////////////////
   
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 6: Starting migration tasks...\n';
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || 'DMVA_GET_CHECKSUM_TASKS(''teradata_source'', parse_json(''{"NA": [[ "' || :P_SOURCE_DATABASE_NAME || '", "' || :P_SOURCE_TABLE_NAME || '" ]]}''))\n';
    CALL MIGRATION_TRACKING_V2.DMVA_GET_CHECKSUM_TASKS('teradata_source', parse_json('{"NA": {"' || :P_SOURCE_DATABASE_NAME || '": ["' || :P_SOURCE_TABLE_NAME || '"]}}'));
   
    -- Capture start time for migration monitoring
    MIGRATION_START_TIME := DATEADD('MINUTE',-1,sysdate());--CURRENT_TIMESTAMP();
   
    ///////////////////////////////////////////// Step 7 - Monitor Migration Tasks /////////////////////////////////////////////
   
    IF (UPPER(P_SKIP_WAITING_FOR_MIGRATION_TASKS) = 'N') THEN
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 7: Monitoring migration tasks...\n';
        -- Loop to monitor migration tasks until completion
        running_count := 1;
        loop_counter := 0;
        max_loops := 2400; -- Maximum 2400 loops (200 minutes at 5 seconds each) for longer migration tasks
        WHILE (running_count > 0 AND loop_counter < max_loops) DO
       
            -- Check for running tasks specific to our table
            SELECT COUNT(*)
            INTO running_count
            FROM dmva_tasks A
            INNER JOIN DMVA_OBJECT_INFO B ON
            B.OBJECT_ID = A.SOURCE_OBJECT_ID
            AND B.OBJECT_NAME = :P_SOURCE_TABLE_NAME
            WHERE (A.status_cd = 'RUNNING' or A.status_cd IS NULL)
            AND A.queue_ts >= :MIGRATION_START_TIME;
           
            -- If still running, wait 2 seconds
            IF (running_count > 0) THEN
                result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Migration Loop ' || loop_counter || ': Found ' || running_count || ' RUNNING tasks for target table, waiting 2 seconds...\n';
                CALL SYSTEM$WAIT(2, 'SECONDS');
            END IF;
            loop_counter := loop_counter + 1;
        END WHILE;
       
        -- Final migration status report
        IF (running_count = 0) THEN
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] All migration tasks completed successfully!\n';
            -- result_message := result_message || 'loop counter:' || loop_counter || '\n';
            -- result_message := result_message || 'running count:' || running_count || '\n';
            -- result_message := result_message || 'started at:' || MIGRATION_START_TIME || '\n';
        ELSE
            result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Maximum wait time reached for migration tasks. Some tasks may still be running.\n';
        END IF;
    ELSE
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 7: Skipping migration task monitoring (P_SKIP_WAITING_FOR_MIGRATION_TASKS = Y)...\n';
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Migration tasks have been initiated but monitoring is skipped as requested.\n';
    END IF;
   
    ///////////////////////////////////////////// Step 8 - Final Verification /////////////////////////////////////////////
   
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 8: Migration procedure completed!\n';
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Please verify data in table: ' || P_TARGET_DATABASE_NAME || '.' || P_TARGET_SCHEMA_NAME || '.' || P_TARGET_TABLE_NAME || '\n';
    RETURN result_message;
   
    EXCEPTION
    WHEN OTHER THEN
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] ERROR: ' || SQLERRM || '\n';
    RETURN result_message;
END;
$$;
