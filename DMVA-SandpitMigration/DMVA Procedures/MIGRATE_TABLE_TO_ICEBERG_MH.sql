CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG.ICEBERG_MIGRATOR.MIGRATE_TABLE_TO_ICEBERG_MH("TABLE_INSTANCE_ID" FLOAT, "RUN_ID" FLOAT, "TABLE_CATALOG" VARCHAR, "TABLE_SCHEMA" VARCHAR, "TABLE_NAME" VARCHAR, "TARGET_TABLE_CATALOG" VARCHAR, "TARGET_TABLE_SCHEMA" VARCHAR, "TABLE_CONFIGURATION" VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	// ---------------------------------------------------------------------------------------------
	// Name: migrate_table_to_iceberg
	// --------------------------------------------------------------------------------------------- 
	// The procedure will migrate a single table to iceberg bases on the parameters passed in. 
	// ---------------------------------------------------------------------------------------------
	// Parameters: 
	//      table_instance_id (integer) 	The table instance id
	//      run_id (integer)            	The identifier for the run 
	//      table_catalog (varchar)     	Database where table to load is located  
	//      table_schema (varchar)      	Schema where table to load is located 
	//      table_name (varchar)        	Name of table to migrate 
	//      target_table_catalog (varchar)  Target database where table to load is located  
	//      target_table_schema (varchar)   Target schema where table to load is located 
	//      table_configuration (variant)	Additional table configuration information 
	//											cluster_key (text) -  String with clustering column list 
	// ---------------------------------------------------------------------------------------------
	// Returns: JSON (Variant)
	//      result:  	Succeeded = True/Failed = False
	//      message:   	Message returned by failed outside of the load
	// ---------------------------------------------------------------------------------------------
	// Date         Who         Description 
	// 07/15/2024   S Ramsey 	Initial version based on data generator roots 
	// 07/24/2024   S Ramsey    Add support for handling the truncations of timestamps to milliseconds
	// 09/16/2024   S Ramsey	Work through issues with quoted object names  
	// 09/19/2024   S Ramsey	Add support to transforming timezone 
	// 05/05/2025   S Ramsey    Added support to create the iceberg table in a new database/schema
    // 05/13/2025   S Ramsey    Fix some issues where code got corrupted 
	// 05/14/2025   S Ramsey    Add support for clustering target table
	// 05/23/2025   S Ramsey    Changes to support additional options for other catalogs, first 
    //                          will be for schema based catalog integration for CBA POC
	// 06/04/2025 	S Ramsey 	Fix issue where trying to create or replace on a database that is 
	//							catalog bound. 
	// 06/04/2025 	S Ramsey 	Added replacement to remove collation 
	// ---------------------------------------------------------------------------------------------
	// ----- Standard tag 
    var std_tag = {origin: ''sf_ps'', name: ''table_to_iceberg'', version:{major: 1, minor: 0}}

    // ----- Define return value
    var ret = {}
    ret[''result''] = true
    ret[''message''] = null
    ret[''debug_sql''] = null
	ret[''start_time''] = getCurrentTimestamp()

    var fk_tbl_cnt = 0
	var dbgstep = 0
    var sql_pk_drp_tbl = ""
	var errorModule = "Main"
	var sql_txt = ""

	// ---- Build out tables names 
	var v_tablename = `"${TABLE_CATALOG}"."${TABLE_SCHEMA}"."${TABLE_NAME}"`
	var v_replace =  (typeof TARGET_TABLE_CATALOG === ''undefined'') 
	if ( v_replace ) {
		var v_icename = `"${TABLE_CATALOG}"."${TABLE_SCHEMA}"."${TABLE_NAME}_${RUN_ID}_tmp"`
	}
	else {
		var v_icename = `"${TARGET_TABLE_CATALOG}"."${TARGET_TABLE_SCHEMA}"."${TABLE_NAME}"`
	}
	var v_oldname = `"${TABLE_CATALOG}"."${TABLE_SCHEMA}"."${TABLE_NAME}_${RUN_ID}_old"`


	// ---- Set the query tag so we can identify all the SQL executing in the procedure
    var parameters = {}
	parameters[''table_inst_id''] = TABLE_INSTANCE_ID
	parameters[''run_id''] = RUN_ID
    parameters[''db''] = TABLE_CATALOG
    parameters[''schema''] = TABLE_SCHEMA
    parameters[''target_db''] = TABLE_CATALOG
    parameters[''target_SSchema''] = TABLE_SCHEMA
    parameters[''table''] = TABLE_NAME
	parameters[''table_config''] = TABLE_CONFIGURATION
    parameters[''proc_name''] = arguments.callee.name
	ret[''parameters''] = parameters

    // ---- Do stuff 
    is_err = false;

    try 
    {
		// ---- Pull configuration from config table
        sql_txt = `SELECT * FROM iceberg_tool_config_vw`

        rs = snowflake.execute({sqlText: sql_txt})
        rs.next()
        settings = rs.getColumnValue(''SETTINGS'')   
        if (! (settings.core.timezone_conversion == ''CHAR'' || settings.core.timezone_conversion == ''NTZ'' )) {
            settings.core.timezone_conversion = ''NONE''
        }
        ret[''settings''] = settings

		// ---- Set the query tag so we can identify all the SQL executing in the procedure (Cannot alter session when running as owner)
		std_tag[''parameters''] = parameters
		std_tag[''settings''] = settings

		snowflake.execute({sqlText: `alter session set query_tag=''`+JSON.stringify(std_tag)+`'';`})

		// ---- Create row in table log
    	snowflake.execute({sqlText: `insert into migration_table_log (table_instance_id, run_id, state_code, log_time, log_message) values (:1, :2, ''RUNNING'', current_timestamp(), null)`, binds:[TABLE_INSTANCE_ID, RUN_ID]})

        // -- Validates the table is good to go and gets other table properties
		chk = checkValid()
	
		if ( !chk.result ) {
			throw new Error(chk.message);
		}

		// ---- Drop amd create iceberg table
		var stmt;
        try {
			sql_txt = `drop iceberg table if exists ${v_icename}`
			ret[''debug_sql''] = sql_txt
			stmt = snowflake.execute({sqlText: sql_txt});

            //throw new Error(`custom msg1: ` + stmt);

			sql_txt = createIcebergDDL(v_tablename, v_icename)
			ret[''debug_sql''] = sql_txt
			stmt = snowflake.execute({sqlText: sql_txt});
            
            //throw new Error(`custom msg2: ` + stmt);
		}  
		catch(err)
    	{
			throw new Error(`Failed to create table ${v_icename}` + err.message)
		}

        // -- Change the grants on iceberg table
        //clone_var = cloneTablePermissions(v_tablename, v_icename)
		//if ( clone_var != ''true'' ){
		//	throw(new Error(`Failed to set permissions: ` + clone_var))
		//}

		// -- Get list of columns in table 
		tbl_cols = getColumnList(v_tablename)
		//ret[''tbl_cols''] = tbl_cols;

		// ---- If provided cluster then add order by 
		order_by = '''' 
		if ( TABLE_CONFIGURATION !== null) {
			if ( TABLE_CONFIGURATION.cluster_key !== null) {
				order_by = `ORDER BY ${TABLE_CONFIGURATION.cluster_key}`	
			} 
		}

        // -- Insert data into iceberg table  
		sql_txt = `INSERT INTO ${v_icename} (${tbl_cols.insertList}) SELECT ${tbl_cols.selectList} FROM ${v_tablename} ${order_by}`
		ret[''debug_sql''] = sql_txt
		snowflake.execute({sqlText: sql_txt});

        // -- Validate the data in the table(s) match
		if ( settings.core.count_only_validation ){
			sql_txt = `
			WITH a AS 
			(
				SELECT COUNT(*) AS hag FROM ${v_icename}
				MINUS 
				SELECT COUNT(*) AS hag FROM ${v_tablename}
			)
			SELECT COUNT(*) AS cnt 
			FROM a`
		}
		else {
		sql_txt = `
			WITH a AS 
			(
				SELECT HASH_AGG(${tbl_cols.iceSelectList}) AS hag FROM ${v_icename}
				MINUS 
				SELECT HASH_AGG(${tbl_cols.selectList}) AS hag FROM ${v_tablename}
			)
			SELECT COUNT(*) AS cnt 
			FROM a`
		}
		ret[''debug_sql''] = sql_txt
		let v_cnt_rs = snowflake.execute({sqlText: sql_txt})
		v_cnt_rs.next()

        // -- If validation OK do the swap.  
		if (v_cnt_rs.CNT == 0) {
			if ( v_replace ) {
				// -- Rename normal table to some backup name 
				sql_txt = `ALTER TABLE ${v_tablename} RENAME TO ${v_oldname}`
				snowflake.execute({sqlText: sql_txt});

				// -- Rename new iceberg table to old table name 
				sql_txt = `ALTER ICEBERG TABLE ${v_icename} RENAME TO ${v_tablename}`
				snowflake.execute({sqlText: sql_txt});

				// -- Drop normal table
				sql_txt = `DROP TABLE ${v_oldname}`
				snowflake.execute({sqlText: sql_txt});
			}
		}
		else {
			throw(new Error(`Migration validation failed.  Total mismatched rows (${v_cnt_rs.CNT})`))
		}
    }
    catch(err) {
        is_err = true
		ret[''finish_time''] = getCurrentTimestamp()
		ret[''result''] = false
        ret[''message''] = err.message

        if (err.code === undefined) {
            ret[''error_code'']  = err.code 
            ret[''error_state''] = err.state
            ret[''error_stack''] = err.stackTraceTxt 
        }
    }

	// -- Cleanup if failed
	 try {
	 	if (is_err) {
	 		snowflake.execute({sqlText: `DROP ICEBERG TABLE IF EXISTS ${v_icename}`});
	 	}
	 }
     catch(err) {}

	ret[''finish_time''] = getCurrentTimestamp()

 	// ---- Update metadata tables as complete 
	if (ret[''result'']) {
		ret[''debug_sql''] = null;
		snowflake.execute({sqlText: `insert into migration_table_log (table_instance_id, run_id, state_code, log_time, log_message) values (:1, :2, ''COMPLETE'', current_timestamp(), null)`, binds:[TABLE_INSTANCE_ID, RUN_ID]})
	}
	else {
    	snowflake.execute({sqlText: `insert into migration_table_log (table_instance_id, run_id, state_code, log_time, log_message) values (:1, :2, ''FAILED'', current_timestamp(), :3)`, binds:[TABLE_INSTANCE_ID, RUN_ID, ret[''message'']]})
	}

	 // ---- Reset query tag (Cannot alter session when running as owner)
	 snowflake.execute({sqlText: "alter session unset query_tag;"});

	 return ret;


	// =============================================================================================
  	// Modular functions for simplification and reuse
	// =============================================================================================

    // ---------------------------------------------------------------------------------------------
    // Name: getCurrentTimestamp
    // --------------------------------------------------------------------------------------------- 
    // Returns current time 
    // ---------------------------------------------------------------------------------------------
	function getCurrentTimestamp()
	{
		let currTS = new Date()
		return currTS.getFullYear() + "-" + (currTS.getMonth() + 1).toString().padStart(2, ''0'') + "-" + currTS.getDate().toString().padStart(2, ''0'') + " " + currTS.getHours().toString().padStart(2, ''0'') + ":" + currTS.getMinutes().toString().padStart(2, ''0'') + ":" + currTS.getSeconds().toString().padStart(2, ''0'')
	}
	
    // ---------------------------------------------------------------------------------------------
    // Name: cloneTablePermissions
    // --------------------------------------------------------------------------------------------- 
	// Parameters: 
   	//      source_table (text) Source table to pull permissions from
	//      target_table (text) Target table to apply permissions on
    // ---------------------------------------------------------------------------------------------
    // Returns success/failure
    // ---------------------------------------------------------------------------------------------
	function cloneTablePermissions(source_table, target_table)
	{
    	var valRet = ''true'' 
		try 
		{
			// --- Get permissions from source table 
			let table_rs = snowflake.execute({sqlText: `SHOW GRANTS ON TABLE ${source_table}`})

			// --- Loop through results and grant to new table 
			while (table_rs.next())
			{
				sql_txt = `GRANT ${table_rs.privilege} ON TABLE ${target_table} TO ROLE ${table_rs.grantee_name}`
				snowflake.execute({sqlText: sql_txt});			
			}
		}
		catch(err) {
			valRet = err 
		}

		return valRet
	}

    // ---------------------------------------------------------------------------------------------
    // Name: getColumnList
    // --------------------------------------------------------------------------------------------- 
	// Parameters: 
   	//      source_table (text) Source table to pull list from
    // ---------------------------------------------------------------------------------------------
    // Returns: Collection with select list and insert list 
    // ---------------------------------------------------------------------------------------------
	function getColumnList(source_table)
	{
  		// --- Get permissions from source table 
    	var retList = {}
		retList[''insertList''] = ''''
		retList[''selectList''] = ''''
		retList[''iceSelectList''] = ''''

		// -- Validates the table exists and is not an iceberg table 
		let column_rs = snowflake.execute({sqlText: `DESCRIBE TABLE ${source_table}`})

		// --- Loop through results and grant to new table 
		i = 0
		while (column_rs.next())
		{
			// -- If not first row add comma 
			if ( i >= 1 ){
				retList[''insertList''] += '',''
				retList[''selectList''] += '',''
				retList[''iceSelectList''] += '',''
			}

			// -- Add name to insert list 
			retList[''insertList''] += `"${column_rs.name}"`

			// -- depending on data type transform data 
			regex = new RegExp(''TIMESTAMP_TZ.*'')
			if (regex.test(column_rs.type))  {
				if (settings.core.timezone_conversion==''NTZ'') {
					retList[''selectList''] += `CAST(CONVERT_TIMEZONE(''UTC'', CAST("${column_rs.name}" AS TIMESTAMP_TZ(6))) AS TIMESTAMP_NTZ(6))`
					retList[''iceSelectList''] += `CAST(CONVERT_TIMEZONE(''UTC'', CAST("${column_rs.name}" AS TIMESTAMP_TZ(6))) AS TIMESTAMP_NTZ(6))`
				}
				else if (settings.core.timezone_conversion==''CHAR'') {
					regex = /TIMESTAMP_TZ\\(([0-9])\\)/gi
					prec = column_rs.type.replaceAll(regex, ''$1'')
					
					if (prec != ''0'') { 
						retList[''selectList''] += `to_char("${column_rs.name}", ''YYYY-MM-DD HH24:MI:SS.FF${prec} TZHTZM'')`
					}
					else {
						retList[''selectList''] += `to_char("${column_rs.name}", ''YYYY-MM-DD HH24:MI:SS TZHTZM'')`
					}
					retList[''iceSelectList''] += `"${column_rs.name}"`
				} 
			}
			else if ( settings.core.truncate_time ){
				// -- If Time or Timestamp then round down to microseconds 
				regex = new RegExp(''TIME.*'')
				if (regex.test(column_rs.type)) {
					retList[''selectList''] += `date_trunc(''microseconds'', "${column_rs.name}")`
					retList[''iceSelectList''] += `date_trunc(''microseconds'', "${column_rs.name}")`
				}
				else {
					retList[''selectList''] += `"${column_rs.name}"`
					retList[''iceSelectList''] += `"${column_rs.name}"`
				}		
			} 
			else {
				retList[''selectList''] += `"${column_rs.name}"`
				retList[''iceSelectList''] += `"${column_rs.name}"`
			}
			i++
		}
		return retList
	}


    // ---------------------------------------------------------------------------------------------
    // Name: createIcebergDDL
    // ---------------------------------------------------------------------------------------------
	// Parameters: 
   	// 	    source_table (text) 			Source table to pull permissions from
	//     	target_table (text) 			Target iceberg table table to create 
    // ---------------------------------------------------------------------------------------------
    // Returns DDL to create iceberg table 
    // ---------------------------------------------------------------------------------------------
	function createIcebergDDL(source_table, target_table)
	{
		// ---- Get DDL for source table 
        var valRet = '''' 
		let table_rs = snowflake.execute({sqlText: `SELECT GET_DDL(''TABLE'',  ''${source_table}'', true) AS ddl`})
		table_rs.next()
		valRet = table_rs.DDL 

        // ------- Standard replacements 
		// -- Replace collation 
		regex = /COLLATE\\s*\\''\\S*?\\''/gi
		valRet = valRet.replaceAll(regex, '' '')

		// -- Replace VARCHAR
		regex = /VARCHAR\\(\\d*\\)/gi
		valRet = valRet.replaceAll(regex, ''STRING'')

		// -- Replace float 
		regex = /( )FLOAT([ ,])/gi
		valRet = valRet.replaceAll(regex, ''$1double$2'')	

		// -- Replace timestamp_tz based on definitions 
		regex = /(TIMESTAMP_TZ.*)\\(([0-9])\\)/gi
		if (settings.core.timezone_conversion==''NTZ'') {
			valRet = valRet.replaceAll(regex, ''TIMESTAMP_NTZ(6)'')
		}
		else if (settings.core.timezone_conversion==''CHAR'') {
			valRet = valRet.replaceAll(regex, ''STRING'')
		} 
		
		// -- If time is truncated then replace time precision 
		if ( settings.core.truncate_time ){
			regex = /(TIME.*)\\([0-9]\\)/gi
			valRet = valRet.replaceAll(regex, ''$1(6)'')
		}

		// -- Replace create statement and target table 
		regex = /create or replace TABLE.*\\(/i
		valRet = valRet.replace(regex, `create iceberg table ${target_table} (`)

        //MH.START CHANGE 20250716
		// -- Replace create transient statement and target table 
		regex = /create or replace TRANSIENT TABLE.*\\(/i
		valRet = valRet.replace(regex, `create iceberg table ${target_table} (`)		
        //MH.END CHANGE 20250716
	
		// -- If provided replace cluster 
		if ( TABLE_CONFIGURATION !== null) {
			if ( TABLE_CONFIGURATION.cluster_key !== null) {
				valRet = valRet.replace('';'', ` CLUSTER BY (${TABLE_CONFIGURATION.cluster_key});`)	
			} 
		}

        // ----- Catalog specific replacements 
        if (settings.core.catalog_type == ''SNOW'') {
            // -- Build location 
            var location = ""
            location = settings.snow.location_pattern
            if (v_replace) {
                location = location.replaceAll("${TABLE_CATALOG}",TABLE_CATALOG.replaceAll(" ", "-"))
                location = location.replaceAll("${TABLE_SCHEMA}",TABLE_SCHEMA.replaceAll(" ", "-"))
            }
            else {
                location = location.replaceAll("${TABLE_CATALOG}",TARGET_TABLE_CATALOG.replaceAll(" ", "-"))
                location = location.replaceAll("${TABLE_SCHEMA}",TARGET_TABLE_SCHEMA.replaceAll(" ", "-"))
            }
            location = location.replaceAll("${TABLE_NAME}",TABLE_NAME.replaceAll(" ", "-"))

            // -- Add additional iceberg config
		    valRet = valRet.replace('';'', ` CATALOG = ''SNOWFLAKE'' EXTERNAL_VOLUME = ''${settings.snow.external_volume}'' BASE_LOCATION = ''${location}''`)		

        }
		else if (settings.core.catalog_type == ''GLUE'') {
			var glue_config = ` catalog_table_name = ''${TABLE_NAME}''`

			if (typeof settings.glue.external_volume != ''undefined'') {
				glue_config += ` external_volume = ''${settings.glue.external_volume}''`
			}

			if (typeof settings.glue.catalog != ''undefined'') {
				glue_config += ` catalog = ''${settings.glue.catalog}''`				
			}
			else {
				glue_config += ` catalog = ''no_catalog''`					
			}

			if (typeof settings.glue.catalog_namespace === ''undefined'') {
				settings.glue.catalog_namespace = "${TABLE_SCHEMA}"
			}
			
			var name_space = settings.glue.catalog_namespace
			if (v_replace) {
				name_space = name_space.replaceAll("${TABLE_CATALOG}",TABLE_CATALOG.replaceAll(" ", "-"))
				name_space = name_space.replaceAll("${TABLE_SCHEMA}",TABLE_SCHEMA.replaceAll(" ", "-"))
			}
			else {
				name_space = name_space.replaceAll("${TABLE_CATALOG}",TARGET_TABLE_CATALOG.replaceAll(" ", "-"))
				name_space = name_space.replaceAll("${TABLE_SCHEMA}",TARGET_TABLE_SCHEMA.replaceAll(" ", "-"))
			}
			ret[''name_space''] = glue_config	
			glue_config += ` catalog_namespace = ''${name_space}''`
			

			if (typeof settings.glue.replace_invalid_characters != ''undefined'') {
				glue_config += ` replace_invalid_characters = ${settings.glue.replace_invalid_characters}`
			}

			if (typeof settings.glue.auto_refresh != ''undefined'') {
				glue_config += ` auto_refresh = ${settings.glue.auto_refresh}`
			}

            // -- Add additional iceberg config
		    valRet = valRet.replace('';'', ` ${glue_config}`)		
		}
        else {
            valRet = valRet
        }

		return valRet
	}


    // ---------------------------------------------------------------------------------------------
    // Name: checkValid
    // --------------------------------------------------------------------------------------------- 
    // Returns a structure with a result of true if valid and a message for failed validation 
    // ---------------------------------------------------------------------------------------------
	function checkValid()
	{
    	var valRet = {}
    	valRet[''result''] = true

		// -- Validates the table exists and is not an iceberg table 
		v_csql = `select table_owner, is_iceberg from ${TABLE_CATALOG}.information_schema.tables where table_schema = ''${TABLE_SCHEMA}'' and table_name = ''${TABLE_NAME}''`
		let v_chk_rs = snowflake.execute({sqlText: v_csql})

		if (v_chk_rs.getRowCount() == 0) {
			valRet[''result''] = false	
			valRet[''message''] = `${v_tablename} table does not exist`
		}
		else {
			v_chk_rs.next()

			// -- Grab table owner while here 
			valRet[''owner''] = v_chk_rs.TABLE_OWNER

			// -- Check if iceberg 
			if (v_chk_rs.IS_ICEBERG == ''YES'') {
				valRet[''result''] = false	
				valRet[''message''] = `${v_tablename} is an iceberg table`
			}
			else {
				// -- Determine if timezone conversion 
				if (settings.core.timezone_conversion==''NONE''){
					v_timestamp_tz = ''TIMESTAMP_TZ''
				}
				else {
					v_timestamp_tz = ''--''
				}
				// -- Validates the table does not contain data types that are not supported by iceberg 
				v_csql = `select count(*) as cnt, array_to_string(array_agg(column_name || '' ('' || data_type || '')''), '', '') as cols
							from ${TABLE_CATALOG}.information_schema.columns
							where table_schema = ''${TABLE_SCHEMA}'' 
							and table_name = ''${TABLE_NAME}''
							and data_type in (''${v_timestamp_tz}'',''VARIANT'',''OBJECT'',''ARRAY'',''GEOGRAPHY'',''GEOMETRY'',''VECTOR'')`
				
				let v_chk_rs = snowflake.execute({sqlText: v_csql})
				v_chk_rs.next() 
				if (v_chk_rs.CNT != 0) {
					valRet[''result''] = false	
					valRet[''message''] = `${v_tablename} contains the following columns that are not supported in iceberg: ${v_chk_rs.COLS}`
				}		
			}
		}
		return (valRet)
	}

  ';