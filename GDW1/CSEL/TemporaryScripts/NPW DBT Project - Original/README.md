# CommBank DataStage to dbt Migration Project

This project contains dbt models that have been converted from DataStage parallel/server jobs. Each DataStage job stage component has been converted to a corresponding dbt model, organized in a folder structure that mimics the original DataStage job hierarchy.

## Getting Started

### Prerequisites

- dbt Core or dbt Cloud installed
- Snowflake account with appropriate permissions
- Python 3.8+ installed
- Git (for version control)
- create required databases/schemas/tables/stored procedures.
- airflow setup 

### Setup Instructions

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd np_projects_commbank_sf_dbt
   ```

2. **Install dependencies** (WIP)
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure your profile**
   - Edit the `~/.dbt/profiles.yml` file to include your Snowflake connection details:
   ```yaml
   np_projects_commbank_sf_dbt:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: <your-snowflake-account>
         user: <your-username>
         password: <your-password>
         role: <your-role>
         database: GDW1_STG
         warehouse: <your-warehouse>
         schema: <your-schema>
         threads: 4
   ```

4. **Create Required DDLs**  
   - The folder for all required tables is located in the `ddl_dml` directory:  
     - `gdw1_ibrg.sql` – Contains all required Iceberg tables.  
     - `gdw1_stg.sql` – Contains the `files` schema (used for the ingestion framework) and datasets for creating temporary tables.  
     - `gdw1.sql` – Contains the Teradata databases as Snowflake schemas and Iceberg views. It also contains Oracle tables that are used in the process tables.  
     - `gdw1_stg.files` – Schema containing the tables that must be populated by the ingestion framework.  

 

5. **Airflow setup details**  
   - this is WIP 

## Running the Models

### Understanding the Structure

The models in this project are organized to reflect the original DataStage job structure:

- Each folder under `models/` represents a high-level job category (e.g., `appt_pdct`, `cse_dataload`)
- Within each category, subfolders represent individual DataStage jobs (e.g., `dltappt_pdctfrmtmp_appt_pdct`)
- Inside each job folder, SQL files represent individual components of the DataStage job

### Running a Complete DataStage Job Equivalent

To run a complete DataStage job equivalent, you need to run all models in the corresponding job folder.

#### Run using folder path selector

```bash
dbt run --select cse_dataload.24processmetadata.processrunstreamstatuscheck
```

## DataStage to dbt Model Mapping

Each DataStage job component has been converted to a dbt model with the following naming convention:

`<component_name>__<job_name>.sql`

For example:
- `changecapture__dltappt_pdctfrmtmp_appt_pdct.sql` represents the "changecapture" component of the "dltappt_pdctfrmtmp_appt_pdct" job


## Environment Variables and Job Parameters

The project uses environment variables defined in the `dbt_project.yml` file under the `vars` section. These correspond to the original DataStage environment variables.

### Job Parameters

Job-specific parameters are defined in the `job_params` section of `dbt_project.yml`. These parameters are organized by job name and can be accessed in models using the `cvar` macro from `macros/load_job_params.sql`.

Example of job parameters in `dbt_project.yml`:
```yaml
job_params:
  processrunstreamstatuscheck:
    ctl_schema: CSE4_CTL
    ctl_database: cse2dev
    run_stream: CSE_CPL_BUS_APP
    # ... other parameters
```

### Using the cvar Macro

The `cvar` macro automatically retrieves the appropriate parameter value for the current model based on its folder path. This allows models to access their job-specific parameters without hardcoding:

```sql
-- Example usage in a model
SELECT {{ cvar('ctl_schema') }} AS schema_name
```

The macro works by:
1. Determining the job folder name from the model's path
2. Looking up parameters for that job in the job_params dictionary
3. Returning the appropriate parameter value or a default if specified

### Overriding Job Parameters

You can override job parameters at runtime using the `--vars` flag in the dbt run command:

```bash
dbt run --select cse_dataload.24processmetadata.processrunstreamstatuscheck --vars '{"run_stream": "CSE_NEW_STREAM", "etl_process_dt": "20250807"}'
```

This is useful for:
- Testing with different parameter values
- Running jobs with date-specific parameters
- Changing target environments without modifying the dbt_project.yml file

### Change log  
#### DATE : 20250807  
   - added  brefore__jobname.sql model for creating the required temp tables used in the models. 
   - updated the models to make use of the temp tables created in the brefore__jobname.sql
   - updated the profiels.yml paramters to use sequence job level parameters for testing from the dbt