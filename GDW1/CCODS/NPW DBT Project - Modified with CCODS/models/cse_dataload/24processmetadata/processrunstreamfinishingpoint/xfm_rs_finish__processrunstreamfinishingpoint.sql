select ETL_D
from {{ ref("run_strm_etl_d_hash__processrunstreamfinishingpoint") }}