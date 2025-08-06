import const
from column import Column
from enums import HashFunction, RollupType
import math
import re
from typing import Any

class System(object):
    def __init__(self, options):
        if options is None:
            options = {}
        for k, v in {
            const.CharacterLengthOverride: None,
            const.DistinctCounts: False,
            const.FloatScaleOverride: None,
            const.HasCollation: False,
            const.HashBuckets: 4,
            const.HashFunction: str(HashFunction.MD5),
            const.MaxValues: False,
            const.MaxSelectColumns: 1000,
            const.MinValues: False,
            const.NullCounts: False,
            const.NumericScaleOverride: None,
            const.SelectModifier: None,
            const.ObjectModifier: None,
            const.SumValues: False,
            const.UseHashForSums: False
        }.items():
            setattr(self, k.lower(), v)
        for k, v in options.items():
            setattr(self, k.lower(), v)
        self.select_modifier = '' if self.select_modifier is None else f' {self.select_modifier}'
        self.object_modifier = '' if self.object_modifier is None else f' {self.object_modifier}'
        self.use_hash_for_sums = self.sum_values and self.use_hash_for_sums
        self.hash_name, self.hash_bits = HashFunction.get(self.hash_function, HashFunction.MD5)
        self.hash = dict(zip([ str(h) for h in [ HashFunction.MD5, HashFunction.SHA2_256, HashFunction.SHA2_512 ] ], [ self.md5, self.sha2_256, self.sha2_512 ]))[self.hash_name]
        self.bytes_per_bucket = int(self.hash_bits / (self.bits_per_byte * self.hash_buckets))
        self.identifier_quoted_re = re.compile(rf'^{re.escape(self.quotes[0])}(.+){re.escape(self.quotes[1])}$')
        self.escaped_like_wildcards_re = re.compile(r'(?<=\\)[%_]')
        self.unescaped_like_wildcards_re = re.compile(r'(?<!\\)[%_]')
        self.unload_location = self.unload_directory
        self.upload_location = None
        self.stage_location = None
    
    @staticmethod
    def get_system(system_type: str, options: dict):
        match system_type.lower():
            case const.Oracle:
                from system_oracle import Oracle
                return Oracle(options)
            case const.Teradata:
                from system_teradata import Teradata
                return Teradata(options)
            case const.Postgres:
                from system_postgres import Postgres
                return Postgres(options)
            case const.Netezza:
                from system_netezza import Netezza
                return Netezza(options)
            case const.Redshift:
                from system_redshift import Redshift
                return Redshift(options)
            case const.SqlServer:
                from system_sqlserver import SqlServer
                return SqlServer(options)
            case const.Cloudera:
                from system_cloudera import Cloudera
                return Cloudera(options)
            case _:
                from system_snowflake import Snowflake
                return Snowflake(options)
    
    # metadata methods (these are client specific; the below is used if no client-specific override is present)

    def get_object_metadata_sql(self, system_name: str, database_name: str, schema_object_names: list) -> str:
        schema_object_filter = self.get_schema_object_filter(schema_object_names, const.TableSchema, const.TableName)
        return f"""select{self.select_modifier}
    {self.column_as(self.literal(self.system_type), const.SystemType)},
    {self.column_as(self.literal(system_name), const.SystemName)},
    {self.column_as(const.TableCatalog, const.DatabaseName)},
    {self.column_as(const.TableSchema, const.SchemaName)},
    {self.column_as(const.TableName, const.ObjectName)},
    {self.column_as(self.literal(const.Table.upper()), const.ObjectType)},
    {self.column_as(const.RowCount, const.EstNumRecords)},
    {self.column_as(self.round(f'{self.nvl(const.Bytes, 0)}/ (1024 * 1024)', 6), const.EstTableSizeMb)},
    {self.column_as(const.Null, const.ExtraInfo)}
from
    {self.identifier(database_name)}.information_schema.tables{self.object_modifier}
where
    {const.TableSchema} != 'INFORMATION_SCHEMA'
    and {const.TableType} = 'BASE TABLE'{{0}}""".format('' if schema_object_filter is None else f"""
    and {schema_object_filter}""")

    def get_column_metadata_sql(self, system_name: str, database_name: str, schema_object_names: list) -> str:
        schema_object_filter = self.get_schema_object_filter(schema_object_names, f'c.{const.TableSchema}', f'c.{const.TableName}')
        return f"""select{self.select_modifier}
    {self.column_as(self.literal(self.system_type), const.SystemType)},
    {self.column_as(self.literal(system_name), const.SystemName)},
    {self.column_as(f'c.{const.TableCatalog}', const.DatabaseName)},
    {self.column_as(f'c.{const.TableSchema}', const.SchemaName)},
    {self.column_as(f'c.{const.TableName}', const.ObjectName)},
    c.{const.ColumnName},
    c.{const.OrdinalPosition},
    c.{const.ColumnDefault},
    c.{const.IsNullable},
    c.{const.DataType},
    c.{const.CharacterMaximumLength},
    c.{const.NumericPrecision},
    c.{const.NumericScale},
    c.{const.DatetimePrecision},
    c.{const.CharacterSetName},
    c.{const.CollationName},
    {self.column_as(const.Null, const.ExtraInfo)},
    {self.column_as('object_construct(c.*)', const.RawMetadata)}
from
    {self.identifier(database_name)}.information_schema.columns c{self.object_modifier}
    inner join {self.identifier(database_name)}.information_schema.tables t{self.object_modifier}
    on c.{const.TableCatalog} = t.{const.TableCatalog} and c.{const.TableSchema} = t.{const.TableSchema} and c.{const.TableName} = t.{const.TableName}
where
    c.{const.TableSchema} != 'INFORMATION_SCHEMA'
    and t.{const.TableType} = 'BASE TABLE'{{0}}""".format('' if schema_object_filter is None else f"""
    and {schema_object_filter}""")

    def get_index_metadata_sql(self, system_name: str, database_name: str, schema_object_names: list) -> str:
        pass

    # profiling methods

    def get_profile_left(self, column):
        return self.len(self.to_char(self.trunc(self.max(self.abs(column.identifier)))))

    def get_profile_right(self, column):
        c_mod = self.mod(column.identifier, 1)
        c_char = self.to_char(self.abs(c_mod))
        d_i = f'{self.decimal_index(c_char)} + 1'
        t = f"trailing '0' from {self.substr(c_char, d_i)}"
        return self.max(f"""case
            when {c_mod} = 0 then 0
            else {self.len(self.trim(t))}
        end""")

    def get_profile_with(self, columns):
        w = []
        for c in columns:
            w += [
                    self.column_as(self.get_profile_left(c), f'{c.column_map}_l'),
                    self.column_as(self.get_profile_right(c) if self.numeric_scale_override is None else self.numeric_scale_override, f'{c.column_map}_r')
                ]
        return ',\n        '.join(w)

    def get_profile_select(self, columns):
        s = []
        for c in columns:
            s += [
                    self.column_as(self.literal(c.column_name), f'{c.column_map}_{const.ColumnName}'),
                    self.column_as(const.SnowflakeNumericPrecision, f'{c.column_map}_{const.NumericPrecision}'),
                    self.column_as(f"""case
        when {c.column_map}_l = 0 and {c.column_map}_r >= {const.SnowflakeNumericPrecision} then {const.SnowflakeNumericPrecision - 1}
        when ({const.SnowflakeNumericPrecision} - {c.column_map}_l) >= {c.column_map}_r then {c.column_map}_r
        when ({const.SnowflakeNumericPrecision} - {c.column_map}_l) > 0 then {const.SnowflakeNumericPrecision} - {c.column_map}_l
        else 0
    end""", f'{c.column_map}_{const.NumericScale}')
                ]
        return ',\n    '.join(s)

    def get_profile_map(self, columns):
        return self.column_as(self.literal(','.join([ c.column_map for c in columns ])), f'{const.ColumnMap}s')

    def get_profile_columns(self, columns):
        return [ c for c in self.get_columns(columns) if c.rollup_type == RollupType.Numeric.value and (c.numeric_precision is None or c.numeric_scale is None) ]

    def get_profile_sqls(self, database_name, schema_name, object_name, columns):
        profile_columns, sqls = self.get_profile_columns(columns), []
        if len(profile_columns):
            sqls = [ f"""with profiles as (
    select{self.select_modifier}
        {self.get_profile_with(profile_columns[start:end])}
    from
        {self.fully_qualified_name(database_name, schema_name, object_name)}{self.object_modifier}
)
select{self.select_modifier}
    {self.get_profile_select(profile_columns[start:end])},
    {self.get_profile_map(profile_columns[start:end])}
from profiles""" for start, end in self.get_start_end(len(profile_columns), max(math.floor(self.max_select_columns / 3), 1)) ]
        return sqls

    # checksum methods

    def get_checksum_function_values(self, object_identifier: str, columns: list[dict], method: dict) -> dict:
        return { const.ObjectIdentifier: f'{object_identifier}{self.object_modifier}', const.ChecksumFunction: method.get(const.ChecksumFunction, self.checksum_function) }

    def get_checksum_by_integer_values(self, object_identifier: str, columns: list[dict], method: dict) -> dict:
        column_identifier, modulus, lookback = self.column_identifier(method[const.ColumnName]), method.get(const.Modulus, 1000000), method.get(const.Lookback, None)
        source_filter = f"""case
            when {const.ExtractGroupId} is null then '{column_identifier} is null'
            else '{column_identifier} >= ' || {const.ExtractGroupId} * {modulus} || ' and {column_identifier} < ' || ({const.ExtractGroupId} + 1) * {modulus}
        end"""
        values = {
            const.ExtractGroupId: self.floor(f'{column_identifier}/{modulus}'),
            const.CharExtractGroupId: self.to_char(const.ExtractGroupId),
            const.SourceFilter: source_filter,
            const.TargetFilter: source_filter,
            const.MinExtractGroupId: const.Null,
            const.WhereFilter: None
        }
        if lookback is not None:
            values.update({ const.MinExtractGroupId: self.to_char(self.floor(f'{lookback}/{modulus}')), const.WhereFilter: f'{column_identifier} >= {lookback}'})
        return values
    
    def get_checksum_by_date_values(self, object_identifier: str, columns: list[dict], method: dict) -> dict:
        column_identifier, period, lookback = self.column_identifier(method[const.ColumnName]), method.get(const.Period, 'month'), method.get(const.Lookback, None)
        date_from = self.to_date(f"''' || {self.to_char(const.ExtractGroupId, self.literal('yyyymmdd'))} || ''', '{self.literal('yyyymmdd')}'")
        source_filter = f"case when {const.ExtractGroupId} is null then '{column_identifier} is null' else '{column_identifier} >= {date_from} and {column_identifier} < {date_from} + interval ''1 {period}''' end"
        values = {
            const.ExtractGroupId: self.date_trunc(self.literal(period), column_identifier),
            const.CharExtractGroupId: self.to_char(const.ExtractGroupId, self.literal('yyyymmdd')),
            const.SourceFilter: source_filter,
            const.TargetFilter: source_filter,
            const.MinExtractGroupId: const.Null,
            const.WhereFilter: None
        }
        if lookback is not None:
            match period.lower():
                case 'year':
                    min_extract_group_id = self.literal(f'{lookback[:4]}0101')
                case 'month':
                    min_extract_group_id = self.literal(f'{lookback[:6]}01')
                case 'day':
                    min_extract_group_id = self.literal(lookback[:8])
            values.update({ const.MinExtractGroupId: min_extract_group_id, const.WhereFilter: f"{column_identifier} >= {self.to_date(min_extract_group_id, self.literal('yyyymmdd'))}" })
        return values
    
    def get_checksum_by_substr_values(self, object_identifier: str, columns: list[dict], method: dict) -> dict:
        column_identifier, length, lookback = self.column_identifier(method[const.ColumnName]), method.get(const.Length, 12), method.get(const.Lookback, None)
        extract_group_id = self.substr(column_identifier, 1, length)
        source_filter = f"case when {const.ExtractGroupId} is null then '{column_identifier} is null' else '{extract_group_id} = ''' || {const.ExtractGroupId} || '''' end"
        values = {
            const.ExtractGroupId: extract_group_id,
            const.CharExtractGroupId: const.ExtractGroupId,
            const.SourceFilter: source_filter,
            const.TargetFilter: source_filter,
            const.MinExtractGroupId: const.Null,
            const.WhereFilter: None
        }
        if lookback is not None:
            values.update({ const.MinExtractGroupId: self.substr(self.literal(lookback), 1, length), const.WhereFilter: f'{column_identifier} >= {lookback}' })
        return values
    
    def get_checksum_by_method(self, values: dict[str, str]) -> str:
        where_filter = '' if (where_filter := values.get(const.WhereFilter)) is None else f"""
    where
        {where_filter}"""
        hashes_groupings = f"""hashes as (
    select{self.select_modifier}
        {self.column_as(values[const.ExtractGroupId], const.ExtractGroupId)},
        {self.column_as(values[const.HashedColumns], const.HashedColumns)}
    from
        {values[const.ObjectIdentifier]}{where_filter}
), groupings""" if const.HashedColumns in values else 'groupings'
        return f"""with {hashes_groupings} as (
    select
        {self.column_as(const.ExtractGroupId if const.HashedColumns in values else values[const.ExtractGroupId], const.ExtractGroupId)},
        {self.column_as(values[const.ChecksumFunction], const.PartitionChecksum)},
        {self.column_as(self.count('1'), const.PartitionRowCount)}
    from
        {'hashes' if const.HashedColumns in values else values[const.ObjectIdentifier]}{'' if const.HashedColumns in values else where_filter}
    group by
        {const.ExtractGroupId if const.HashedColumns in values else values[const.ExtractGroupId]}
)
select{'' if const.HashedColumns in values else self.select_modifier}
    {self.column_as(values[const.CharExtractGroupId], const.ExtractGroupId)},
    {self.column_as(values[const.MinExtractGroupId], const.MinExtractGroupId)},
    {self.column_as(values[const.SourceFilter], const.SourceFilter)},
    {self.column_as(values[const.TargetFilter], const.TargetFilter)},
    {const.PartitionChecksum},
    {const.PartitionRowCount}
from
    groupings"""
    
    def get_checksum_by_table(self, values: dict[str, str]) -> str:
        with_hashes = f"""with hashes as (
    select{self.select_modifier}
        {self.column_as(values[const.HashedColumns], const.HashedColumns)}
    from
        {values[const.ObjectIdentifier]}
)
""" if const.HashedColumns in values else ''
        return f"""{with_hashes}select{'' if const.HashedColumns in values else self.select_modifier}
    {self.column_as(self.literal(const.WholeTable), const.ExtractGroupId)},
    {self.column_as(const.Null, const.MinExtractGroupId)},
    {self.column_as(self.literal('1 = 1'), const.SourceFilter)},
    {self.column_as(self.literal('1 = 1'), const.TargetFilter)},
    {self.column_as(values[const.ChecksumFunction], const.PartitionChecksum)},
    {self.column_as(self.count('1'), const.PartitionRowCount)}
from
    {'hashes' if const.HashedColumns in values else values[const.ObjectIdentifier]}"""

    def get_checksum_sql(self, database_name: str, schema_name: str, object_name: str, columns: list[dict], method: dict) -> str:
        object_identifier = self.fully_qualified_name(database_name, schema_name, object_name)
        checksum_function_values = self.get_checksum_function_values(object_identifier, columns, method)
        match method.get(const.Type, const.WholeTable).lower():
            case const.ByInteger:
                return self.get_checksum_by_method({ **self.get_checksum_by_integer_values(object_identifier, columns, method), **checksum_function_values })
            case const.ByDate:
                return self.get_checksum_by_method({ **self.get_checksum_by_date_values(object_identifier, columns, method), **checksum_function_values })
            case const.BySubstr:
                return self.get_checksum_by_method({ **self.get_checksum_by_substr_values(object_identifier, columns, method), **checksum_function_values })
            case _:
                return self.get_checksum_by_table(checksum_function_values)
    
    # unload methods

    def get_unload_sql(self, database_name, schema_name, object_name, source_columns, source_filter):
        return f"""select{self.select_modifier}
    {{0}}
from
    {self.fully_qualified_name(database_name, schema_name, object_name)}{self.object_modifier}
where
    {source_filter}""".format(',\n    '.join([ self.column_as(self.charamelize(c), c.identifier) for c in self.get_columns(source_columns) ]) if not(source_columns is None or len(source_columns) == 0) else '*')

    def get_unload_request(self, database_name: str, schema_name: str, object_name: str, source_columns: list, source_filter: str, escape_column_names: list, binary_column_names: list) -> dict:
        return {
            const.UnloadSql: self.get_unload_sql(database_name, schema_name, object_name, source_columns, source_filter),
            const.UnloadLocation: self.unload_location,
            const.FetchChunkSize: self.fetch_chunk_size,
            const.EscapeColumns: escape_column_names,
            const.BinaryColumns: binary_column_names
        }

    # upload methods
    
    # measure methods

    def distinct_count_as(self, column):
        return self.column_as(self.count_distinct(column.identifier), f"{column.column_map}_{const.DistinctCount}")
    
    def null_count_as(self, column):
        return self.column_as(self.sum(f'case when {column.identifier} is null then 1 else 0 end'), f'{column.column_map}_{const.NullCount}')

    def min_value_as(self, column):
        return self.column_as(self.charamelize(column, self.min(self.floor(column.identifier, column.numeric_scale) if column.truncated() else column.identifier)), f'{column.column_map}_{const.MinValue}')
    
    def max_value_as(self, column):
        return self.column_as(self.charamelize(column, self.max(self.floor(column.identifier, column.numeric_scale) if column.truncated() else column.identifier)), f'{column.column_map}_{const.MaxValue}')
    
    def sum_value_as(self, column):
        return self.column_as(self.charamelize(column, self.sum(self.floor(column.identifier, column.numeric_scale) if column.truncated() else column.identifier)) if column.summable() else self.literal(const.NotSummable), f'{column.column_map}_{const.SumValue}')
    
    def hashed_column_as(self, column):
        return self.column_as(self.hash(self.nvl(self.charamelize(column), self.literal(const.NullToken)) if column.nullable() else self.charamelize(column)), f'{column.column_map}_{const.Hash}')

    def hashed_sum_column_as(self, column, i):
        return self.column_as(self.sum(self.to_number(self.substr(f'{column.column_map}_{const.Hash}', (i * self.bytes_per_bucket) + 1, self.bytes_per_bucket), self.literal(self.format_hex(self.bytes_per_bucket)))), f'{column.column_map}_sum_{i + 1}')

    def get_iceberg_column_identifier(self, column_name):
        """
        Generate lowercase column identifier for Iceberg tables.
        Iceberg tables on catalog-linked databases require lowercase column names.
        """
        # Remove quotes if present and convert to lowercase
        clean_name = column_name.strip('"').strip("'").lower()
        # Re-quote if needed for special characters or reserved words
        if not self.identifier_regular_re.search(clean_name):
            return self.quoted(clean_name)
        return clean_name

    def get_columns_with_iceberg_support(self, columns, is_iceberg_catalog=1):
        """
        Process columns with optional Iceberg catalog support for lowercase identifiers.
        """
        processed_columns = []
        for i, c in enumerate(columns, start=1):
            #if is_iceberg_catalog:
            if is_iceberg_catalog == 1:
                # Use lowercase identifiers for Iceberg catalog tables
                iceberg_identifier = self.get_iceberg_column_identifier(c[const.ColumnName.upper()])
                column_data = {**c, const.Identifier: iceberg_identifier}
            else:
                # Use standard identifier processing
                column_data = {**c, const.Identifier: self.column_identifier(c[const.ColumnName.upper()])}
            processed_columns.append(Column(i, **column_data))
        return processed_columns

    def get_measure_sqls(self, database_name, schema_name, object_name, columns, where_clause, is_iceberg_catalog=1):
        """
        Generate measure SQL statements for data profiling.
        
        Args:
            database_name: Target database name
            schema_name: Target schema name  
            object_name: Target table/object name
            columns: List of column metadata
            where_clause: WHERE clause filter
            is_iceberg_catalog: Boolean flag indicating if this is an Iceberg table on a catalog-linked database
        """
        # Process columns with appropriate identifier handling based on table type
        all_columns = self.get_columns_with_iceberg_support(columns, is_iceberg_catalog)
        measure_columns = [c for c in all_columns if c.rollup_type != RollupType.Other.value]
        
        sqls = []
        if len(measure_columns) > 0:
            measure_column_count = sum(1 for m in [ True, self.distinct_counts, self.null_counts, self.min_values, self.max_values ] if m is True)
            measure_column_count += self.hash_buckets if self.use_hash_for_sums else 1 if self.sum_values else 0
            object_identifier = self.fully_qualified_name(database_name, schema_name, object_name)
            
            # Add comment to generated SQL indicating processing type
            sql_comment = f"-- {'Iceberg catalog table (lowercase columns)' if is_iceberg_catalog==1 else 'Standard table processing'}"
            
            for start, end in self.get_start_end(len(measure_columns), max(math.floor(self.max_select_columns / measure_column_count), 1)):
                hashed_columns, hashed_sum_columns, column_maps, unhashed_columns = [], [], [], [ self.column_as(self.count('1'), const.RowCount) ]
                if (self.distinct_counts or self.null_counts or self.min_values or self.max_values or self.sum_values):
                    for c in measure_columns[start:end]:
                        unhashed_columns += [ self.column_as(self.literal(c.column_name), f'{c.column_map}_{const.ColumnName}')]
                        unhashed_columns += [ k for k, v in dict(zip([ self.distinct_count_as(c), self.null_count_as(c), self.min_value_as(c), self.max_value_as(c) ], [ self.distinct_counts, self.null_counts, self.min_values, self.max_values ])).items() if v is True ]
                        if self.sum_values:
                            if self.use_hash_for_sums:
                                hashed_columns += [ self.hashed_column_as(c) ]
                                hashed_sum_columns += [ self.hashed_sum_column_as(c, i) for i in range(self.hash_buckets) ]
                            else:
                                unhashed_columns += [ self.sum_value_as(c) ]
                        column_maps += [ c.column_map ]
                    unhashed_columns += [ self.column_as(self.literal(','.join(column_maps)), f'{const.ColumnMap}s') ]
                if len(hashed_columns) > 0:
                    sqls.append(f"""{sql_comment}
with hashed as (
    select{self.select_modifier}
        {{0}}
    from
        {object_identifier}{self.object_modifier}
    where
        {where_clause}
), hashed_sums as (
    select
        {{1}}
    from
        hashed
), unhashed as (
    select{self.select_modifier}
        {{2}}
    from
        {object_identifier}{self.object_modifier}
    where
        {where_clause}
)
select{self.select_modifier}
    *
from
    unhashed,
    hashed_sums""".format(',\n        '.join(hashed_columns), ',\n        '.join(hashed_sum_columns), ',\n        '.join(unhashed_columns)))
                else:
                    sqls.append(f"""{sql_comment}
select{self.select_modifier}
    {{0}}
from
    {object_identifier}{self.object_modifier}
where
    {where_clause}""".format(',\n    '.join(unhashed_columns)))
        return sqls
    
    # supporting sql methods (copy to system_<system>.py to override)

    def abs(self, literal):
        return f'abs({literal})'
    
    def concat(self, literal):
        return f'concat({literal})'
    
    def concat_ws(self, delimiter, literal):
        return f'concat_ws({delimiter}, {literal})'
    
    def count(self, literal):
        return f'count({literal})'

    def count_distinct(self, literal):
        return self.count(f'distinct {literal}')
    
    def date_trunc(self, period, literal):
        return f'date_trunc({period}, {literal})'

    def decimal_index(self, literal):
        return f"regexp_instr({literal}, '\\.')"
    
    def floor(self, literal, scale=None):
        return f'floor({literal}{"" if scale is None else f", {scale}"})'
    
    def format_date(self) -> str:
        return 'yyyy-mm-dd'
    
    def format_datetime(self, precision: Any=None) -> str:
        return f'{self.format_date()} {self.format_time(precision)}'
    
    def format_hex(self, length=None):
        if length is None:
            length = 0
        return f"{'0' * (max(length - 1, 0))}x"
    
    def format_numeric(self, precision: Any=None, scale: Any=None) -> str:
        if precision is None:
            precision = const.SnowflakeNumericPrecision
        if scale is None:
            scale = const.SnowflakeNumericScale
        left, right = max((precision - scale - 1), 0) * '9', f".{scale * '0'}" if scale else ''
        return f'FM{left}0{right}'
    
    def format_time(self, precision: Any=None) -> str:
        precision = f".ff{precision}{max(const.SnowflakeTimestampPrecision - precision, 0) * '0'}" if precision else ''
        return f'hh24:mi:ss{precision}'
    
    def format_timestamp(self, precision: Any=None) -> str:
        return self.format_datetime(precision)
    
    def len(self, literal):
        return f'len({literal})'
    
    def lower(self, literal):
        return f'lower({literal})'
    
    def max(self, literal):
        return f'max({literal})'
    
    def md5(self, literal):
        return f'md5({literal})'
    
    def min(self, literal):
        return f'min({literal})'
    
    def mod(self, literal, modulus):
        return f'mod({literal}, {modulus})'
    
    def nvl(self, literal: Any, token: Any=None) -> str:
        if token is None:
            token = self.literal(const.NullToken)
        return f'nvl({literal}, {token})'
    
    def round(self, literal, scale=None):
        return f'round({literal}, {0 if scale is None else scale})'
    
    def sha2_256(self, literal):
        return f'sha2({literal}, 256)'
    
    def sha2_512(self, literal):
        return f'sha2({literal}, 512)'
    
    def substr(self, literal, start, end=None):
        return f'substr({literal}, {start}{"" if end is None else f", {end}"})'
    
    def sum(self, literal):
        return f'sum({literal})'
    
    def to_char(self, literal, format=None):
        return f'to_char({literal}{"" if format is None else f", {format}"})'
    
    def to_date(self, literal, format=None):
        return f'to_date({literal}{"" if format is None else f", {format}"})'
    
    def to_number(self, literal, format=None):
        return f'to_number({literal}{"" if format is None else f", {format}"})'
    
    def trim(self, literal):
        return f'trim({literal})'
    
    def trunc(self, literal):
        return f'trunc({literal})'

    # other public methods

    def charamelize(self, column, literal=None):
        if literal is None:
            literal = column.identifier
        match column.rollup_type:
            case RollupType.Character.value:
                return self.substr(literal, 1, self.character_length_override) if self.character_length_override is not None and column.character_maximum_length > self.character_length_override else literal
            case RollupType.Date.value:
                return self.to_char(literal, self.literal(self.format_date()))
            case RollupType.Datetime.value:
                return self.to_char(literal, self.literal(self.format_datetime(column.datetime_precision)))
            case RollupType.Numeric.value:
                return self.to_char(literal, self.literal(self.format_numeric(const.SnowflakeNumericPrecision, self.numeric_scale_override if self.numeric_scale_override is not None and column.numeric_scale > self.numeric_scale_override else column.numeric_scale)))
            case RollupType.Time.value:
                return self.to_char(literal, self.literal(self.format_time(column.datetime_precision)))
            case RollupType.Timestamp.value:
                return self.to_char(literal, self.literal(self.format_timestamp(column.datetime_precision)))
        return self.to_char(literal)

    def column_as(self, literal, alias):
        return f'{literal} as {alias}'
    
    def column_identifier(self, literal: str) -> str:
        return self.escape_curly_braces(literal if self.identifier_quoted_re.search(literal) else self.quoted(literal))
    
    def escape_curly_braces(self, literal: str) -> str:
        return re.sub(r'[{}]', lambda b: b.group(0) * 2, literal)
    
    def fully_qualified_name(self, *literals: str) -> str:
        return '.'.join([ self.identifier(literal) for literal in literals ])
    
    def get_case_statement(self, when_conditions, else_condition, indent=None):
        if indent is None:
            indent = '    '
        return f"""case
{indent}    {{0}}
{indent}    else {else_condition}
{indent}end""".format(f'\n{indent}    '.join([ f'when {k} then {v}' for k, v in when_conditions.items() ]))
    
    def get_columns(self, columns):
        return [ Column(i, **{**c, const.Identifier: self.column_identifier(c[const.ColumnName.upper()]) }) for i, c in enumerate(columns, start=1) ]
    
    def get_schema_object_filter(self, schema_object_names: list, schema_column: str, object_column: str, indent: str=' ' * 8) -> str:
        if [ '%', '%' ] in schema_object_names:
            return None
        conditions = []
        for schema_pattern, object_pattern in schema_object_names:
            condition = []
            if not(schema_pattern is None or schema_pattern == '%'):
                condition.append(f"{schema_column} {self.like_or_equal(schema_pattern)} '{schema_pattern}'")
            if not(object_pattern is None or object_pattern == '%'):
                condition.append(f"{object_column} {self.like_or_equal(object_pattern)} '{object_pattern}'")
            conditions.append(' and '.join(condition))
        return f"""case
{indent}    when {{0}} then 1
{indent}    else 0
{indent}end = 1""".format(f' then 1\n{indent}    when '.join(conditions))
    
    def get_start_end(self, num_columns, columns_per_select):
        return [ (i * columns_per_select, min(num_columns, (i + 1) * columns_per_select)) for i in range(math.ceil(num_columns / columns_per_select)) ]
    
    def has_escaped_like_wildcards(self, literal: str) -> bool:
        return bool(self.escaped_like_wildcards_re.search(literal))
    
    def has_unescaped_like_wildcards(self, literal: str) -> bool:
        return bool(self.unescaped_like_wildcards_re.search(literal))
    
    def identifier(self, literal: str) -> str:
        return self.escape_curly_braces(self.quoted(literal) if not(self.identifier_quoted_re.search(literal) or self.identifier_regular_re.search(literal)) else literal)
    
    def like_or_equal(self, literal: str) -> str:
        return 'like' if self.has_unescaped_like_wildcards(literal) else '='
    
    def literal(self, literal: str) -> str:
        return "'" + re.sub(r"'", "''", str(literal)) + "'"
    
    def quoted(self, literal: str) -> str:
        return f"{self.quotes[0]}{literal}{self.quotes[1]}"
    
    # private methods