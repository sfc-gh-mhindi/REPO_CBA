import teradatasql
import os

from misc import const
from .connection import Connection

class TeradataConnection(Connection):
    def __init__(self, details, logger):
        self.host = None
        self.user = None
        self.password = None
        self.arraysize = None
        self.logmech = None
        self.kerberos_keytab = None
        self.kerberos_user = None
        self.tz_info = {}
        super().__init__(details, logger)
        self.details = { const.Host: self.host, const.User: self.user, const.Password: self.password }
        self.arraysize = 1 if self.arraysize is None else self.arraysize
        self.get_connection()
        for (k, v) in self.fetchone(f"""with v as (
    select infodata as version from dbc.dbcinfo where infokey = 'VERSION'
), tz as (
    select
        extract(timezone_hour from current_timestamp) - extract(timezone_hour from current_timestamp at time zone interval '00:00' hour to minute) as session_tzh,
        extract(timezone_minute from current_timestamp) - extract(timezone_minute from current_timestamp at time zone interval '00:00' hour to minute) as session_tzm
), l as (
    select
        max(case when tablename = 'Dbase' and columnname = 'DatabaseName' then columnlength end) as database_name_length,
        max(case when tablename = 'TVM' and columnname = 'TVMName' then columnlength end) as table_name_length,
        max(case when tablename = 'TVFields' and columnname = 'FieldName' then columnlength end) as column_name_length
    from
        dbc.columnsv
    where
        databaseName = 'DBC'
        and tablename in ('Dbase', 'TVM', 'TVFields')
        and columnname in ('DatabaseName', 'TVMName', 'FieldName')
)
select * from v, tz, l""").items():
            setattr(self, k, v)
        if const.Timezone in self.tz_info:
            _ = self.execute(f"set time zone '{self.tz_info[const.Timezone]}'")
        else:
            self.session_tzr = f"{str(self.session_tzh).replace('+', '').zfill(2)}:{str(self.session_tzm).zfill(2)}"
            self.tzr = f"{str(self.tz_info.get(const.Tzh, self.session_tzh)).replace('+', '').zfill(2)}:{str(self.tz_info.get(const.Tzm, self.session_tzm)).zfill(2)}"
            if self.session_tzr != self.tzr:
                _ = self.execute(f"set time zone interval '{self.tzr}' hour to minute")
    
    def connection_closed(self):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute('select 1')
            return False
        except:
            return True

    def get_connection(self):
        try:
            if all(k is not None for k in [ self.kerberos_keytab, self.kerberos_user, self.host, self.logmech ]):
                os.system(f'kinit -kt {self.kerberos_keytab} {self.kerberos_user}')
                self.connection = teradatasql.connect(host=self.host, logmech=self.logmech)
            else:
                self.connection = teradatasql.connect(**self.details)
        except Exception as e:
            self.logger.critical(e)
            raise e
    
    def get_cursor(self):
        cursor = self.connection.cursor()
        cursor.arraysize = self.arraysize
        return cursor
    
    @staticmethod
    def check_details(**details):
        details = { k.lower(): v for k, v in details.items() if v is not None }
        if not(all(k in list(details.keys()) for k in [ const.Type, const.Host ]) and (all(k in list(details.keys()) for k in [ const.User, const.Password ]) or const.Logmech in list(details.keys()))):
            raise KeyError(f'{const.Type}, {const.Host} and either ({const.User}, {const.Password}) or {const.Logmech} must be present in details')
        return details
