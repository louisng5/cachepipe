import sqlite3
import pandas as pd
from .config import _hash_len
import json
import os
from contextlib import contextmanager
from .adapter import SqliteAdapter
_INDEX_COL_NAME = 'pipefn_internal_index_'

class CallContext():
    def __init__(self,pipefn,schema,func_call,caller_context,call_args,call_kwargs,dependency_key,magic_argspec):
        self.adapter = SqliteAdapter(pipefn.pipe.base_db_path,schema,magic_argspec,call_args,call_kwargs,dependency_key,func_call,self)
        self.pipefn = pipefn
        self.func_call = func_call
        self.call_args=call_args
        self.call_kwargs = call_kwargs
        if not caller_context:
            self.head = self
            self._exception_cleanup_fns = []
            self._normal_cleanup_fns = []
        else:
            self.head = caller_context.head


    def get_source(self):
        try:
            if not self.adapter.check_source():
                df = self.pipefn.serdes._serialize(
                    (
                        self.func_call(*self.call_args,**self.call_kwargs)
                    )
                )
                self.adapter.submit_result(df)

            return self.pipefn.serdes._deserialize(self.adapter.get_result())

        except Exception as ex:
            if hasattr(self,'_exception_cleanup_fns'):
                for fn in reversed(self._exception_cleanup_fns):
                    fn()
            raise ex
        else:
            if hasattr(self,'_normal_cleanup_fns'):
                for fn in reversed(self._exception_cleanup_fns):
                    fn()

    def remove_old_cache(self, table_name, conn):
        normal_name = table_name[:-56]
        table_df = pd.read_sql("SELECT name as tbl_name from sqlite_master WHERE type='table'",conn)
        table_df['normal_name'] = table_df.tbl_name.apply(lambda x: x[:-56])
        table_df = table_df[table_df.normal_name == normal_name]
        cur = conn.cursor()
        for tbl in table_df.tbl_name.values:
            if table_name != tbl:
                cur.execute('drop table ' + tbl)