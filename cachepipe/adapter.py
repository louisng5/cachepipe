from abc import ABC, abstractmethod
import io
import numpy as np
from .schemas import Schema,TABLEINDEX,TABLENAME,DBNAME
from .config import _hash_fn,_hash_len
import sqlite3
from contextlib import contextmanager
import os
import pandas as pd
from pdb import set_trace as st
class BaseAdapter(ABC):
    @abstractmethod
    def conn(self,lock=False):
        pass
    @abstractmethod
    def check_source(self):
        pass
    @abstractmethod
    def get_source(self):
        pass

    @abstractmethod
    def get_file_path(self):
        pass

    @staticmethod
    def db_file_exist(db_path):
        return os.path.isfile(db_path)


class SqliteAdapter():
    _INDEX_COL_NAME = 'pipefn_internal_index_'

    def __init__(self,base_db_path,schema_dic,magic_argspec,call_args,call_kwargs,dependency_key,func_call,call_context):
        self.base_db_path = base_db_path
        self.schema = Schema(schema_dic,magic_argspec(*call_args,**call_kwargs))
        self.magic_argspec = magic_argspec
        self.dependency_key = dependency_key
        self.func_call = func_call
        self.call_context = call_context

    def conn(self):
        return sqlite3.connect(self.get_file_path(), timeout=15)

    @contextmanager
    def cx_conn(self,immidiate=False):
        conn = self.conn()
        if immidiate:
            conn.execute('BEGIN immediate ')
        try:
            yield conn
        finally:
            conn.close()

    def check_source(self):
        pass


    def get_source(self):
        pass


    def get_file_path(self):
        return self.base_db_path.format(**self.schema.arg_dic(DBNAME))


    def tablename(self):
        ares_key = self.schema.args_key(TABLENAME)
        table_name_key = self.dependency_key + ares_key
        hash_obj = _hash_fn()
        hash_obj.update(table_name_key.encode())
        return self.func_call.__name__ + hash_obj.hexdigest()


    def db_file_exist(self):
        return os.path.isfile(self.get_file_path())

    @staticmethod
    def table_exist(table_name, conn):
        return conn.execute("SELECT count(1) FROM sqlite_master WHERE type='table' AND name=?",[table_name]).fetchall()[0][0] == 1

    def table_key_exist(self,tableindex_args,table_name, conn):
        keys = tableindex_args.keys()
        return conn.execute("select count(1) from {0} where ".format(table_name) + ' and '.join([self._INDEX_COL_NAME + k + ' = ? ' for k in keys]) + 'LIMIT 1', [tableindex_args[k] for k in keys]).fetchall()[0][0] == 1


    def old_tables(self,table_name, conn):
        table_prefix = table_name[:-_hash_len] + '%'

        return \
        conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name like ? AND length(name) = ?"
                     " AND name != ?",
                     [table_prefix,len(table_name),table_name]).fetchall()

    def remove_old_cache(self,exception_cleanup_fn,normal_cleanup_fn,conn):
        for table_name,_ in self.old_tables(self.tablename(),conn):
            conn.execute("ALTER TABLE  '{0}' RENAME TO '{0}_del'".format(table_name))
            exception_cleanup_fn.append(self.rename_tbl_fn(table_name,table_name + '_del'))
            normal_cleanup_fn.append(self.drop_table_fn(table_name + '_del'))


    def rename_tbl_fn(self,tablename_new,tablename_old):
        def rename_tbl():
            with self.cx_conn() as conn:
                conn.execute("ALTER TABLE  '{0}' RENAME TO '{1}'".format(tablename_old,tablename_new))
        return rename_tbl

    def drop_table_fn(self,tablename):
        def drop_table():
            with self.cx_conn() as conn:
                conn.execute("drop table IF EXISTS {0}".format(tablename))
        return drop_table

    def reg_exception_cleanup(self,fns):
        self.call_context.head._exception_cleanup_fns += fns

    def reg_normal_cleanup(self,fns):
        self.call_context.head._normal_cleanup_fns += fns

    def del_rows(self):
        with self.cx_conn() as conn:
            keys = self.schema.arg_dic(TABLEINDEX).keys()
            conn.execute("delete from {0} where "
                                .format(self.tablename) + ','.join([self._INDEX_COL_NAME + k + ' = ?' for k in keys]),
                                [self.schema.arg_dic(TABLEINDEX)[k] for k in keys])

    def check_source(self):
        if not self.db_file_exist():
            return False

        with self.cx_conn(immidiate=True) as conn:
            if not self.table_exist(self.tablename(), conn):
                return False
            else:
                if self.schema.arg_dic(TABLEINDEX) != {} and not self.table_key_exist(self.schema.arg_dic(TABLEINDEX),
                                                                               self.tablename(), conn):
                    return False
        return True

    def subquery(self):
        pass

    def submit_result(self,df):
        exception_cleanup_fn = []
        normal_cleanup_fn = []
        if self.schema.arg_dic(TABLEINDEX) != {}:
            df = df.assign(**{self._INDEX_COL_NAME + k: v for k, v in self.schema.arg_dic(TABLEINDEX).items()})
        if not self.db_file_exist():
            exception_cleanup_fn.append(lambda: os.remove(self.get_file_path()))
        with self.cx_conn(immidiate=True) as conn:
            if not self.table_exist(self.tablename(), conn):
                exception_cleanup_fn.append(self.drop_table_fn(self.tablename()))
            else:
                if self.schema.arg_dic(TABLEINDEX) != {} and not self.table_key_exist(self.schema.arg_dic(TABLEINDEX),
                                                                                  self.tablename(),
                                                                               conn):
                    exception_cleanup_fn.append(self.del_rows)

            self.remove_old_cache(exception_cleanup_fn, normal_cleanup_fn, conn)

            if len(exception_cleanup_fn) > 0:
                self.reg_exception_cleanup(exception_cleanup_fn)
                df.to_sql(self.tablename(), conn, index=True, index_label=df.index.names, if_exists='append')

    def _get_result_sql(self):
        sql ="select * from {0} ".format(self.tablename())
        if self.schema.arg_dic(TABLEINDEX) != {}:
            keys = self.schema.arg_dic(TABLEINDEX).keys()
            sql = sql + ' where ' + ' and '.join([self._INDEX_COL_NAME + k + ' = ?' for k in keys])
        return sql

    def get_result(self):
        with self.cx_conn() as conn:
            keys = self.schema.arg_dic(TABLEINDEX).keys()
            # faster than pd.read_sql
            cur = conn.execute(self._get_result_sql(),[self.schema.arg_dic(TABLEINDEX)[k] for k in keys])
            df = pd.DataFrame(cur.fetchall(), columns=list(map(lambda x: x[0], cur.description))).drop(
                [self._INDEX_COL_NAME + i for i in self.schema.arg_dic(TABLEINDEX).keys()], axis=1)
        return df