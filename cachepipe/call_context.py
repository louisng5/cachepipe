import sqlite3
import pandas as pd
from .config import _hash_len
import json
import os
from contextlib import contextmanager
import inspect
from pdb import set_trace as st
from .adapter import SqliteAdapter
_INDEX_COL_NAME = 'pipefn_internal_index_'

class CallContext():
    def __init__(self,pipefn,schema,func_call,call_args,call_kwargs,dependency_key,magic_argspec):
        self.adapter = SqliteAdapter(pipefn.pipe.base_db_path,schema,magic_argspec,call_args,call_kwargs,dependency_key,func_call,self)
        self.pipefn = pipefn
        self.func_call = func_call
        self.call_args=call_args
        self.call_kwargs = call_kwargs
        caller_context = self.get_caller_context()
        if not caller_context:
            self.head = self
            self._exception_cleanup_fns = []
            self._normal_cleanup_fns = []
        else:
            self.head = caller_context.head

    def get_caller_context(self):
        caller_context = self._caller_context()
        if self._call_eligible(caller_context):
            return caller_context
        else:
            raise Exception('You are trying to call {0}() from {1}() which is not specified in dependencies.'.format(
                self.pipefn.__name__,
                caller_context.pipefn.__name__
            )
            )

    def _caller_context(self):
        frame = inspect.currentframe().f_back
        while frame:
            if '_pipe_call_context' in frame.f_locals:
                return frame.f_locals['_pipe_call_context']
            else:
                frame = frame.f_back
        return

    def _call_eligible(self,caller_context):
        return (caller_context is None) or (self.pipefn in caller_context.pipefn.dependencies)

    def call(self):
        _pipe_call_context = self
        return self.func_call(*self.call_args,**self.call_kwargs)

    @contextmanager
    def tran(self):
        try:
            yield
        except Exception as ex:
            if hasattr(self,'_exception_cleanup_fns'):
                for fn in reversed(self._exception_cleanup_fns): fn()
            raise ex
        else:
            if hasattr(self,'_normal_cleanup_fns'):
                for fn in reversed(self._normal_cleanup_fns): fn()


    def _check_source(self):
        return self.adapter.check_source()

    def _cache_source(self):
        if not self._check_source():
            df = self.pipefn.serdes._serialize(
                    self.call()
            )
            self.adapter.submit_result(df)

    def _get_source(self):
        self._cache_source()
        return self.pipefn.serdes._deserialize(self.adapter.get_result())

    def get_source(self):
        return self.with_tran(self._get_source)

    def cache_source(self):
        return self.with_tran(self._cache_source)

    def check_source(self):
        return self.with_tran(self._check_source)

    def with_tran(self,fn):
        with self.tran():
            return fn()


