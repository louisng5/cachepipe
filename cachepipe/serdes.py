from abc import ABC, abstractmethod
import pandas
from pdb import set_trace as st
import json
import pickle
class matecls(type):
    def __new__(cls, name, bases, clsdict):
        new_cls = super().__new__(cls, name, bases, clsdict)

        assert_has_return_type = False
        return_type = None
        type_cls_dic = None
        if 'return_type' in clsdict:
            return_type = clsdict['return_type']
        for base in bases:
            if base._assert_sub_class_has_return_type:
                assert_has_return_type = True
            if not return_type and hasattr(base,'return_type'):
                return_type = base.return_type
            if hasattr(base,'_type_cls_dic'):
                type_cls_dic = base._type_cls_dic

        if assert_has_return_type:
            if type_cls_dic is not None and return_type:
                try:
                    if isinstance(return_type,type):
                        type_cls_dic[return_type] = new_cls
                    else:
                        for tp in return_type:
                            type_cls_dic[tp] = new_cls
                except:
                    raise Exception('return_type should be either type or iterable.')
            else:
                raise Exception('return_type needed.')
        return new_cls

class BaseSerDes(metaclass=matecls):
    _assert_sub_class_has_return_type = False
    @classmethod
    def _serialize(cls,data):
        pass

    @classmethod
    def _deserialize(cls,date):
        pass

class TypeDefaultSerDes(BaseSerDes):
    _assert_sub_class_has_return_type = True
    _type_cls_dic = {}

class _BasePandasSerDes():
    _index_prefix = '__pipeindex__'
    _assert_sub_class_has_return_type = True
    return_type = pandas.core.frame.DataFrame
    @classmethod
    def _serialize(cls,df):
        df = cls.serialize(df)
        df.index.names = [PandasSerDes._index_prefix + (str(i) if i else '') for i in df.index.names]
        return df

    @classmethod
    def _deserialize(cls,df):
        df = df.set_index([i for i in df.columns if i.startswith(PandasSerDes._index_prefix)])
        df.index.names = [i[len(PandasSerDes._index_prefix):] if PandasSerDes._index_prefix != i else None for i in df.index.names]
        return cls.deserialize(df)

class _BaseObjectSerDes():
    _assert_sub_class_has_return_type = True
    try:
        basestring
    except NameError:
        basestring = str
    return_type = [basestring,int,float,bool]
    @classmethod
    def _serialize(cls,data):
        # subclass.serialize should phase data to sqlite support data type
        data = cls.serialize(data)
        df = pandas.DataFrame([[data]], columns=['data'])
        return df

    @classmethod
    def _deserialize(cls,df):
        data = df['data'].values[0]
        return cls.deserialize(data)

class BaseObjectSerDes(_BaseObjectSerDes):
    @staticmethod
    def serialize(data):
        return data

    @staticmethod
    def deserialize(data):
        return data


class BasePandasSerDes(_BasePandasSerDes):
    @staticmethod
    def serialize(df):
        return df

    @staticmethod
    def deserialize(df):
        return df

class JsonSerDes(BaseObjectSerDes, TypeDefaultSerDes):
    return_type = [dict,list,tuple]
    serialize = json.dumps
    deserialize = json.loads

class ObjectSerDes(BaseObjectSerDes, TypeDefaultSerDes):
    pass
class PandasSerDes(BasePandasSerDes,TypeDefaultSerDes):
    pass

class PickleSerDes(BaseObjectSerDes):
    serialize = pickle.dumps
    deserialize = pickle.loads

# print(PandasSerDes._type_cls_dic)
# print(DefaultProxySerDes._type_cls_dic)