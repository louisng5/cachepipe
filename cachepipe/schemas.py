try:
  basestring
except NameError:
  basestring = str
import numbers


class SchemaItem():
    pass

class TABLENAME(SchemaItem):
    pass

class TABLEINDEX(SchemaItem):
    pass

class DBNAME(SchemaItem):
    pass

class IGNORE(SchemaItem):
    pass

class Schema():
    def __init__(self, dic, argspec):
        self.sehema_dic = dic
        self.argspec = argspec
        self._arg_dic_cache = {}

    def arg_dic(self,type):
        if type not in self._arg_dic_cache:
            self._arg_dic_cache[type] = {k: self.argspec[k] for k, v in self.sehema_dic.items() if v == type}
        return self._arg_dic_cache[type]

    def args_key(self,type):
        return _args_key(self.arg_dic(type))


def _args_key(args_dic):
    sorted_args = ((i,args_dic[i]) for i in sorted(args_dic))
    return ','.join([k + ':' + _to_string(v) for k,v in sorted_args])


def _to_string(obj):
    try:
        hash(obj)
    except TypeError as ex:
        raise ex
    for dtype in objtype_to_string_dic:
        if isinstance(obj,dtype):
            return objtype_to_string_dic[dtype](obj)

def _obj_string(obj):
    type_px = type(obj).__name__
    return type_px + ':' + str(obj)

def _tuple_to_string(obj):
    type_px = type(obj).__name__
    return type_px + ':(' + ':'.join([_to_string(i) for i in obj]) + ')'

objtype_to_string_dic = {
    numbers.Number:_obj_string,
    tuple:_tuple_to_string,
    basestring:_obj_string
}