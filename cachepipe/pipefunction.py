from pdb import set_trace as st
import inspect
from functools import update_wrapper,partial
import numbers
from .config import _hash_fn
from .call_context import CallContext
from .serdes import BaseSerDes,TypeDefaultSerDes
from abc import ABC, abstractmethod

try:
  basestring
except NameError:
  basestring = str

fn_s = """
def magic_func {0}:
    return inspect.getargvalues(inspect.currentframe()).locals
                """

TABLENAME = 0
TABLEINDEX = 1
DBNAME = 2
tb = None

class Dependency(ABC):
    @abstractmethod
    def dependency_key(self):
        pass

    @property
    def __name__(self):
        return self.__class__.__name__

class Ver(Dependency):
    def __init__(self,ver_num):
        assert isinstance(ver_num,int)
        self.ver = str(ver_num)

    def dependency_key(self):
        return 'ver:' + self.ver

class PipeFunction:
    def __init__(self,pipe,ver,schema,dependencies,preturn_type,serdes, fn):
        self.fn = fn
        update_wrapper(self, fn)
        self.pipe = pipe
        self.magic_argspec,self.args,anno_return_type = _analyse_argspec(fn)
        if preturn_type == Annotation:
            preturn_type = anno_return_type
        if all([preturn_type,anno_return_type]):
            assert preturn_type == anno_return_type
            return_type = preturn_type
        else:
            return_type = preturn_type if preturn_type else anno_return_type

        self.serdes = self.pipe.get_serdes(return_type) if not serdes else serdes
        self.ver = Ver(ver)
        if len(schema) != len(self.args):
            raise Exception(set(fn) + ' has ' + str(len(self.args)) + ' positional argument(s), while length of schema is ' + str(len(schema)) + '.')
        self.schema = self.magic_argspec(*schema)
        self._dependencies = set(dependencies)
        self._dependencies.add(self.ver)
        self.normal_cleanup_fn = []
        self.exception_cleanup_fn = []
        pipe.reg(self)

    @property
    def dependencies(self):
        # print('hit')
        return {self._get_func_from_pipe(i) if isinstance(i,str) else self._assert_dependencies_type(i) for i in self._dependencies}

    def _assert_dependencies_type(self,i):
        if not (isinstance(i,PipeFunction) or isinstance(i,Dependency)):
            raise TypeError(str(i) + ' dependencies should be of either "PipeFunction" or "Dependency" type. ')
        return i

    def _get_func_from_pipe(self,funcname):
        try:
            return self.pipe.fns[funcname]
        except KeyError as ex:
            raise KeyError('Cannot find function ' + funcname)

    def _sorted_dependencies(self):
        return sorted(self.dependencies,key=lambda x:x.__name__)

    def dependency_key(self):
        if not hasattr(self, '_dependency_key'):
            self._dependency_key =  "fn:" + self.fn.__name__ + "(" + ''.join(["(" + i.dependency_key() + ")" for i in self._sorted_dependencies()]) + ")"
        return self._dependency_key

    def tablename(self,*args, **kwargs):
        args_dic = {k:v for k,v in self.magic_argspec(*args, **kwargs).items() if k in self._tablename_args}
        ares_key = self._args_key(args_dic)
        table_name_key = self.dependency_key() + ares_key
        hash_obj = _hash_fn()
        hash_obj.update(table_name_key.encode())
        return self.__name__ + hash_obj.hexdigest()

    def __call__(self, *args, **kwargs):

        if not self.pipe.initialized:
            self.pipe.initialize()
        caller_context = self._caller_context()
        if self._call_eligible(caller_context):
            _pipe_call_context = CallContext(self,
                                             schema=self.schema,
                                             func_call=self.fn,
                                             # tablename=call_tablename,
                                             caller_context=caller_context,
                                             call_args = args,
                                             call_kwargs = kwargs,
                                             dependency_key = self.dependency_key(),
                                             magic_argspec=self.magic_argspec)

            return _pipe_call_context.get_source()
        else:
            raise Exception('You are trying to call {0}() from {1}() which is not specified in dependencies.'.format(
                self.__name__,
                caller_context.pipefn.__name__
            )
            )

    def _cached_call(self,):
        pass

    def _args_key(self,args_dic):
        sorted_args = ((i,args_dic[i]) for i in sorted(args_dic))
        return ','.join([k + ':' + _to_string(v) for k,v in sorted_args])

    def _caller_context(self):
        frame = inspect.currentframe().f_back
        while frame:
            if '_pipe_call_context' in frame.f_locals:
                return frame.f_locals['_pipe_call_context']
            else:
                frame = frame.f_back
        return

    def _call_eligible(self,caller_context):
        return (caller_context is None) or (self in caller_context.pipefn.dependencies)

    def __repr__(self):
        return "<" + self.__module__ + " PipeFunction (" + self.fn.__repr__() + ")>"

    # def __getattr__(self, item):
    #     if item in ['__defaults__','__globals__','__annotations__','__kwdefaults__','__code__']:
    #         return getattr(self.fn,item)
    #     else:
    #         raise AttributeError(item)


def _analyse_argspec(fn):
    try:
        from inspect import signature
        def magic_func(*arg,**kwargs):
            sig = signature(fn)
            ba = sig.bind(*arg,**kwargs)
            ba.apply_defaults()
            return ba.arguments
        sg = signature(fn)
        return magic_func,\
               sg.parameters,\
               None if sg.return_annotation is inspect._empty else sg.return_annotation
    except ImportError:
        try:
            asp = inspect.getfullargspec(fn)
        except:
            asp = inspect.getargspec(fn)
        exec(fn_s.format(inspect.formatargspec(*[v if k != 'annotations'else {} for v,k in zip(asp,asp._fields)])),
             {'fn': fn, 'inspect': inspect},
             locals()
             )
        try:
            return_type = asp.annotations['return']
        except:
            return_type = None
        return locals()['magic_func'],\
               inspect.getargspec(fn).args,\
               return_type

class Annotation():
    pass

class CachePipe():
    def __init__(self,base_db_path):
        self.calling = None
        self.fns = {}
        self.base_db_path = base_db_path
        self.initialized = False
        class PipeSerDes(BaseSerDes):
            _type_cls_dic = {}
            _assert_sub_class_has_return_type = True
        self.SerDes = PipeSerDes

    def get_serdes(self,_type):
        if _type in self.SerDes._type_cls_dic:
            return self.SerDes._type_cls_dic[_type]
        else:
            return TypeDefaultSerDes._type_cls_dic[_type]

    def initialize(self):
        # for i in self.fns.values():
        #     i.update_dependencies()
        for i in self.topsorted:
            # trigger dependency_key property in topological sort order, to avoid recursion limit
            i.dependency_key()
        self.initialized = True

    def func(self,ver,schema,dependencies=[],return_type=Annotation,serdes=None):
        assert isinstance(return_type,type)
        return partial(PipeFunction,self,ver,schema,dependencies,return_type,serdes)

    def reg(self,fn):
        if fn.__name__ in self.fns:
            raise Exception('Function with name ' + fn.__name__ + ' has already been registered')
        self.fns[fn.__name__] = fn

    def _fn_graph(self):
        return {v:set([i for i in v.dependencies if isinstance(i,PipeFunction)]) for k,v in self.fns.items()}

    @property
    def topsorted(self):
        if not hasattr(self, '_topsorted'):
            self._topsorted = top_sort(self._fn_graph())
        return self._topsorted

def top_sort(graph):
    # iterative top sort with loop detection in place.
    # graph will be modified in place
    # graph: {object:set([object,object]),}
    v = set()
    out = []
    while len(v) < len(graph):
        s =list(graph.keys() - v)[0]
        q=[s]
        vsup = set([s])
        while q:
            if graph[q[-1]]:
                x=next(iter(graph[q[-1]]))
                if x in vsup:
                    raise Exception('loop')
                q += [x]
                vsup.add(x)
            else:
                n=q.pop()
                if q:
                    graph[q[-1]].remove(n)
                if not n in v:
                    v.add(n)
                    out += [n]
    return out

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
