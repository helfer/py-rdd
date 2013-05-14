import types
import collections
import marshal
import pickle
import base64
import httplib
import xmlrpclib

class HTTP_with_timeout(httplib.HTTP):
    def __init__(self, host='', port=None, strict=None, timeout=5.0):
        if port == 0: port = None
        self._setup(self._connection_class(host, port, strict, timeout=timeout))

    def getresponse(self, *args, **kw):
        return self._conn.getresponse(*args, **kw)

class TimeoutTransport(xmlrpclib.Transport):
    timeout = 10.0
    def set_timeout(self, timeout):
        self.timeout = timeout
    def make_connection(self, host):
        h = HTTP_with_timeout(host, timeout=self.timeout)
        return h

def capture_globals(func):
  output = {}
  for k, v in func.func_globals.items():
    try:
      output[k] = marshal.dumps(v)
    except ValueError:
      pass
  return output

def recover_globals(globals_dict):
  output = {}
  for k, v in globals_dict.items():
    output[k] = marshal.loads(v)
  output.update(globals())
  return output

def pds(*args,**kwargs):
  if len(kwargs) != 0:
    raise Exception("kwargs pickle not implemented yet")
  x =  base64.b64encode(pickle.dumps(args))
  return x

def pls(p):
  return pickle.loads(base64.b64decode(p))


def encode_function(function):
  return marshal.dumps((function.func_code, capture_globals(function)))

def decode_function(encoded_function):
  func_code, func_globals = marshal.loads(encoded_function)
  func_globals = recover_globals(func_globals)
  return types.FunctionType(func_code, func_globals)

## TODO: broken
def flatten(l):
  for el in l:
    if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
      for sub in flatten(el):
        yield sub
      else:
        yield el
