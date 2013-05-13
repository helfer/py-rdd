import types
import collections
import marshal
import pickle
import base64


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
