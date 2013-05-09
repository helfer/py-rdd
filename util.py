import types
import collections
import marshal
import pickle
import base64


def pds(*args,**kwargs):
  if len(kwargs) != 0:
    raise Exception("kwargs pickle not implemented yet")
  x =  base64.b64encode(pickle.dumps(args))
  return x

def pls(p):
  return pickle.loads(base64.b64decode(p))


def encode_function(function):
  print function
  return marshal.dumps(function.func_code)

def decode_function(encoded_function):
  func_code = marshal.loads(encoded_function)
  return types.FunctionType(func_code, globals())

def flatten(l):
  for el in l:
    if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
      for sub in flatten(el):
        yield sub
      else:
        yield el
