import types
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
  return marshal.dumps((function.func_code, function.func_closure))

def decode_function(encoded_function):
  func_code, func_closure = marshal.loads(encoded_function)
  return types.FunctionType(func_code, globals(), closure = func_closure)
