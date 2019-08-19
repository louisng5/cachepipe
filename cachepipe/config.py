import hashlib
_hash_fn = hashlib.sha256
_hash_obj = _hash_fn()
_hash_obj.update('a'.encode())
_hash_len = len(_hash_obj.hexdigest())
del _hash_obj