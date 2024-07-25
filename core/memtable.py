import struct
import os
import json


class Memtable:

  def __init__(self) -> None:
    self.data = {}

  def put(self, key, value):
    self.data[key] = value

  def get(self, key):
    if key in self.data:
      return self.data[key]
    return None

  def flush(self, path):
    sorted_data = sorted(self.data.items())
    with open(path, 'wb') as f:
      for key, value in sorted_data:
        key_encoded = key.encode('utf-8')
        value_encoded = value.encode('utf-8')
        f.write(struct.pack('I', len(key_encoded)))
        f.write(key_encoded)
        f.write(struct.pack('I', len(value_encoded)))
        f.write(value_encoded)
    self.data = {}

  def flush_for_db(self, path):
    sorted_data = sorted(self.data.items())
    with open(path, 'wb') as f:
      for key, value in sorted_data:
        key_encoded = key.encode('utf-8')
        value_encoded = json.dumps(value).encode('utf-8')
        f.write(struct.pack('I', len(key_encoded)))
        f.write(key_encoded)
        f.write(struct.pack('I', len(value_encoded)))
        f.write(value_encoded)
    self.data = {}
    print("after", self.data)
