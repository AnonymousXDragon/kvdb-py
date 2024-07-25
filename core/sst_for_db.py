import os
import struct
import json


class SSTableForDb:

  def __init__(self, path):
    self.path = path
    self.data = {}
    self.index = []
    self.load()

  def load(self):
    with open(self.path, "rb") as f:
      while True:
        key_len = f.read(4)
        if not key_len:
          break
        key_len = struct.unpack("I", key_len)[0]
        key = f.read(key_len).decode('utf-8')
        value_len = struct.unpack("I", f.read(4))[0]
        value = f.read(value_len).decode('utf-8')
        self.index.append((key, f.tell() - 4 - key_len - 4 - value_len))
        self.data[key] = value
    # print("data:",self.data)

  def get(self, key):
    return self.data.get(key, None)

  def write(self, data):
    with open(self.path, "wb") as f:
      for k, v in data.items():
        k_encoded = k.encode("utf-8")
        v_encoded = v.encode("utf-8")
        f.write(struct.pack("I", len(k_encoded)))
        f.write(k_encoded)
        f.write(struct.pack("I", len(v_encoded)))
        f.write(v_encoded)
        self.index.append(
            (k, f.tell() - 4 - len(k_encoded) - 4 - len(v_encoded)))
