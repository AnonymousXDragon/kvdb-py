import bisect
import os
import struct


class SSTable:

  def __init__(self, path):
    self.path = path
    self.index = []
    self.load_index()

  def load_index(self):
    with open(self.path, 'rb') as f:
      while True:
        key_len = f.read(4)
        if not key_len:
          break

        key_len = struct.unpack('I', key_len)[0]
        key = f.read(key_len).decode('utf-8')
        value_len = struct.unpack('I', f.read(4))[0]
        value = f.read(value_len).decode('utf-8')
        self.index.append((key, value))

  def get(self, key):
    index = [k for k, _ in self.index]
    pos = bisect.bisect_left(index, key)
    if pos < len(self.index) and self.index[pos][0] == key:
      return self.index[pos][1]
    return None
