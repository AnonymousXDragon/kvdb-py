# write ahead log
# all changes are logged before they are applied to memtable or sstable
# if system crashes the log can be replayed to recover to the leatest state
# durablity: if system crashes, the log can be replayed to recover to the leatest state

import os
import struct
import json


class WAL:

  def __init__(self, path) -> None:
    self.path = path
    self.file = open(path, "a+b")

  def log(self, key, value):
    key_encoded = key.encode("utf-8")
    val_encoded = json.dumps(value).encode("utf-8")
    self.file.write(struct.pack("I", len(key_encoded)))
    self.file.write(key_encoded)
    self.file.write(struct.pack("I", len(val_encoded)))
    self.file.write(val_encoded)
    self.file.flush()

  def recover(self):
    self.file.seek(0)
    logs = []
    while True:
      key_len = self.file.read(4)
      if not key_len:
        break
      key_len = struct.unpack("I", key_len)[0]
      key = self.file.read(key_len).decode("utf-8")
      value_len = struct.unpack("I",self.file.read(4))[0]
      value = self.file.read(value_len).decode("utf-8")
      logs.append((key,json.loads(value)))
    return logs

  def close(self):
    self.file.close()
