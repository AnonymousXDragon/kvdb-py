import glob
import os
import struct
from .memtable import Memtable
from .sstable import SSTable


class LsmTree:

  def __init__(self, dir):
    self.memtable = Memtable()
    self.dir = dir
    if not os.path.exists(dir):
      os.makedirs(dir)
    self.sstables = self.load_sstable()

  def load_sstable(self):
    sstable_files = glob.glob(os.path.join(self.dir, '.sst'))
    return [SSTable(f) for f in sstable_files]

  def put(self, key, value):
    self.memtable.put(key, value)
    if len(self.memtable.data) > 10:
      self.flush_memtable()

  def get(self, key):
    if self.memtable.get(key) is not None:
      return self.memtable.get(key)

    for sstable in reversed(self.sstables):
      value = sstable.get(key)
      if value is not None:
        return value

    return None

  def flush_memtable(self):
    sstable_path = os.path.join(self.dir, f"sstable-{len(self.sstables)}.sst")
    self.memtable.flush(sstable_path)
    self.sstables.append(SSTable(sstable_path))

  def compact(self):
    if len(self.sstables) < 2:
      return
    new_ssltable_path = os.path.join(self.dir,
                                     f"sstable-{len(self.sstables)}.sst")
    with open(new_ssltable_path, "wb") as f:
      merged_data = {}
      for sstable in self.sstables:
        for key, value in sstable.index:
          merged_data[key] = value
        sorted_table = sorted(merged_data.items())
        for k, v in sorted_table:
          key_encoded = k.encode('utf-8')
          value_encode = v.encode('utf-8')
          f.write(struct.pack("I", len(key_encoded)))
          f.write(key_encoded)
          f.write(struct.pack("I", len(value_encode)))
          f.write(value_encode)
    self.sstables = [SSTable(new_ssltable_path)]
