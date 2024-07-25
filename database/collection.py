from core import Memtable, SSTableForDb, WAL
import glob
import os
import json


class Collection:

  def __init__(self, name, path) -> None:
    self.name = name
    self.db_path = path
    self.memtable = Memtable()
    self.sstables = []
    self.wal = WAL(os.path.join(path, name, "wal.log"))
    self.load_sstables()
    self.recover_data()

  def load_sstables(self):
    sstables = glob.glob(
        os.path.join(self.db_path, f"sstable-{len(self.sstables)}.sst"))
    self.sstables = [SSTableForDb(f) for f in sstables]

  def recover_data(self):
    logs = self.wal.recover()
    for k, v in logs:
      self.memtable.put(k, v)

  def insert(self, key, document):
    self.wal.log(key, document)
    self.memtable.put(key, json.dumps(document))
    if len(self.memtable.data) > 10:
      self.flush_memtable()

  def manual_flush(self):
    self.flush_memtable()

  def find(self, name):
    document = self.memtable.get(name)
    if document is not None:
      return json.loads(document)
    for sstable in reversed(self.sstables):
      document = sstable.get(name)
      if document is not None:
        return json.loads(document)
    return None

  def flush_memtable(self):
    sstable_path = os.path.join(self.db_path, self.name,
                                f"sstable-{len(self.sstables)}.sst")
    self.memtable.flush_for_db(sstable_path)
    self.sstables.append(SSTableForDb(sstable_path))
    self.wal.close()
    os.remove(self.wal.path)
    self.wal = WAL(os.path.join(self.db_path, self.name, "wal.log"))

  def compact(self):
    if len(self.sstables) < 2:
      return
    merged_data = {}
    new_sstable_path = os.path.join(self.db_path, self.name,
                                    f"sstable-{len(self.sstables)}.sst")
    for sstable in self.sstables:
      merged_data.update(sstable.data)
    sstable = SSTableForDb(new_sstable_path)
    sstable.write(merged_data)
    self.sstables = [sstable]
