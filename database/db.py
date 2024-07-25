import os
from .collection import Collection


class Database:

  def __init__(self, path) -> None:
    if not os.path.exists(path):
      os.makedirs(path)
    self.path = path
    self.collections = {}

  def create_collection(self, name):
    collection_path = os.path.join(self.path, name)
    if not os.path.exists(collection_path):
      os.makedirs(collection_path)
    collection = Collection(name, self.path)
    self.collections[name] = collection
    return collection

  def get_collection_or_create(self, name):
    collection_path = os.path.join(self.path, name)
    if not os.path.exists(collection_path):
      collection = Collection(name, self.path)
      self.collections[name] = collection
    return self.collections[name]

  def get_collection(self, name):
    return self.collections.get(name, None)
