from database import Database

db = Database("mydb")

collection = db.create_collection("users")
collection1 = db.create_collection("states")

collection.insert('user1', {'name': 'Winner1', 'age': 25 , 'job': "sd"})

print(collection.find("user1"))

print(len(collection.memtable.data))
print(db.get_collection("states"))

collection.compact()
print(collection.memtable.data)
