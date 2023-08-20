from pymongo import MongoClient
from bson.objectid import ObjectId

class DatabaseLayer(object):
    def __init__(self, host, port, username, password) :
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        # Store all credentials into a single URI:
        self.uri = 'mongodb://' + username + ':' + password + '@' + host + ':' + port
        print(self.uri)
    
    def connect(self, logging=False):
        self.connection = MongoClient(self.uri)
        if logging:
            # show dbs in mongo shell
            print(self.connection.list_database_names())
    
    # Set the current Mongo database for queries
    # similar to mongo shell's use command
    # Params: specify database to use as a string
    def setDatabase(self, database) :
        self.db = self.connection[database]
        
    # This method implements a read capability, similar to mongo shell's find or findOne()
    # Specify the collection as a string and specify the filter as a Python dictionary
    # Returns a Mongo cursor
    def read(self, collection, filter={}):
        # Attempts to read from the collection
        try:
            c = self.db[collection]
            result = list(c.find(filter))
            return result
        # Throws exception if unable to read from the collection
        except Exception as e:
            print("Error with Reading: {e}")
            return []
    
    # Adding write functionality for second part of assignment:
    def create(self, collection, data):
        # Attempts to create an entry in the collection
        try:
            c = self.db[collection]
            c.insert_one(data)
            return True
        # Throws exception if unable to create an entry
        except Exception as e:
            print("Error with Creation: {e}")
            return False

    # Adding update functionality
    def update(self, collection, filter, data):
        # Attempts to update an entry in the collection
        try:
            c = self.db[collection]
            result = c.update_many(filter, {'$set': data})
            return result.modified_count
        # Throws exception if unable to update an entry
        except Exception as e:
            print("Error with Updating: {e}")
            return 0

    # Adding delete functionality
    def delete(self, collection, filter):
        # Attempts to delete an entry in the collection
        try:
            c = self.db[collection]
            result = c.delete_many(filter)
            return result.deleted_count
        # Throws exception if unable to delete an entry
        except Exception as e:
            print("Error with Deletion: {e}")
            return 0
