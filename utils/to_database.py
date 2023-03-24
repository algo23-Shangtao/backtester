import pymongo

class DataBaseMongo(object):
    def __init__(self, db='ctp_db', collect='rb'):
        self.client = pymongo.MongoClient('localhost:27017')
        self.db = self.client[db]
        self.collection = self.db[collect]
    def insert_data(self, data_list):    
        self.collection.insert_many(data_list)