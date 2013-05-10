from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from pymongo import MongoClient
import re

client = MongoClient()
db = client.testDB
businesses = db.testBiz

total = set()

class ProcessBiz(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def extract_ratings(self, _, record):
        """Take in a record, pass rating info to reducer"""
        if record['type'] == 'review':
            yield [record['business_id'], (record['user_id'], record['stars'])]
        if record['type'] == 'business':
            yield [record['business_id'], record['stars']]

            ##/

    def store_db(self, business_id, emit):
        """store ratings info into database"""
        for value in emit:
            # print emit
            temp = businesses.find_one({'business_id': business_id})
            if temp:
                if type(value) == float:
                    businesses.update({'business_id': business_id}, {'average_stars': float(value)})
                    # temp.['average_stars'] = float(value[0])
                else:
                    new = temp['ratings'].append((value[0], float(value[1])))
                    businesses.update({'business_id': business_id}, {'ratings': new})
            else:
                store = {'business_id': business_id,
                        }
                if type(value) == float:
                    store['average_stars'] = float(value)
                    store['ratings'] = []
                else:
                    store['average_stars'] = -1.0
                    store['ratings'] = [(value[0], float(value[1]))]
                store_id = businesses.insert(store)
        ##/


    def steps(self):
        return [self.mr(self.extract_ratings, self.store_db)]

if __name__ == '__main__':
    ProcessBiz.run()
