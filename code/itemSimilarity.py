from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from pymongo import MongoClient
import re
import json
import sys

WORD_RE = re.compile(r"[\w']+")
client = MongoClient()
db = client.testDB
users = db.allUsers

total = set()

class ProcessUser(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def extract_ratings(self, _, record):
        """Take in a record, pass rating info to reducer"""
        if record['type'] == 'review':
            yield [record['user_id'], (record['business_id'], record['stars'])]
        if record['type'] == 'business':
            yield [record['user_id'], record['average_stars']]

            ##/

    def store_db(self, user_id, emit):
        """store ratings info into database"""
        for value in emit:
            # print emit
            temp = users.find_one({'user_id': user_id})
            if temp:
                if type(value) == float:
                    users.update({'user_id': user_id}, {'average_stars': float(value)})
                    # temp.['average_stars'] = float(value[0])
                else:
                    new = temp['ratings'].append((value[0], float(value[1])))
                    users.update({'user_id': user_id}, {'ratings': new})
            else:
                store = {'user_id': user_id,
                        }
                if type(value) == float:
                    store['average_stars'] = float(value)
                    store['ratings'] = []
                else:
                    store['average_stars'] = -1.0
                    store['ratings'] = [(value[0], float(value[1]))]
                store_id = users.insert(store)
        ##/

    def store_json(self, user_id, emit):
        rc = {'user_id': user_id, 'ratings': []}
        for value in emit:
            if type(value) == float:
                rc['average_stars'] = value
            else:
                rc['ratings'].append((value[0], float(value[1])))
        std = 0.0
        if not 'average_stars' in rc.keys():
            rc['average_stars'] = sum([x[1] for x in rc['ratings']])\
                    /len(rc['ratings'])
        for t in rc['ratings']:
            std += (t[1] - rc['average_stars']) ** 2
        
        if len(rc['ratings']) != 0:
            std = std/len(rc['ratings'])
        std = std**0.5
        rc['std'] = std
        if len(rc['ratings']) > 5:
            # print 'good stuff!', user_id
            sys.stderr.write('good user! ' + str(len(rc['ratings'])) + ' \n')
        yield ('', json.dumps(rc))


    def steps(self):
        return [self.mr(self.extract_ratings, self.store_json)]

if __name__ == '__main__':
    ProcessUser.run()
