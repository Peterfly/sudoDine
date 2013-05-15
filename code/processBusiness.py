from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from pymongo import MongoClient
import re
import sys, json
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
            yield [record['business_id'], (record['stars'], record['name'])]

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

    def store_json(self, business_id, emit):
        rc = {'business_id': business_id, 'ratings': []}
        for value in emit:
            if type(value[0]) == float:
                rc['name'] = value[1]
                rc['average_stars'] = value[0]
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
        # yield ('', json.dumps(rc))
        print json.dumps(rc)

    def steps(self):
        return [self.mr(self.extract_ratings, self.store_json)]

if __name__ == '__main__':
    ProcessBiz.run()
