from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

import itertools
import re
import json
import sys

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

from pymongo import MongoClient

import re
import json
import sys

degree = 5

WORD_RE = re.compile(r"[\w']+")

total = set()

class UserAnalyzer(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper1(self, _, record):
        # sys.stderr.write('extracting ' + record['user_id'] + '\n')
        if len(record['ratings']) > degree:
            for r in record['ratings']:
                print r[0]
                yield r[0], (r[1] - record['average_stars'], record['user_id'], record['std'])        

    def combiner1(self, business_id, middleware):
        """ middleware consists of rating on this business minus average rating,
            user_id, standard deviation
        """
        # print business_id
        # sys.stderr.write('combininig \n')
        # combine them into 2-element tuples
        for double in itertools.combinations(middleware, 2):
            user1 = double[0]
            user2 = double[1]
            multiple = double[0][0] * double[1][0]
            key = tuple(sorted([user1[1], user2[1]]))
            yield key, (multiple, user1[2], user2[2])

    def steps(self):
        # print "=====critical steps"
        return [self.mr(self.mapper1, self.combiner1)]
                
if __name__ == '__main__':
    UserAnalyzer.run()
