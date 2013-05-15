from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

import itertools
import re
import json
import sys

degree = 5

WORD_RE = re.compile(r"[\w']+")

total = set()

class HelperUser(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def map_comparison(self, _, record):
        print record
        if len(record['ratings']) > degree:
            for r in record['ratings']:
                yield r[0], (r[1] - record['average_stars'], record['user_id'], record['std'])        

    def distribute_similarity(self, business_id, middleware):
        """ middleware consists of rating on this business minus average rating,
            user_id, standard deviation
        """
        print business_id
        # combine them into 2-element tuples
        for double in itertools.combinations(middleware, 2):
            user1 = double[0]
            user2 = double[1]
            multiple = double[0][0] * double[1][0]
            key = tuple(sorted([user1, user2]))
            yield key, (multiple, user1[2], user2[2])

    def calculte_similarity(key, middleware):
        """
            Aggregate similarity for tuple(user_id, user_id)
            middleware consists of result, std1, std2
        """
        result = 0.0
        print key
        for m in middleware:
            std1 = m[1]
            std2 = m[2]
            result += m[0]
        print key, result/std1/std2

    def steps(self):
        print "specifying steps"
        return [self.mr(self.map_comparison, self.distribute_similarity),]

if __name__ == '__main__':
    HelperUser.run()
