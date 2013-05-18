from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

import itertools
import re
import json
import sys

CUTOFF = 4

degree = 5

WORD_RE = re.compile(r"[\w']+")

total = set()

class UserProcessor(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def map_user(self, _, record):
        if record['type'] == 'review':
            yield 'all', (record['user_id'], record['business_id'], record['stars'], record['votes'])

    def reduce_user(self, unused, middleware):
        for iterand in itertools.combinations(middleware, 2):
            r1 = iterand[0]
            r2 = iterand[1]
            if r1[1] != r2[1]:
                continue
            similar = False
            if r1[2] >= CUTOFF and r2[2] >= CUTOFF:
                similar = True
            if r1[0] > r2[0]:
                yield (r1[0], r2[0]), {'business_id': r1[1], 'is_similar': similar}
            else:
                yield (r2[0], r1[0]), {'business_id': r1[1], 'is_similar': similar}

    def processor(self, twin, data):
        rc = {'users': twin}
        count = 0.0
        sim_count = 0.0
        for temp in data:
            count += 1
            if temp['is_similar']:
                sim_count += 1
        rc['similarity'] = sim_count/count
        print json.dumps(rc)

    def steps(self):
        return [self.mr(self.map_user, self.reduce_user),
                self.mr(reducer=self.processor)]

if __name__ == '__main__':
    UserProcessor.run()