#!/usr/bin/env python

import sys
from nltk.corpus import wordnet

output = open(sys.argv[1], 'w+')

for s in wordnet.all_synsets():
    name = s.name().split('.')
    output.write('{}\t{}\t{}\n'.format(s.offset(), name[0], name[1]))

output.close()
print 'done'
