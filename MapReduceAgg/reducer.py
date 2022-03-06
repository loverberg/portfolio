#!/usr/bin/env python
"""reducer.py"""

from itertools import groupby
from operator import itemgetter
import sys


def mapper_result(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)


def perform_reducer(separator='\t'):
    data = mapper_result(sys.stdin, separator=separator)

    for current_word, group in groupby(sorted(data, key=itemgetter(0)), key=itemgetter(0)):
        try:
            total_count = [float(count) for current_word, count in group]
            total_count2 = round((sum(total_count)/len(total_count)), 2)
            if current_word.find("2020") > 0:
                print("%s%s%f" % (current_word, ';', total_count2))
        except ValueError:
            pass


if __name__ == '__main__':
    perform_reducer()
