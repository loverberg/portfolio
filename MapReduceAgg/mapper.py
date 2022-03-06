#!/usr/bin/env python
"""mapper"""

import sys


def perform_map():
    pay_type_dict = {"1" : "Credit card", "2" : "Cash", "3" : "No charge", "4" : "Dispute",
                     "5" : "Unknown", "6" : "Voided trip"}

    for line in sys.stdin:
        line = line.strip()
        columns = line.split(',', 17)

        pickup = columns[1].strip()
        pickup = pickup[0:7]

        pay_type = columns[9].strip()
        for k in pay_type_dict.keys():
            pay_type = pay_type.replace(k, pay_type_dict[k])

        tips = columns[13].strip()
        if pay_type != "":
            print('%s;%s\t%s' % (pay_type, pickup, tips))
        else:
            print('%s;%s\t%s' % ("Unknown", pickup, tips))


if __name__ == '__main__':
    perform_map()
