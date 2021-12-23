"""Part a 1 . Input and output
Number of transactions occurring every month between the start and end of the dataset
format
//Fields contains line as follows.
//   0            1             2           3    4     5            6
//block_number; from address; to_address; value; gas; gas_price; block_timestamp
"""
from mrjob.job import MRJob
from datetime import datetime
import time

class PartA1(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if (len(fields)) == 7 :
                time_stamp = int(fields[6])
                time_epoch = int(fields[6])
                year_month = time.strftime("%m/%Y", time.gmtime(time_epoch))
                # but for now we get the month and year combo data
                # sort the data and clean output in python and show graph
                yield (year_month, 1)
        except:
            pass
            #do nothing

    def combiner(self, year_month, counts):
        yield (year_month, sum(counts))

    def reducer(self, year_month, counts):
        yield (year_month, sum(counts))


if __name__ == '__main__':
    PartA1.run()
