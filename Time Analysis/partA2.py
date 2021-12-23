"""Part a 2
The average value of transaction in each month between the start and end of the dataset
//Fields contains line as follows.
//   0            1             2           3    4     5            6
//block_number; from address; to_address; value; gas; gas_price; block_timestamp
"""
from mrjob.job import MRJob
from datetime import datetime
import time

class PartA2(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7 :
                time_epoch = int(fields[6])
                year_month = time.strftime("%m/%Y", time.gmtime(time_epoch))
                # to get the average value of transaction every month of the year
                value = float(fields[3])/1000000000 # divide by a lot of 1 0s => to round the value up
                yield (year_month, (value, 1)) # count = 1 as to calculate the avergae based on toatl number of transactiosn
        except:
            pass
            #do nothing

    def combiner(self, year_month, values):
        total = 0.0 # since float
        count = 0
        for v in values:
            count += v[1]
            total += v[0] # get total value of transcs for that month of the year
        yield (year_month, (total, count))

    def reducer(self, year_month, values):
        total = 0.0
        count = 0
        for v in values:
            count += v[1]
            total += v[0] # get total value of transcs for that month of the year
        yield (year_month, total/count) # calculate the average value of transactions for each month of the year

if __name__ == '__main__':
    PartA2.run()
