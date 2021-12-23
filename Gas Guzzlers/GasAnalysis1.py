"""
Gas Guzzlers: For any transaction on Ethereum a user must supply gas. How has gas price changed over time?
Have contracts become more complicated, requiring more gas, or less so? How does this correlate with
your results seen within Part B.
// transactions
//   0            1             2           3    4     5            6
//block_number; from address; to_address; value; gas; gas_price; block_timestamp

// contracts
// 0           1        2         3            4
// address; is_erc20; is_erc721; block_number; block_timestamp

// blocks
// 0        1     2        3         4     5          6          7          8
// number; hash; miner; difficulty; size; gas_limit; gas_used; timestamp; transaction_count

 """
# Part 1
# time analysis with gas price
# from transactions get clean lines, get time stamp and price and for each (yyy-mm) collect (price,1) like count of gas transacrions and total price...
# get average gas price based on time
# save results as a text file to show statistics

from mrjob.job import MRJob
from datetime import datetime
import time

class GasAnalysis1(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7 :
                time_epoch = int(fields[6])
                year_month = time.strftime("%m/%Y", time.gmtime(time_epoch))
                price = float(fields[5])
                yield (year_month, (price, 1)) # count = 1 # timestamp and gas price
        except:
            pass
            #do nothing

    def combiner(self, year_month, values):
        total = 0.0 # since float
        count = 0
        for v in values:
            count += v[1]
            total += v[0] # get total gas price for that month of the year
        yield (year_month, (total, count))

    def reducer(self, year_month, values):
        total = 0.0
        count = 0
        for v in values:
            count += v[1]
            total += v[0]
        yield (year_month, total/count) # do the average gas price for the time stamp

if __name__ == '__main__':
    GasAnalysis1.run()
