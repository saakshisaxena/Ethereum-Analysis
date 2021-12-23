"""
Fork the Chain: There have been several forks of Ethereum in the past.
Identify one or more of these and see what effect it had on price and general usage.
// transactions
//   0            1             2           3    4     5            6
//block_number; from address; to_address; value; gas; gas_price; block_timestamp

Consider fork occured in 2017-October(10) found via https://ethereum.org/en/history/#2017
 """
from mrjob.job import MRJob
import time

class PartA(MRJob):

    def mapper(self,_,line):
        try:
            fields = line.split(',')
            val = float(fields[5]) #gas price
            date = time.gmtime(float(fields[6])) #timestamp
            if len(fields) == 7:
                if (date.tm_year== 2017 and date.tm_mon== 10):
                    yield ((date.tm_mday), (1, val)) #get based on day of the month


        except:
            pass

    def combiner(self,key,val):
        count = 0
        total = 0
        for v in val:
            count+= v[0]
            total= v[1]

        yield (key, (count,total))

    def reducer(self,key,val):
        count = 0
        total = 0
        for v in val:
            count += v[0]
            total = v[1]

        yield (key, (count,total))

if __name__=='__main__':
    PartA.run()
