"""
Gas Guzzlers: For any transaction on Ethereum a user must supply gas. How has gas price changed over time?
 Have contracts become more complicated, requiring more gas, or less so? How does this correlate with
 your results seen within Part B.
 // contracts
 // 0           1        2         3            4
 // address; is_erc20; is_erc721; block_number; block_timestamp
// blocks
 // 0        1     2        3         4     5          6          7          8
 // number; hash; miner; difficulty; size; gas_limit; gas_used; timestamp; transaction_count

 """
# part 2
# contracts based on time ..... year-month - key; difficulty, gas_used,
# for join use block number
# check the output and plot graph

from mrjob.job import MRJob, MRStep
from datetime import datetime
import time

class GasAnalysis2(MRJob):

    def mapper1(self, _, line):
        try:
            if(len(line.split(","))==5):
                fields = line.split(",")
                block_id = fields[3] #block id
                # print("C: block_id: ", block_id)
                yield(block_id, ('C', 1))
            if(len(line.split(","))==9):
                # yield(len(line.split(",")), None)
                fields = line.split(",")
                block_id = fields[0]
                difficulty = int(fields[3])/10000000
                gas_used = int(fields[6])/10000
                time_stamp = int(fields[7])
                year_month = time.strftime('%Y-%m', time.gmtime(time_stamp))
                # print("B: block_id, diff, gasused, y-m: ", block_id, difficulty, gas_used, year_month)
                yield(block_id, ('B', (difficulty, gas_used), year_month))
        except:
            pass

    def reducer1(self, block_id, values): # to check if its a smart contract
            contr_add_exists = False
            gas_used = []
            difficulty = []
            year_month = []
            for v in values:
                if v[0]=='B':
                    year_month.append(v[2])# get the time stamp to do it based on time
                    difficulty.append(int(v[1][0]))
                    gas_used.append(int(v[1][1]))
                elif v[0]=='C':
                    contr_add_exists = True

            if contr_add_exists:
                yield(year_month, (sum(difficulty), sum(gas_used))) # get smart contracts and its details based on block id

    def mapper2(self, key, values):
        try:#based on year month
            yield(key, values)
        except:
            pass

    def reducer2(self, key, values): # reduce it using timestamp as key and get total value for that time stamp
        diff = 0
        gas_used = 0

        for v in values:
            diff+=v[0]
            gas_used+=v[1]
        yield(key, (diff, gas_used))

    def steps(self):
          return [MRStep(mapper=self.mapper1,
                          reducer=self.reducer1),
                  MRStep(mapper=self.mapper2,
                          reducer=self.reducer2)]

if __name__ == '__main__':
    GasAnalysis2.run()
