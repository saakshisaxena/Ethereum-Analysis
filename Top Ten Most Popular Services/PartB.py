"""Part B
Evaluate the top 10 smart contracts by total Ether received.

//Fields contains line as follows.

// transactions
//   0            1             2           3    4     5            6
//block_number; from address; to_address; value; gas; gas_price; block_timestamp

// contracts
 // 0           1        2         3            4
 // address; is_erc20; is_erc721; block_number; block_timestamp
"""
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol

class PartB(MRJob):
    def mapper_join(self, _, line):
        fields = line.split(',')
        try:
            if len(fields)==5: #contracts
                address1=fields[0]
                yield(address1, ('C',1)) # sendign 1 as we need it for it to be an array of values
            elif len(fields)==7: #transactions
                address = fields[2]
                value = int(fields[3])
                yield(address, ('T',value))
        except:
            pass

    def reducer_join(self, key, values):
        contract_exists = False
        vals=[]
        for i in values:
            if i[0]=='T':
                vals.append(i[1])
            elif i[0]=='C':
                contract_exists = True

        if contract_exists:
            yield(key, sum(vals))

    def mapper_top10(self, key, value):
        yield(None, (key, value)) # carry forward the key and value pairs received from the previous job

    def reducer_top10(self, _, keys):
        sorted_vals = sorted(keys, reverse = True, key = lambda tup:tup[1])
        for v in sorted_vals[:10]:
            yield v[0], v[1] # trying to print without nulls

    def steps(self):
        return [MRStep(mapper = self.mapper_join,
                        reducer=self.reducer_join),
                MRStep(mapper = self.mapper_top10,
                        reducer = self.reducer_top10)]


if __name__ == '__main__':
    PartB.run()
