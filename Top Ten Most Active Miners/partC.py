"""Part C
//Fields contains line as follows. - Blocks
// 0        1     2        3         4     5          6          7          8
// number; hash; miner; difficulty; size; gas_limit; gas_used; timestamp; transaction_count

Evaluate the top 10 miners by the size of the 'blocks' mined.You will first have to aggregate blocks
to see how much each miner has been involved in. You will want to aggregate 'size' for addresses in
the 'miner' field. This will be similar to the wordcount that we saw in Lab 1 and Lab 2.

You can add each value from the reducer to a list and then sort the list to obtain the most active miners.
"""
from mrjob.job import MRJob, MRStep

class PartC(MRJob):

# job 1 aggregrate miners by size
    def mapper_agg(self, _, line):
        # miner and size aggregation
        try:
            fields = line.split(',')
            if len(fields) == 9 : # blocks table
                miner = fields[2]
                size = float(fields[4])
                yield (miner, size)
        except:
            pass
            #do nothing

    def combiner_agg(self, miner, size):
        total = 0.0 # since float values
        for v in size:
            total += v
        yield (miner, total)

    def reducer_agg(self, miner, size):
        total = 0.0
        for v in size:
            total += v
        yield (miner, total)





# job 2 get the top 10
    def mapper_top10(self, key, size):
        try:
            # the key and the values are passed from the previous jab!
            yield (None, (key, float(size)))
        except :
            pass



    def combiner_top10(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1]) # take miner as key
        i = 0
        for value in sorted_values:
            yield ("top", value)
            i += 1
            if i >= 10:
                break


    def reducer_top10(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield ("{} - {} ".format(value[0],value[1]),None)
            i += 1
            if i >= 10:
                break

    def steps(self):
          return [MRStep(mapper=self.mapper_agg,
                          combiner=self.combiner_agg,
                          reducer=self.reducer_agg),
                  MRStep(mapper=self.mapper_top10,
                          combiner=self.combiner_top10,
                          reducer=self.reducer_top10)]

if __name__ == '__main__':
    PartC.run()
