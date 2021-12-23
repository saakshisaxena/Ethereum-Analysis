"""
// who profitted most from it...address and value of gas provided for that time period
// transactions
//   0            1             2           3    4     5            6
//block_number; from address; to_address; value; gas; gas_price; block_timestamp
"""

from mrjob.job import MRJob, MRStep
import time

class Fork2(MRJob):

    def mapper1(self,_,line):
        try:
            fields = line.split(',')
            val = float(fields[5])
            date = time.gmtime(float(fields[6]))
            address = fields[2]
            if len(fields) == 7:
                if (date.tm_year== 2017 and date.tm_mon== 10): # October 2017
                    yield (address, val)


        except:
            pass

    def combiner1(self,key,val):
        yield (key, sum(val))

    def reducer1(self,key,val):
        yield (key, sum(val))

# job 3 get the top addresses who benefitted
    def mapper_top(self, key, val):
        try:
            yield (None, (key, float(val)))
        except :
            pass



    def combiner_top(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1]) # take address as key
        i = 0
        for value in sorted_values:
            yield ("top", value)
            i += 1
            if i >= 10:
                break


    def reducer_top(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield (value[0],value[1])
            i += 1
            if i >= 3:
                break

    def steps(self):
          return [MRStep(mapper=self.mapper1,
                          combiner=self.combiner1,
                          reducer=self.reducer1),
                  MRStep(mapper=self.mapper_top,
                          combiner=self.combiner_top,
                          reducer=self.reducer_top)]

if __name__=='__main__':
    Fork2.run()
