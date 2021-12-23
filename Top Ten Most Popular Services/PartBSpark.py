"""Part B in Spark
Evaluate the top 10 smart contracts by total Ether received.

//Fields contains line as follows.

// transactions
//   0            1             2           3    4     5            6
//block_number; from address; to_address; value; gas; gas_price; block_timestamp

// contracts
 // 0           1        2         3            4
 // address; is_erc20; is_erc721; block_number; block_timestamp
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time
import datetime
import pyspark

def timer(func): # to get time with each executation and print average time at the end
    sc = pyspark.SparkContext()
    for i in range(5):
        start = datetime.datetime.now().replace(microsecond=0)
        func(sc)
        end = datetime.datetime.now().replace(microsecond=0)
        lengthOfJob = end - start # hrs mins secs
        print("\nattempt {attemptno} \nspark time: {timevalue}\n".format(attemptno = i, timevalue = lengthOfJob))


def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) == 7: # transactions
            str(fields[2]) # to_addr
            if int(fields[3]) == 0:
                return False # value exists or not
        elif len(fields) == 5: # contracts
            str(fields[0]) # contract addr
        else:
            return False
        return True
    except:
        return False


def spark_PartB(sc):
    transactions = sc.textFile('/data/ethereum/transactions').filter(is_good_line)
    contracts = sc.textFile('/data/ethereum/contracts').filter(is_good_line)
    addr_value= transactions.map(lambda l: (l.split(',')[2], int(l.split(',')[3])))
    aggregrate_values = addr_value.reduceByKey(lambda a,b: a+b)
    step3 = aggregrate_values.join(contracts.map(lambda x: (x.split(',')[0], 'contract'))) # join
    top10 = step3.takeOrdered(10, key = lambda x: -x[1][0]) # top 10

    for record in top10:
        print("{},{}".format(record[0], int(record[1][0]/1000000000000000000))) # round the value up otherwise it'll be too big of a value


timer(spark_PartB)
