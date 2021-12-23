"""
Popular Scams: Utilising the provided scam dataset, what is the most lucrative form of scam?
How does this change throughout time, and does this correlate with certain known scams going
offline/inactive?

//Fields contains line as follows.
// transactions
//   0            1             2           3    4     5            6
//block_number; from address; to_address; value; gas; gas_price; block_timestamp

// Scams CSV
//   0     1    2    3       4          5            6       7
//ScamID, Name, URL, Coin, Category, Sub-Category, Address, Status

# scam id, date, agg value, plot through time..........
# upload scams.csv to adreomeda hadoop user ss389 space
"""

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) == 7: # transactions table
            str(fields[2]) # to_addr
            if int(fields[3]) == 0:
                return False # not a good line; we don't want it
        elif len(fields) == 8: # scamscsv
            str(fields[6]) # Scam addr
            str(fields[4]) # Scam category
        else:
            return False
        return True
    except:
        return False

def mapper_trans(line):
    try:
        fields = line.split(',')
        to_addr = fields[2]
        value = int(fields[3])
        timestamp = int(fields[6])
        year_month = time.strftime('%Y-%m', time.gmtime(timestamp))

        key = to_addr
        value = (year_month, value, 1)
        return (key, value)
    except:
        pass

sc = pyspark.SparkContext()
transactions = sc.textFile('/data/ethereum/transactions').filter(is_good_line)
scams = sc.textFile('scams.csv').filter(is_good_line)
scams_addr = scams.map(lambda l: (l.split(',')[6], l.split(',')[4])) #get address and category from scams
step1 = transactions.map(mapper_trans) # get address, year_month, value, count(1)
step2 = step1.join(scams_addr) # join based on common value
step3 = step2.map(lambda x: ((x[1][1], x[1][0][0]), (x[1][0][1], x[1][0][2]))) # get values in form we want via mapper
step4 = step3.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]) ) # reduce based on numeric values in values
step5 = step4.map(lambda x: '{},{},{},{},{}'.format(x[0][0], x[0][1], float(x[1][0]/1000000000000000000), x[1][1], float(x[1][0]/1000000000000000000)/x[1][1])) # format values and show what we want to display
step5.saveAsTextFile('scam_transactions_analysis') #save it
