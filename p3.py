import pyspark
from pyspark import SparkContext, SparkConf
from pyspark import SparkContext
import time
conf = SparkConf().setMaster("local").setAppName("Wiki")
sc = SparkContext.getOrCreate(conf=conf)
lines_rdd = sc.textFile("pagecounts-20160101-000000_parsed.out")

start_time = time.time()

unique_terms_set = set()

for line in lines_rdd.toLocalIterator():
    fields = line.split(" ")
    e = str(fields[1]).lower().split("_")
    for txt in e:
      txt = ''.join(ss for ss in txt if ss.isalnum()) 
      unique_terms_set.add(str(txt)) 

unique_terms_count = len(unique_terms_set)

print("Number of unique terms in page titles:", unique_terms_count)

end_time = time.time()
elapsed_time = end_time - start_time
print("Elapsed time:", elapsed_time, "seconds")

file1 = open("file2.txt", "w")
file1.write("Number of unique terms in page titles_loop:"+ str(unique_terms_count)+ "\n")
file1.write("Elapsed time1:"+ str(elapsed_time) + "\n")


print("########################################")
start_time2 = time.time()

dfsplit=lines_rdd.map(lambda line: line.split(" "))

terms_rdd = dfsplit.flatMap(lambda row:row[1].lower().split("_"))
normalized_terms_rdd = terms_rdd.map(lambda term: ''.join(txt for txt in term if txt.isalnum()))

cnt=normalized_terms_rdd.map(lambda trm:(trm,1)).reduceByKey(lambda txt1,tx2:txt1+tx2)
count=cnt.count()
print("Number of unique terms in page titles:", count)

end_time2 = time.time()
elapsed_time2 = end_time2 - start_time2
print("Elapsed time2:", elapsed_time2, "seconds")

file1.write("Number of unique terms in page titles_map:"+ str(unique_terms_count)+ "\n")
file1.write("Elapsed time2:"+ str(elapsed_time2) + "\n")
file1.close()
sc.stop()
