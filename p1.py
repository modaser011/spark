import pyspark
from pyspark import SparkContext, SparkConf
import time
conf = SparkConf().setMaster("local").setAppName("Wiki")
sc = SparkContext.getOrCreate(conf=conf)
lines_rdd = sc.textFile("pagecounts-20160101-000000_parsed.out")

start_time = time.time()

min_size = 1000000000
max_size = 0
sum = 0
count = 0

for x in lines_rdd.toLocalIterator():
  z=x.split(" ")
  size=int(z[3])
  max_size=max(max_size,size)
  min_size=min(min_size,size)
  sum+=size
  count+=1
mean = sum / count

print("Minimum Page Size:", min_size)
print("Maximum Page Size:", max_size)
print("Average Page Size:", mean)

end_time = time.time()
elapsed_time = end_time - start_time
print("Elapsed time:", elapsed_time, "seconds")

file1 = open("file.txt", "w")
file1.write("Min Page Size llop: " + str(min_size) + "\n")
file1.write("Max Page Size loop: " + str(max_size) + "\n")
file1.write("Avg Page Size loop: " + str(mean) + "\n")
file1.write("time in 1: " + str(elapsed_time) + "\n")
##map reduce

start_time2 = time.time()

fields_rdd = lines_rdd.map(lambda line: line.split(" "))
page_sizes_rdd = fields_rdd.map(lambda fields: int(fields[3]))

min_size = page_sizes_rdd.reduce(lambda x, y: min(x, y))
max_size = page_sizes_rdd.reduce(lambda x, y: max(x, y))
avg_size = page_sizes_rdd.mean()
print("map reduce#############################")
print("Minimum Page Size:", min_size)
print("Maximum Page Size:", max_size)
print("Average Page Size:", avg_size)

end_time2 = time.time()
elapsed_time2 = end_time2 - start_time2
print("Elapsed time2:", elapsed_time2, "seconds")
file1.write("Min Page Size map: " + str(min_size) + "\n")
file1.write("Max Page Size map: " + str(max_size) + "\n")
file1.write("Avg Page Size map: " + str(avg_size) + "\n")
file1.write("Elapsed time2: " + str(elapsed_time2) + "\n")

file1.close()
sc.stop()
