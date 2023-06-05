import pyspark
from pyspark import SparkContext, SparkConf
from pyspark import SparkContext
import time
start_time = time.time()
conf = SparkConf().setMaster("local").setAppName("Wiki")
sc = SparkContext.getOrCreate(conf=conf)
lines_rdd = sc.textFile("pagecounts-20160101-000000_parsed.out")
start_time = time.time()
dataframe = {}

def parse_line(line):
    lines = line.split(" ")
    return lines[1], lines[2:]

for line in lines_rdd.toLocalIterator():
    titl, dataPage = parse_line(line)
    if titl in dataframe:
        dataframe[titl].append(dataPage)
    else:
        dataframe[titl] = [dataPage]

print(len(dataframe))
end_time = time.time()
elapsed_time = end_time - start_time
print("Elapsed time:", elapsed_time, "seconds")
file1 = open("file4.txt", "w")

file1.write("lenth of list: " + str(len(dataframe)) + "\n")
file1.write("time of p5_loop: " + str(elapsed_time) + "\n")


print("############################################")
start_time2 = time.time()

dataframe = lines_rdd.map(parse_line)
finaldata = dataframe.groupByKey().flatMap(lambda tx: [(tx[0], list(tx[1]))])

print(finaldata.count())
end_time2 = time.time()
elapsed_time2 = end_time2 - start_time2
print("Elapsed time2:", elapsed_time2, "seconds")

file1.write("lenth of list: " + str(finaldata.count()) + "\n")
file1.write("time of p5_map: " + str(elapsed_time2) + "\n")
file1.close()
sc.stop()
