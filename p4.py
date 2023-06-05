import pyspark
from pyspark import SparkContext, SparkConf
from pyspark import SparkContext
import time
start_time = time.time()
conf = SparkConf().setMaster("local").setAppName("Wiki")
sc = SparkContext.getOrCreate(conf=conf)
lines_rdd = sc.textFile("pagecounts-20160101-000000_parsed.out")
start_time = time.time()
frequent_items = {}

for line in lines_rdd.toLocalIterator():
    d=line.split(" ")
    if len(d) >=2:
      page_title = line.split(" ")[1]
    else:
      continue
    if page_title not in frequent_items:
        frequent_items[page_title] = 1
    else:
        frequent_items[page_title] = frequent_items[page_title]+1

for i in frequent_items.items():
  most_freq_title = list(i)
  #i stopped the run because therre are 2968696 item
  #print(most_freq_title)
print(len(frequent_items))
end_time = time.time()
elapsed_time = end_time - start_time
print("Elapsed time:", elapsed_time, "seconds")
file1 = open("file3.txt", "w")
file1.write("lenth of list: " + str(len(frequent_items)) + "\n")
file1.write("time of p4_loop: " + str(elapsed_time) + "\n")


print("############################################")
start_time2 = time.time()

def split_pages(line):
    project_code, page_title, page_hits, page_size = line.split(' ')
    return project_code, page_title, int(page_hits), int(page_size)

pgs = lines_rdd.map(split_pages)
tlt_and_cnt = pgs.map(lambda p: (p[1], 1))
tlt_and_cnt = tlt_and_cnt.reduceByKey(lambda tx1, tx2: tx2 + tx1)

#for title, count in tlt_and_cnt.toLocalIterator():
     #print(f"Title: {title} , Count: {count}")
      #file1.write(f"Title: {title}, Count: {count}\n")

print(tlt_and_cnt.count())
end_time2 = time.time()
elapsed_time2 = end_time2 - start_time2
print("Elapsed time2:", elapsed_time2, "seconds")

file1.write("lenth of list: " + str(tlt_and_cnt.count()) + "\n")
file1.write("time of p4_map: " + str(elapsed_time2) + "\n")
file1.close()
sc.stop()
