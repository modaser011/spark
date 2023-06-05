import pyspark
from pyspark import SparkContext, SparkConf
from pyspark import SparkContext
import time
conf = SparkConf().setMaster("local").setAppName("Wiki")
sc = SparkContext.getOrCreate(conf=conf)
lines_rdd = sc.textFile("pagecounts-20160101-000000_parsed.out")

start_time = time.time()

the_titles_count = 0
non_english_the_titles_count = 0

for line in lines_rdd.toLocalIterator():
    fields = line.split(" ")
    str1=str(fields[1])
    str2=str(fields[0])
    if str1[0:4].lower()=="the_":
        the_titles_count += 1
        if str2!="en":
            non_english_the_titles_count += 1

print("Number of page titles starting with 'The':", the_titles_count)
print("Number of page titles starting with 'The' and not part of the English project:", non_english_the_titles_count)

end_time = time.time()
elapsed_time = end_time - start_time
print("Elapsed time:", elapsed_time, "seconds")
file1 = open("file1.txt", "w")
file1.write("Number of page titles starting with 'The'_loop:"+ str(the_titles_count) + "\n")
file1.write("Number of page titles starting with 'The' and not part of the English project_loop:"+ str(non_english_the_titles_count) + "\n")
file1.write("time in 1: " + str(elapsed_time) + "\n")

print("####################################################")
start_time2 = time.time()

the_titles_rdd = lines_rdd.filter(lambda line: str(line.split(" ")[1])[0:4].lower()==("the_"))

the_titles_count = the_titles_rdd.count()

non_english_the_titles_rdd = the_titles_rdd.filter(lambda line: str(line.split(" ")[0]).lower()!=("en"))

non_english_the_titles_count = non_english_the_titles_rdd.count()

print("Number of page titles starting with 'The':", the_titles_count)
print("Number of page titles starting with 'The' and not part of the English project:", non_english_the_titles_count)
end_time2 = time.time()
elapsed_time2 = end_time2 - start_time2
print("Elapsed time:", elapsed_time2, "seconds")

file1.write("Number of page titles starting with 'The'_map:"+ str(the_titles_count) + "\n")
file1.write("Number of page titles starting with 'The' and not part of the English project_map:"+ str(non_english_the_titles_count) + "\n")
file1.write("time in 2: " + str(elapsed_time2) + "\n")
file1.close()
sc.stop()


