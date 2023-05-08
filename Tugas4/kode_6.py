from pyspark import *
# sc = SparkContext()
sc = SparkContext.getOrCreate();

from operator import add
lines = sc.textFile("/workspaces/Big Data/Tugas4/data/README.md")
counts = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))