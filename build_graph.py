'''
 Spark Job Model Production
 
 $ spark-submit --py-files graphframes.zip --jars jars/scala-logging-api_2.11-2.1.2.jar,jars/scala-logging-slf4j_2.11-2.1.2.jar  build_graph.py 


 Marlesson
'''
import os
import sys
import argparse
import time
import importlib
from pyspark.sql import SparkSession

from graphframes import *

if __name__ == '__main__':
  # Session Spark
  spark = SparkSession\
      .builder\
      .appName("Graph")\
      .getOrCreate()  

  # Create a Vertex DataFrame with unique ID column "id"
  v = spark.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
  ], ["id", "name", "age"])
  # Create an Edge DataFrame with "src" and "dst" columns
  e = spark.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
  ], ["src", "dst", "relationship"])
  # Create a GraphFrame
  from graphframes import *
  g = GraphFrame(v, e)

  # Query: Get in-degree of each vertex.
  g.inDegrees.show()

  # Query: Count the number of "follow" connections in the graph.
  g.edges.filter("relationship = 'follow'").count()

  # # Run PageRank algorithm, and show results.
  # results = g.pageRank(resetProbability=0.01, maxIter=20)
  # results.vertices.select("id", "pagerank").show()        