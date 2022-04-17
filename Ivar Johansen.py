# Databricks notebook source
!pip install graphframes
from graphframes import *

# COMMAND ----------

stations = sc.textFile("FileStore/tables/week3/stations.csv")
stations = stations.map(lambda x: x.split(","))
stations = stations.map(lambda x: (int(x[0]), str(x[1])))
vertices = sqlContext.createDataFrame(stations,["id", "station_name"])
vertices.show(3)

# COMMAND ----------

trips = sc.textFile("FileStore/tables/week3/trips.csv")
trips = trips.map(lambda x: x.split(","))
trips = trips.map(lambda x: (int(x[4]), int(x[7])))
edges = sqlContext.createDataFrame(trips,["src", "dst"])
edges.show(3)

# COMMAND ----------

graph = GraphFrame(vertices, edges)

# COMMAND ----------

graph.inDegrees.sort("inDegree", ascending=False).show(3)

# COMMAND ----------

graph.outDegrees.sort("outDegree", ascending=False).show(3)

# COMMAND ----------

results = graph.pageRank(resetProbability=0.15, maxIter=10)
results.vertices.sort("pagerank", ascending=False).show(3)
