# BigData

**Click here to see the plots:** [![Plots](https://img.shields.io/badge/click-plots-green.svg)](https://middleware2018-pss.github.io/plots.html)

## Queries


1. The percentage of canceled flights per day, throughout the entire data set
2. Weekly percentages of delays that are due to weather, throughout the entire data set
3. The percentage of flights belonging to a given "distance group" that were able to halve their departure delays by the time they arrived at their destinations. Distance groups assort flights by their total distance in miles. Flights with distances that are less than 200 miles belong in group 1, flights with distances that are between 200 and 399 miles belong in group 2, flights with distances that are between 400 and 599 miles belong in group 3, and so on. The last group contains flights whose distances are between 2400 and 2599 miles.
4. A weekly "penalty" score for each airport that depends on both the its incoming and outgoing flights. The score adds 0.5 for each incoming flight that is more than 15 minutes late, and 1 for each outgoing flight that is more than 15 minutes late.

## Extra Queries
5. Busiest Airports and Flight Traffic
6. Taxi Time

## Spark
Docker container used to run Spark can be spawned by running:
```
docker run -it --name=spark-notebook -p 8888:8888 -v PATH/TO/REPO:/home/jovyan/work jupyter/all-spark-notebook
```
We implemented the queries using pyspark, the official Python API for Spark. We used both spark [RDD](https://spark.apache.org/docs/latest/rdd-programming-guide.html) and [SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) APIs to perform the queries, therefore we implemented all the queries with both the approaches and the code can be found in the relative python notebooks.
Some considerations about the two APIs and how they compare, we haven't benchmarked the two implementations because it was out of the scope of the assignment, the machine we were using did not allow to run them rigorously and the litterature seams to have already explored the topic, but as far as we can tell the performances of the SQL version are on average slightly better than the one of the same queries run using the RDD APIs, this is mainly due to the optimizations performed from the spark query optimizer [Catalyst](https://databricks.com/glossary/catalyst-optimizer). The better performances and the increased readability due to the declarative syntax of the SQL language used, makes these APIs a really viable option for the kind of queries we had to implement. Has to be said that we have found the SQL APIs much harder to use, because of the many errors that could not be checked at "compile time", but were only checked during execution, wasting lots of time due to the errors being thrown after the query had already been executing for a while. 

All the spark code can be found in [here](spark):
  - [Spark-queries](spark/Spark-queries.ipynb): notebook with queries implemented using RDD API
  - [Spark-SQL-queries](spark/Spark-SQL-queries.ipynb): notebook with queries implemented using spark SQL
  - [ExtraQuery](spark/ExtraQuery.ipynb): the extra query implemented using both RDD and SQL APIs
  - [results](spark/results): the computed results as csv files
    - `query[0-4].csv/`: contain the correspondent query results for the assigned queries
    - `query-extra-N.csv/`: contain the correspondent query results for some extra query we have performed
    - `extraQuery/` contains the result for the extra query
    
## Hadoop
We implemented the queries using the Apache Hadoop framework, configured on pseudo-distributed mode, on Java. 
In every query both keys and values classes have been chosen to reduce the total execution time in the given enviroment. 

All the Hadoop code can be found in [here](BDHadoop).

The single queries code:

  - [Query 1](BDHadoop/src/Query1)
  - [Query 2](BDHadoop/src/Query2)
  - [Query 3](BDHadoop/src/Query3)
  - [Query 4](BDHadoop/src/Query4)
  - [Query 5](BDHadoop/src/Query5)
  - [Query 6](BDHadoop/src/Query6)
  - [Query 6b](BDHadoop/src/Query6b)

## Plots
**Click here to see the plots:** [![Plots](https://img.shields.io/badge/click-plots-green.svg)](https://middleware2018-pss.github.io/plots.html)

The query visualizations were created in a **Jupyter Notebook** using different libraries managed by **Conda**: 
- [Pandas](https://pandas.pydata.org/)
- [Bokeh](https://bokeh.pydata.org/)
- [Seaborn](https://seaborn.pydata.org/)
- [Holoviews](http://holoviews.org/)
- [GeoViews](http://geo.holoviews.org/)
- [Cartopy](https://scitools.org.uk/cartopy)

Due to the fact that some of the plots are interactive, they cannot be
visualized directly on GitHub or using Nbviewer.
For this reason we provide a self-contained HTML with all the plots, their description
and a button to show/hide code snippets.
When writing plots descriptions we tried not only to explain the query, but also
to make clear why we choose a particular visualization and how it can be related
to other plots.

## N.B.
Due to the different implementation of calendar libraries in Java and Python (respectively: https://docs.python.org/2/library/datetime.html and https://docs.oracle.com/javase/7/docs/api/java/util/Calendar.html) the results of the queries 2,4,5,6 are slightly different.
