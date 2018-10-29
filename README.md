# BigData

click here to see the plots -> [![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/middleware2018-PSS/BigData/master?filepath=plots.ipynb)

### Queries


1. The percentage of canceled flights per day, throughout the entire data set
2. Weekly percentages of delays that are due to weather, throughout the entire data set
3. The percentage of flights belonging to a given "distance group" that were able to halve their departure delays by the time they arrived at their destinations. Distance groups assort flights by their total distance in miles. Flights with distances that are less than 200 miles belong in group 1, flights with distances that are between 200 and 399 miles belong in group 2, flights with distances that are between 400 and 599 miles belong in group 3, and so on. The last group contains flights whose distances are between 2400 and 2599 miles.
4. A weekly "penalty" score for each airport that depends on both the its incoming and outgoing flights. The score adds 0.5 for each incoming flight that is more than 15 minutes late, and 1 for each outgoing flight that is more than 15 minutes late.


### Spark
Docker container used to run Spark can be spawned by running:
```
docker run -it --name=spark-notebook -p 8888:8888 -v PATH/TO/REPO:/home/jovyan/work jupyter/all-spark-notebook
```
We implemented the queries using pyspark, the official Python API for Spark. We Used both spark DataFrame and SQL APIs to perform the queries, therefore we implemented all the queries with both the approaches and the code can be found in the relative python notebooks `./spark/spark/*.ipynb`.
Some considerations about the two APIs and how they compare, we haven't benchmarked the two implementations because it was out of the scope of the assignment, the machine we were using did not allow to run them rigorously and the litterature seams to have already explored the topic, but as far as we can tell the performances of the SQL version are on average slightly better than the one of the same queries run using the Dataframe APIs, this is mainly due to the optimizations performed from the spark query optimizer [Catalyst](https://databricks.com/glossary/catalyst-optimizer). The better performances and the increased readability due to the declarative syntax of the SQL language used, makes these APIs a really viable option for the kind of queries we had to implement. Has to be said that we have found the SQL APIs much harder to use, because of the many errors that could not be checked at "compile time", but were only checked during execution, wasting lots of time due to the errors being thrown after the query had already been executing for a while. 

All the spark code can be found in [here](spark):
  - [Spark-queries](spark/Spark-queries.ipynb): notebook with queries implemented using DataFrame API
  - [Spark-SQL-queries](spark/Spark-SQL-queries.ipynb): notebook with queries implemented using spark SQL
  - [ExtraQuery](spark/ExtraQuery.ipynb): the extra query implemented using both DataFrame and SQL APIs
  - [results](spark/results): the computed results as csv files
    - `queryN.csv/`: contain the correspondent query results for the assigned queries
    - `query-extra-N.csv/`: contain the correspondent query results for some extra query we have performed
    - `extraQuery/` contains the result for the extra query we have decided to keep as extra query

### Plots
Some of the plots are interactive and they are not supported by GitHub, click on the Binder badge to view the Jupyter Notebook with all the plots. Some of them are not displayed automatically the first time, in order to load all plots, click on the first cell (the cell with all the imports), run the cell with `Ctrl+Enter` and
then run all the other cells clicking on `Cell -> Run All Below`.
