# BigData

**Click here to see the plots:** [![Plots](https://img.shields.io/badge/click-plots-green.svg)](https://middleware2018-pss.github.io/plots.html)

## Queries


1. The percentage of canceled flights per day, throughout the entire data set
2. Weekly percentages of delays that are due to weather, throughout the entire data set
3. The percentage of flights belonging to a given "distance group" that were able to halve their departure delays by the time they arrived at their destinations. Distance groups assort flights by their total distance in miles. Flights with distances that are less than 200 miles belong in group 1, flights with distances that are between 200 and 399 miles belong in group 2, flights with distances that are between 400 and 599 miles belong in group 3, and so on. The last group contains flights whose distances are between 2400 and 2599 miles.
4. A weekly "penalty" score for each airport that depends on both the its incoming and outgoing flights. The score adds 0.5 for each incoming flight that is more than 15 minutes late, and 1 for each outgoing flight that is more than 15 minutes late.


## Spark
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
