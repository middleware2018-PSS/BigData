# BigData

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/middleware2018-PSS/BigData/master?filepath=plots.ipynb)

### Spark
Docker container used to run Spark can be spawned by running:
```
docker run -it --name=spark-notebook -p 8888:8888 -v PATH/TO/REPO:/home/jovyan/work jupyter/all-spark-notebook
```
We implemented the queries using pyspark, the official Python API for Spark. We Used both spark DataFrame and SQL APIs to perform the queries, therefore we implemented all the queries with both the approaches and the code can be found in the relative python notebooks `./spark/spark/*.ipynb`.
Some considerations about the two APIs and how they compare, we haven't benchmarked the two implementations because it was out of the scope of the assignment, the machine we were using did not allow to run them rigorously and the litterature seams to have already explored the topic, but as far as we can tell the performances of the SQL version are on average slightly better than the one of the same queries run using the Dataframe APIs, this is mainly due to the optimizations performed from the spark query optimizer [Catalyst](https://databricks.com/glossary/catalyst-optimizer). The better performances and the increased readability due to the declarative syntax of the SQL language used, makes these APIs a really viable option for the kind of queries we had to implement. Has to be said that we have found the SQL APIs much harder to use, because of the many errors that could not be checked at "compile time", but were only checked during execution, wasting lots of time due to the errors being thrown after the query had already been executing for a while. 

### Plots
Some of the plots are interactive and they are not supported by GitHub, click on the Binder badge to view the Jupyter Notebook with all the plots. Some of them are not displayed automatically the first time, in order to load all plots, click on the first cell (the cell with all the imports), run the cell with `Ctrl+Enter` and
then run all the other cells clicking on `Cell -> Run All Below`.
