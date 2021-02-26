# Motor Vehicle Collisions Project  
#### Ken Yokokawa, Matthew Zhang, Zijia Hu


## Data Sources

Permit Data (Zijia):
[*Street Construction Permits*](https://data.cityofnewyork.us/Transportation/Street-Construction-Permits/tqtj-sjs8)

Volume Data (Matthew):
[*Traffic Volume Counts (2014-2019)*](https://data.cityofnewyork.us/Transportation/Traffic-Volume-Counts-2014-2019-/ertz-hr4r)

Collision Data (Ken):
[*Motor Vehicle Collisions - Crashes*](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95)


## Data Ingestion

Data was directly downloaded from City of NY website to a local machine before being uploaded to Dumbo. 
<!-- Further details are in `/data_ingest/ingest.txt`.  -->
To upload datasets HDFS, here are some example commands:

1. Download dataset from online as a csv file
https://data.cityofnewyork.us/Transportation/Street-Construction-Permits/tqtj-sjs8

2. Log in to dumbo
`ssh -Y zh1130@dumbo.es.its.nyu.edu` (you can replace "zh1130" by your netID)

3. Make a directory on Dumbo
`mkdir /home/zh1130/PBDAAProject`

4. Open a new terminal window and transfer dataset csv file from laptop to dumbo
`scp ./Street_Construction_Permits.csv zh1130@dumbo.es.its.nyu.edu:/home/zh1130/PBDAAProject/Street_Construction_Permits.csv`

5. On the terminal window that have already loged in to dumbo, make a directory on HDFS
`hdfs dfs -mkdir /user/zh1130/PBDAAProject`

6. Put the dataset into HDFS
`hdfs dfs -put /home/zh1130/PBDAAProject/Street_Construction_Permits.csv /user/zh1130/PBDAAProject/`

7. Share dataset with other people (replace "netid" by the netID of the person you want to share with)
`hdfs dfs -setfacl -R -m default:user:netid:rwx /user/zh1130/PBDAAProject`
`hdfs dfs -setfacl -R -m user:netid:rwx /user/zh1130/PBDAAProject`


8. Check permissions
`hdfs dfs -getfacl /user/zh1130/PBDAAProject`
`hdfs dfs -getfacl /user/zh1130/PBDAAProject/Street_Construction_Permits.csv`


## Data Flow

The following are directories in this project and the purpose of the code in those directories. The code should be run from the following directories in the order presented below:

#### `/data_ingest`:
description for where to get the data, how it should be uploaded to Dumbo, HDFS. 

#### `/etl_code`:
MapReduce Code for cleaning the ingested datasets.  

#### `/profiling code`:
MapReduce Code for profiling the cleaned datasets. outputs the number of rows in a dataset, and can be used to compare the number of rows between the original dataset and the cleaned dataset

#### `/ana_code/aggregate_code`:
MapReduce Code for aggregating datasets. For collisions dataset, the total number of collisions (alongside total number of injuries, deaths) is stored for each intersection. For volume dataset, the total traffic over the 24 hour observation period is stored for each intersection. If there is more than 1 observation for an intersection, the averages of the observation is stored. For the permits dataset, the total number of permits are stored for each intersection. MapReduce Output can be read like a .csv file in Hive.

#### `/ana_code/merge_code`:
Hive code for merging datasets. Will import the MapReduce output from the aggregate MapReduce jobs and store as tables 'aggregatecollisions' 'aggregatepermits' and 'aggregatevolume'. Then will perform two joins: on tables 'aggregatecollisions' and 'aggregatepermits', and on tables 'aggregatecollisions' 'aggregatevolume'. The first joined dataset will be refered to as 'mergePermits' and the second as 'mergeVolume' from here on.  

####  `/ana_code/rank_code`:
Hive code for ranking the top intersections based on collisions as well as traffic volume. Produces results that can be substantively analyzed to find trends among intersections with high collisions. Results are not stored on HDFS (See `analytic-paper` for results).

#### `/ana_code/rank_permit_code`:
Hive code for ranking the top intersections based on permits, similar to the code above. Produces results that can be substantively analyzed to find trends among intersections with permit counts. Results are not stored on HDFS.

#### `/ana_code/regression_code`:
Spark code for tanking the merged datasets 'mergePermits' and 'mergeVolume' from Hive and performing a regression analysis on them. Produces results that can be interpreted to find relation between number of permits and amount of collisions, as well as volume of traffic and number of collisions. Results are not stored on HDFS.


