# SparkStreaming-Using-Scala-
Spark program that for each RDD of Dstream:  
1. Counts the word frequency and save the output on HDFS
2. Filters out the short words (i.e., < 5characters) and then counts the co-occurrence 
frequency of words
3. Filters out the short words (i.e., < 5 characters) and then counts the co-occurrence 
frequency of words using updateStateByKey operation




FILES:
- a4.scala -> source code for all three tasks
- Assignment4.zip --> scala project files zipped 
- Assignment4.jar --> Jar file for porject (no dependencies included as hadoop has all required spark dependencies )




Input files: any files inputted into the specified directory 
output_files: outputted in hadoop working directory 

- outputA3 -> contains:
	- TaskA-00X : directory containing TaskA output for file X inputted 
	- TaskB-00X : directory containing TaskB output for file X inputted 
	- TaskC: Cumulative TaskC output 
- RddCheckPoint: directory containing checkpoint data used for part C



Source Code: all source code should be located in s3923076_BDP_A4 file 
 	
	Assignment4.zip: Developed scala porject 
	Assignment4.jar: jar file created from scala project 
    

HOW TO RUN: 

1. Any files you wish to input into the spark streaming please make sure you upload it to the master node inlcuding the Assignment4.jar file 

2. Ideally , For checkpoint purposes you should make sure /RddCheckPoint directory exists in the output working directory (e.g. hadoop fs -mkdir /RddCheckPoint)
however it should still work otherwise and create that directory 

3. Whatever directory you wish to use as the input directory please make sure that directory exists and is empty before running 

4. Start streaming using:
spark-submit --class streaming.A4 --master yarn --deploy-mode client Assignment4.jar hdfs:///XXXX (enter input directory , e.g. user/s3923076)

5. Copy input files over to input directory
