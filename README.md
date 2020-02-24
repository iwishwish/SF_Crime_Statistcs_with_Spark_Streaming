# SF Crime Statistcs with Spark Streaming

### Q1: How did changing values on the SparkSession property parameters affect the throughput and latency of the data?    

When kafka produces data at the rate of 1/second and with maxOffsetsPerTrigger=200, tuning parameters doesn't bring much difference, it may due to the small scale of dataset. So all my tuning were based on all input data ready in kafka and spark streaming consume data from earliest.    
 
To increase maxOffsetsPerTrigger and number of cores lead to better performance throughput and latency in this case. In the code, I did twice repartition to match the number of cores, one was before aggregtion and the other was before join,  this is helpful to balance the use of memory.

### Q2: What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?    
Optimal combination of property:      
maxOffsetsPerTrigger = 48000    
master("local[16]")    
repartition(16)    
     
With these parameters, spark can analyze all the input data in 5 batches with batch triggered in 30 seconds interval and keep the peak of total memory usage under 500M. Whether increase or decrease these parameter the overal performance about time and memory will go bad.
