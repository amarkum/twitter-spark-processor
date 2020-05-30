# twitter-spark-processor
Twitter Spark Processor Application cosnumes messages from multiple kafka topic and thier partition<br/>
It serailizes and stores only the specific attributes from tweet JSON, serialzes and writes it to AVRO.<br/>

When Shutdown Hook is called, It gracefully shuts downs the SparkSession and flushes the Avro to a file in local system.
