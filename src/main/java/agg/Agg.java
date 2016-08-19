package agg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import constants.Constants;

/**
 * Has several types of aggregation.
 * Raul Barth
 */
public class Agg 
{
	private static String inputFile;
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		checkInputs(args);
	    
	    // Create a Java Spark Context
		JavaSparkContext sc = setSparkContext();
	    
	    // Create RDD
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		JavaRDD<String> newRDD = sc.textFile(inputFile).map(
	  	      new Function<String, String>() {
	  	        public String call(String line) {
	  	          line = line.split("cmlogs,")[1];
	  	          return line;
	  	        }
	  	});
		
		// Create DataFrame
	    DataFrame df = sqlContext.read().json(newRDD);
	    
	    //Explode fields pf _source
	    DataFrame dfAgg = df.select
	    		(df.col("_id"), 
	    		df.col("_source").getField("Service").as("Service"),
	    		df.col("_source").getField("CARF").as("CARF"),
	    		df.col("_source").getField("ElapsedTransactionTime").as("ElapsedTransactionTime").cast((DataTypes.IntegerType)));
	    
	   //Complete DataFrame - Explode fields pf _source
	    DataFrame dfFull = buildFullDataFrame(df);
	    //dfFull.printSchema();
	    //dfFull.show();    
	   	    
	    //Group By sendTimestamp
	    DataFrame dfagg1 = dfFull.groupBy(dfFull.col("ElapsedTransactionTime"))
		    	.agg(org.apache.spark.sql.functions.avg(dfFull.col("ElapsedTransactionTime")).as("Average of Time"));	    
	    
	    //dfFull.registerTempTable("testTable");
	    //sqlContext.sql("select CARF from testTable group by CARF").show();

	    //Filter for ElapsedTransactionTime > 4000
	    //dfFull.where("ElapsedTransactionTime > 4000").show();
	    	    
	    //Group By Service
	    /*DataFrame agg1 = dfAgg.groupBy(dfAgg.col("Service"))
	    	.agg(org.apache.spark.sql.functions.max(dfAgg.col("ElapsedTransactionTime")).as("Max of Time"), 
	    			org.apache.spark.sql.functions.sum(dfAgg.col("ElapsedTransactionTime")).as("Sum of Time"),
	    					org.apache.spark.sql.functions.avg(dfAgg.col("ElapsedTransactionTime")).as("Average of Time"));*/
	    
	   //Groupd By CARF
	   /* DataFrame agg2 = dfAgg.groupBy(dfAgg.col("CARF"))
	    	.agg(org.apache.spark.sql.functions.max(dfAgg.col("ElapsedTransactionTime")).as("Max of Time"), 
	    			org.apache.spark.sql.functions.sum(dfAgg.col("ElapsedTransactionTime")).as("Sum of Time"),
	    					org.apache.spark.sql.functions.avg(dfAgg.col("ElapsedTransactionTime")).as("Average of Time"));*/
	    
	   //Show DataFrame
	   /*agg1.show();
	    //Sort by column cresc
	    agg1.sort("Max of Time").show();
	    //Print Schema as a tree
	    agg1.printSchema();
	    //Query as a table
	    agg1.registerTempTable("testTable");
	    sqlContext.sql("select Service from testTable").show();
	    //Save the DataFrame (many partitions)
	    agg1.toJSON().saveAsTextFile("/testDataFrame");*/
	    //agg2.show();
	}
	
	private static JavaSparkContext setSparkContext(){
		SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME);
	    conf.set(Constants.HDFS_KEY, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set(Constants.LOCALFS_KEY, org.apache.hadoop.fs.LocalFileSystem.class.getName());
	    return new JavaSparkContext(conf);
	}
		
	private static void checkInputs(String[] args){
		if (args.length < Constants.ARGS_MIN){
			System.err.println(Constants.USAGE_TEXT);
			System.exit(1);
		}	
		else{
			inputFile = args[0];
		}
	}
	
	public static DataFrame buildFullDataFrame(DataFrame df){
		 DataFrame dfFull = df.select
		    		(df.col("_index"), df.col("_type"), df.col("_id"), df.col("_score"), 
		    		df.col("_source").getField("Service").as("Service"),
		    		df.col("_source").getField("Channel").as("Channel"),
		    		df.col("_source").getField("Peak").as("Peak"),
		    		df.col("_source").getField("Host").as("Host"),
		    		df.col("_source").getField("UserID").as("UserID"),
		    		df.col("_source").getField("meta").getField("id").as("metaId"),
		    		df.col("_source").getField("meta").getField("rev").as("metarev"),
		    		df.col("_source").getField("meta").getField("flags").as("metaflags"),
		    		df.col("_source").getField("TransactionID").as("TransactionID"),
		    		df.col("_source").getField("TransactionStatus").as("TransactionStatus"),
		    		df.col("_source").getField("DcxID").as("DcxID"),
		    		df.col("_source").getField("FromSap").as("FromSap"),
		    		df.col("_source").getField("Version").as("Version"),
		    		df.col("_source").getField("Organisation").as("Organisation"),
		    		df.col("_source").getField("sendTimestamp").as("sendTimestamp").cast(DataTypes.DateType),
		    		df.col("_source").getField("ElapsedServiceTime").as("ElapsedServiceTime").cast(DataTypes.IntegerType),
		    		df.col("_source").getField("EventType").as("EventType"),
		    		df.col("_source").getField("Prefix").as("Prefix"),
		    		df.col("_source").getField("ServiceVersion").as("ServiceVersion"),
		    		df.col("_source").getField("HostName").as("HostName"),
		    		df.col("_source").getField("CARF").as("CARF"),
		    		df.col("_source").getField("Source").as("Source"),
		    		df.col("_source").getField("ElapsedTransactionTime").as("ElapsedTransactionTime").cast(DataTypes.IntegerType));	    
		return dfFull;		
	}
}