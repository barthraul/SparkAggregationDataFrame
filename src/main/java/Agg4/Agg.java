package Agg4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

/**
 * Has several types of aggregation.
 * Raul Barth
 */
public class Agg 
{
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
		      System.err.println("Usage: AggregateExample4 <inputFile>");
		      System.exit(1);
		}
	    String inputFile = args[0];
	    
	    // Create a Java Spark Context
	    SparkConf conf = new SparkConf().setAppName("Agg4-AggregationWithDataFrame");
	    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    //SPARK SQL EXAMPLE
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		JavaRDD<String> newRDD = sc.textFile(inputFile).map(
	  	      new Function<String, String>() {
	  	        public String call(String line) {
	  	          line = line.split("cmlogs,")[1];
	  	          return line;
	  	        }
	  	});
	    DataFrame df = sqlContext.read().json(newRDD);
	    
	    //Explode fields pf _source
	    DataFrame dfAgg = df.select
	    		(df.col("_id"), 
	    		df.col("_source").getField("Service").as("Service"),
	    		df.col("_source").getField("CARF").as("CARF"),
	    		df.col("_source").getField("ElapsedTransactionTime").as("ElapsedTransactionTime").cast((DataTypes.IntegerType)));
	    
	    //Group By Service
	    DataFrame agg1 = dfAgg.groupBy(dfAgg.col("Service"))
	    	.agg(org.apache.spark.sql.functions.max(dfAgg.col("ElapsedTransactionTime")).as("Max of Time"), 
	    			org.apache.spark.sql.functions.sum(dfAgg.col("ElapsedTransactionTime")).as("Sum of Time"),
	    					org.apache.spark.sql.functions.avg(dfAgg.col("ElapsedTransactionTime")).as("Average of Time"));
	    
	    //Groupd By CARF
	    DataFrame agg2 = dfAgg.groupBy(dfAgg.col("CARF"))
	    	.agg(org.apache.spark.sql.functions.max(dfAgg.col("ElapsedTransactionTime")).as("Max of Time"), 
	    			org.apache.spark.sql.functions.sum(dfAgg.col("ElapsedTransactionTime")).as("Sum of Time"),
	    					org.apache.spark.sql.functions.avg(dfAgg.col("ElapsedTransactionTime")).as("Average of Time"));
	    
	    //Show DataFrame
	    agg1.show();
	    //Sort by column cresc
	    agg1.sort("Max of Time").show();
	    //Print Schema as a tree
	    agg1.printSchema();
	    //Query as a table
	    agg1.registerTempTable("testTable");
	    sqlContext.sql("select Service from testTable").show();
	    //Save the DataFrame (many partitions)
	    agg1.toJSON().saveAsTextFile("/testDataFrame");
	    //agg2.show();
	}
}