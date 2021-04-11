package com.RUSpark;

/* any necessary Java packages here */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import java.util.*;
import java.text.*;
import java.util.List;

public class RedditHourImpact {


    private static String getDateKey(Long unix_timestamp){

        Date date = new Date((unix_timestamp)*1000L);
        SimpleDateFormat jdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        jdf.setTimeZone(TimeZone.getTimeZone("EST"));
        String date_full = jdf.format(date);
        String key = date_full.split(" ")[1].split(":")[0];
        return key;
    }
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];



        SparkSession spark = SparkSession
                .builder()
                .appName("RedditHourImpact")
                .getOrCreate();

        JavaRDD<Row> lines = spark.read().format("csv").option("sep", ",").option("inferSchema", "true").load(InputPath).javaRDD();
        //JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

        JavaPairRDD<String, Integer> imageImpact = lines.mapToPair(line ->{
            Long unix_timestamp = Long.valueOf(line.getInt(1));
            String key = getDateKey(unix_timestamp);
            Integer scorePerImage= line.getInt(4)+line.getInt(5)+line.getInt(6);
            return new Tuple2<>(key, scorePerImage);
        });

        JavaPairRDD<String, Integer> counts = imageImpact.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + " " + tuple._2());
        }

        spark.stop();
	}

}
