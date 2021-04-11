package com.RUSpark;

/* any necessary Java packages here */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */

        SparkSession spark = SparkSession
                .builder()
                .appName("RedditPhotoImpact")
                .getOrCreate();

        JavaRDD<Row> lines = spark.read().format("csv").option("sep", ",").option("inferSchema", "true").load(InputPath).javaRDD();
        //JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

        JavaPairRDD<Integer, Integer> imageImpact = lines.mapToPair(line ->{
            //System.out.println(line);
            //String[] columns = line.split(",");
            //System.out.println(line.getInt(0) + line.getInt(4)+ line.getInt(5) + line.getInt(6));
            Integer key = line.getInt(0);
            Integer scorePerImage= line.getInt(4)+line.getInt(5)+line.getInt(6);
            return new Tuple2<>(key, scorePerImage);
        });

        JavaPairRDD<Integer, Integer> counts = imageImpact.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<Integer, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + " " + tuple._2());
        }

        spark.stop();
		
	}

}
