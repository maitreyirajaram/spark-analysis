package com.RUSpark;

/* any necessary Java packages here */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.util.List;

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];

        SparkSession spark = SparkSession
                .builder()
                .appName("NetflixMovieAverage")
                .getOrCreate();

        JavaRDD<Row> lines = spark.read().format("csv").option("sep", ",").option("inferSchema", "true").load(InputPath).javaRDD();


        JavaPairRDD<Integer, Tuple2<Integer, Integer>> ratings = lines.mapToPair(line ->{
            Integer key = line.getInt(0);
            Tuple2<Integer, Integer> ratingsPerMovie = new Tuple2<>(line.getInt(2), 1);
            return new Tuple2<>(key, ratingsPerMovie);
        });

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> sum_ratings = ratings.reduceByKey((x,y) -> new Tuple2<Integer, Integer>(x._1() + y._1(), x._2() + y._2()));

        //x._1 - key, x._2 - value Tuple2
        JavaPairRDD<Integer, Double> average_ratings = sum_ratings.mapToPair(x -> new Tuple2(x._1(), ((double)(x._2()._1()) / x._2()._2())));

        List<Tuple2<Integer, Double>> output = average_ratings.collect();
        for (Tuple2<?,?> tuple : output) {
            Double avg_movie = Double.parseDouble(new DecimalFormat("#.##").format(tuple._2));
            System.out.println(tuple._1() + " " + avg_movie);
        }

        spark.stop();
		
	}

}
