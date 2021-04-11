package com.RUSpark;

/* any necessary Java packages here */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NetflixGraphGenerate {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
      System.exit(1);
    }

        String InputPath = args[0];

        SparkSession spark = SparkSession
                .builder()
                .appName("NetflixGraphGenerate")
                .getOrCreate();

        JavaRDD<Row> lines = spark.read().format("csv").option("sep", ",").option("inferSchema", "true").load(InputPath).javaRDD();

        //movie, rating --> set of customer ids
        JavaPairRDD<Tuple2<Integer, Integer>, Set<Integer>> ratings = lines.mapToPair(line ->{
            Integer movie_id = line.getInt(0);
            Integer rating = line.getInt(2);
            Tuple2<Integer, Integer> key = new Tuple2(movie_id, rating);
            Set<Integer> customers = new HashSet<>();
            customers.add(line.getInt(1));
            return new Tuple2<>(key, customers);
        });

        JavaPairRDD<Tuple2<Integer, Integer>, Set<Integer>> ratings_by_customers = ratings.reduceByKey((x,y) -> {
            x.addAll(y);
            return new HashSet<>(x);
        });


        //make edges between all customers within set
        JavaRDD<Set<Integer>> ratings_by_customers_values = ratings_by_customers.values();



        JavaPairRDD<Tuple2<Integer, Integer>, Integer> customerPairs = ratings_by_customers_values.flatMapToPair(line ->{
            List<Tuple2<Tuple2<Integer, Integer>, Integer>> pair_list = new ArrayList<>();
            for(Integer i : line) {
                for (Integer j : line) {
                    if (i != j) {
                        Tuple2<Integer, Integer> customerPair = new Tuple2<>(i, j);
                        pair_list.add(new Tuple2(customerPair, 1));
                    }
                }
            }
            return pair_list.listIterator();
        });


        //reduce by key to get all occurrences

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> matchCustomerWeights = customerPairs.reduceByKey((i1, i2) -> i1 + i2);


        List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = matchCustomerWeights.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + " " + tuple._2());
        }

        spark.stop();


    }

}
