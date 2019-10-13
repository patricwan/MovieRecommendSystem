package com.java.statistics;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class StatisticsRecommender {

    static String MONGODB_MOVIE_COLLECTION = "Movie";
    static String MONGODB_RATING_COLLECTION = "Rating";

    //统计的表的名称
    static String RATE_MORE_MOVIES = "RateMoreMovies";
    static String RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies";
    static String AVERAGE_MOVIES = "AverageMovies";
    static String GENRES_TOP_MOVIES = "GenresTopMovies";


    public static void main(String[] args) {
        System.out.println("This is the statistics Recommender Start");
        Map configMap = new HashMap();
        configMap.put("spark.cores", "local[*]");
        configMap.put("mongo.uri", "mongodb://10.1.1.100:27017/recommender");
        configMap.put("mongo.db", "recommender");

        SparkConf sparkConf = new SparkConf().setMaster((String)configMap.get("spark.cores")).setAppName("OfflineRecommender");

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();


        Dataset<Row> ratingDF = spark.read().option("uri", (String)configMap.get("mongo.uri")).option("collection", MONGODB_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load();
        ratingDF.show();
        ratingDF.createOrReplaceTempView("ratings");

        Dataset<Row> movieDF = spark.read().option("uri", (String)configMap.get("mongo.uri")).option("collection", MONGODB_MOVIE_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load();
        movieDF.show();
        movieDF.createOrReplaceTempView("movies");

        //In history, rank by ratings
        Dataset<Row> rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid order by count desc");
        rateMoreMoviesDF.show();

        //average score of movie
        Dataset<Row> averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid order by avg desc");
        averageMoviesDF.show();

    }
}
