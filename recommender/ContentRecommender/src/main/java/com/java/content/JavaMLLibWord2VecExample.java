package com.java.content;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


public class JavaMLLibWord2VecExample {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        Map configMap = new HashMap();
        configMap.put("spark.cores", "local[*]");

        SparkConf sparkConf = new SparkConf().setMaster((String) configMap.get("spark.cores")).setAppName("OfflineRecommender");
        //JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        JavaRDD<String> lines = spark.read().textFile("/root/github/MovieRecommendSystem/recommender/DataLoader/src/main/resources/movies.csv").javaRDD();
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        Word2Vec  word2vec = new Word2Vec();
        //Word2VecModel model = word2vec.fit(lines);




    }
}
