package com.java.content;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;

public class JavaWord2VecLoadModel {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("OfflineRecommender");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        Word2VecModel sameModel = Word2VecModel.load("/root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/wordmodelarticle.txt");



    }

}
