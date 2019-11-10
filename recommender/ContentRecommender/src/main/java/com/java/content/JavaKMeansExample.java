package com.java.content;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

// $example on$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.*;
// $example off$

public class JavaKMeansExample {
    public static void main(String[] args) {

        Map configMap = new HashMap();
        configMap.put("spark.cores", "local[*]");
        configMap.put("mongo.uri", "mongodb://10.1.1.100:27017/recommender");
        configMap.put("mongo.db", "recommender");


        SparkConf sparkConf = new SparkConf().setMaster((String)configMap.get("spark.cores")).setAppName("OfflineRecommender");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // $example on$
        // Load and parse data
        String path = "/root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/kmeans_data.txt";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);
        });
        parsedData.cache();

        // Cluster the data into two classes using KMeans
        int numClusters = 2;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        System.out.println("Cluster centers:");
        for (Vector center: clusters.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = clusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        // Save and load model
        clusters.save(jsc.sc(), "KMeansModel");
        KMeansModel sameModel = KMeansModel.load(jsc.sc(),
                "KMeansModel");
        // $example off$

        //Predict which cluster it belongs to
        //sameModel.predict()
        List<Vector> list = parsedData.collect();
        for (Vector eachVec: list) {
            int clusterNo = sameModel.predict(eachVec);
            System.out.println(" vector " + eachVec + " belongs to cluster no " + clusterNo);
        }

        System.out.println("Cluster Centers " + sameModel.clusterCenters());

        jsc.stop();
    }
}


