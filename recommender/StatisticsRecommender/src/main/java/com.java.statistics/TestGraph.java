package com.java.statistics;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.apache.log4j.*;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.graphx.*;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import  java.util.Arrays;


import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

public  class TestGraph {

    public static void main(String[] args) throws Exception {

        System.out.println("This is the start of the program main Test Graph");

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi").master("local[*]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());


        testSimplePi(jsc);



        spark.stop();
    }
    //https://blog.csdn.net/qq_41851454/article/details/80388443
    public static void testGraph1(JavaSparkContext jsc) {
        ArrayList vertexCol = new ArrayList<Tuple2<Long, Tuple2<String, Integer>>>();

       vertexCol.add(new Tuple2<Integer, Tuple2<String, Integer>>(1, new Tuple2<String, Integer>("Bob", 13)));
       vertexCol.add(new Tuple2<Integer, Tuple2<String, Integer>>(2, new Tuple2<String, Integer>("Alice", 23)));
       vertexCol.add(new Tuple2<Integer, Tuple2<String, Integer>>(3, new Tuple2<String, Integer>("Frank", 26)));
        vertexCol.add(new Tuple2<Integer, Tuple2<String, Integer>>(4, new Tuple2<String, Integer>("Frank", 26)));
        vertexCol.add(new Tuple2<Integer, Tuple2<String, Integer>>(5, new Tuple2<String, Integer>("Frank", 26)));
        vertexCol.add(new Tuple2<Integer, Tuple2<String, Integer>>(6, new Tuple2<String, Integer>("Frank", 26)));
        
        ArrayList edgeList = new ArrayList<Edge>();
        edgeList.add(new Edge(1,2, 7));
        edgeList.add(new Edge(3,4, 9));
        edgeList.add(new Edge(6,4, 12));

        JavaRDD<Tuple2<Long, Tuple2<String, Integer>>> vertexRDD = jsc.parallelize(vertexCol);

        JavaRDD<Edge> edgeRDD = jsc.parallelize(edgeList);

        //Graph graph = Graph.apply(vertexRDD, edgeRDD);





    }


    public static void testSimplePi(JavaSparkContext jsc) {
        int slices = 2;
        int n = 100000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map(integer -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y <= 1) ? 1 : 0;
        }).reduce((integer, integer2) -> integer + integer2);

        System.out.println("*******************************Pi is roughly " + 4.0 * count / n);

    }

}