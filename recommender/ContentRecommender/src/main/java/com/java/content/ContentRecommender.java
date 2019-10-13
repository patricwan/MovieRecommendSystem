package com.java.content;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.jblas.DoubleMatrix;
import java.util.*;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.map;

import scala.Tuple2;


public class ContentRecommender {
    static String MONGODB_MOVIE_COLLECTION = "Movie";

    static String CONTENT_MOVIE_RECS = "ContentMovieRecs";

    public static void main(String[] args) {
        System.out.println("This is the content Recommender Start");

        Map configMap = new HashMap();
        configMap.put("spark.cores", "local[*]");
        configMap.put("mongo.uri", "mongodb://10.1.1.100:27017/recommender");
        configMap.put("mongo.db", "recommender");

        SparkConf sparkConf = new SparkConf().setMaster((String)configMap.get("spark.cores")).setAppName("OfflineRecommender");

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();


        Dataset<Row> dataset = spark.read().option("uri", (String)configMap.get("mongo.uri")).option("collection", MONGODB_MOVIE_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load().select("mid", "name", "genres");
        //https://www.cnblogs.com/feiyumo/p/8763186.html
        Dataset<Row> datasetMovie =   dataset.withColumn("genres", regexp_replace(dataset.col("genres"), "(\\|)", " "));
        datasetMovie.cache();

       //datasetMovie.show();

       Tokenizer tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words");
       Dataset<Row> wordsData = tokenizer.transform(datasetMovie);
       HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50);

       Dataset<Row> featurizedData = hashingTF.transform(wordsData);

       IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
      // 训练idf模型，得到每个词的逆文档频率
       IDFModel idfModel = idf.fit(featurizedData);
       Dataset<Row> rescaledData = idfModel.transform(featurizedData);

       rescaledData.show();

       Encoder<Tuple2<Integer, double[]>> tupleEncoder = Encoders.tuple(Encoders.INT(),
       				spark.implicits().newDoubleArrayEncoder());
       Dataset<Tuple2<Integer, double[]>> mappedData = rescaledData.map((MapFunction<Row,Tuple2<Integer, double[]>>) originalRow -> {
           Row newRow = null;
           SparseVector vector = originalRow.getAs(5) ;
           double[] arrayDouble =  vector.toArray();
           return new Tuple2<Integer, double[]>(originalRow.getInt(0), arrayDouble);
       }, tupleEncoder);
       mappedData.show();

       JavaRDD<Tuple2<Integer, double[]>> movieFeaturesRdd = mappedData.rdd().toJavaRDD();
        JavaPairRDD<Integer, DoubleMatrix> javaPairMatrix =  movieFeaturesRdd.mapToPair(new PairFunction<Tuple2<Integer, double[]>, Integer, DoubleMatrix>() {
           private static final long serialVersionUID = 1L;
           @Override
           public Tuple2<Integer, DoubleMatrix> call(Tuple2<Integer, double[]> tuple) throws Exception {

               return new Tuple2(tuple._1, new DoubleMatrix(tuple._2));
           }
       });

        javaPairMatrix.collect();

       // javaPairMatrix.foreach(row -> {
         //   System.out.println(row);
        //});
        //https://blog.csdn.net/u013230189/article/details/81659963
        JavaPairRDD<Tuple2<Integer, DoubleMatrix>, Tuple2<Integer, DoubleMatrix>> cartesianRDD = javaPairMatrix.cartesian(javaPairMatrix)
                .filter( new Function<Tuple2<Tuple2<Integer, DoubleMatrix>, Tuple2<Integer, DoubleMatrix>>, Boolean>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Boolean call(Tuple2<Tuple2<Integer, DoubleMatrix>, Tuple2<Integer, DoubleMatrix>> tuple) throws Exception {
                        return tuple._1._1 != tuple._2._1;
                    }
                });

        JavaPairRDD<Integer, Tuple2<Integer, Double>> scoreRDD = cartesianRDD.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, DoubleMatrix>, Tuple2<Integer, DoubleMatrix>>, Integer, Tuple2<Integer, Double>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Integer, Tuple2<Integer, Double>> call(Tuple2<Tuple2<Integer, DoubleMatrix>, Tuple2<Integer, DoubleMatrix>> tuple) throws Exception {
                double simScore = consinSim(tuple._1._2, tuple._2._2);
                return new Tuple2<Integer, Tuple2<Integer, Double>>(tuple._1._1, new Tuple2<Integer, Double>(tuple._2._1, simScore));
            }
        }).filter(tuple -> {
            return tuple._2._2 > 0.6;
        });

        /*scoreRDD.foreach(row -> {
                      System.out.println(row);
        });*/

        JavaPairRDD<Integer,Iterable<Tuple2<Integer, Double>>> groupedSim = scoreRDD.groupByKey();
        JavaPairRDD<Integer, String> recommendedPairRDD= groupedSim.mapToPair(new PairFunction<Tuple2<Integer,Iterable<Tuple2<Integer, Double>>>, Integer,String>(){
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Integer,String> call(Tuple2<Integer,Iterable<Tuple2<Integer, Double>>> tuple) throws Exception {
                Iterator recommendedIter = tuple._2.iterator();
                List list = new ArrayList<Tuple2<Integer, Double>>();
                StringBuilder strRecomm = new StringBuilder();
                while(((Iterator) recommendedIter).hasNext()) {
                    Tuple2<Integer, Double> nextOne = (Tuple2<Integer, Double>)recommendedIter.next();
                    list.add(nextOne);
                    strRecomm.append(nextOne._1);
                    strRecomm.append("_");
                }
               /* list.sort(new Comparator() {
                    @Override
                    public int compare(Object tA, Object tB) {
                        double compareValues = ((Tuple2<Integer, Double>)tA)._2 - ((Tuple2<Integer, Double>)tB)._2;
                        if (compareValues >= 0) {
                            return 1;
                        } else {
                            return -1;
                        }
                    }
                });*/
                System.out.println("recommended " + list.size());
                return new Tuple2<Integer,String>(tuple._1, strRecomm.toString());
            }
        });

        recommendedPairRDD.foreach(row -> {
            System.out.println(row);
        });

    }

    // 求向量余弦相似度
    public static double consinSim(DoubleMatrix movie1, DoubleMatrix movie2) {
        return movie1.dot(movie2) / ( movie1.norm2() * movie2.norm2() );
    }
}