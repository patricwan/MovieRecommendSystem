package com.java.content;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.DenseVector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.commons.io.IOUtils;
import java.io.*;


// $example off$

//
public class JavaWord2VecExample {
    public static void main(String[] args) throws Exception {

        Map configMap = new HashMap();
        configMap.put("spark.cores", "local[*]");
        configMap.put("mongo.uri", "mongodb://10.1.1.100:27017/recommender");
        configMap.put("mongo.db", "recommender");

        SparkConf sparkConf = new SparkConf().setMaster((String)configMap.get("spark.cores")).setAppName("OfflineRecommender");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        String article1 = readFileContents("/root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/article1.txt");
        String article2 = readFileContents("/root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/article2.txt");


        // $example on$
        // Input data: Each row is a bag of words from a sentence or document.
        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList(article1.split(" "))),
                RowFactory.create(Arrays.asList(article2.split(" ")))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> documentDF = spark.createDataFrame(data, schema);

        // Learn a mapping from words to Vectors.
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("text")
                .setOutputCol("result")
                .setVectorSize(10)
                .setMinCount(0);

        Word2VecModel model = word2Vec.fit(documentDF);
        model.save("/root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/wordmodelarticle.txt");


        Dataset<Row> result = model.transform(documentDF);


        for (Row row : result.collectAsList()) {
            List<String> text = row.getList(0);
            Vector vector = (Vector) row.get(1);
            System.out.println("Text: " + text + " => \nVector: " + vector + "\n");
        }
        // $example off$

        result.printSchema();



        Word2VecModel sameModel = Word2VecModel.load("/root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/wordmodelarticle.txt");
        Dataset<Row> resultNew = sameModel.transform(documentDF);

        resultNew.foreach(eachOne -> {
            System.out.println("Vector of article " + eachOne.get(1));
        });

        spark.stop();
    }

    public static String readFileContents(String fileName) throws Exception {

        InputStream in = null;
        try {
            in = new FileInputStream(fileName);
            String fileContents;
            fileContents = IOUtils.toString(in);

            fileContents = fileContents.replace(",", " ");
            fileContents = fileContents.replace(".", " ");

            return fileContents;
        } catch (Exception e) {

        }
        finally {

        }

        return "";
    }

    public static Vector combineTwoVectors(Vector vecA, Vector vecB) {

        double[] vecAArray = vecA.toArray();
        double[] vecBArray = vecB.toArray();

        double[] oneArray = ArrayUtils.addAll(vecAArray, vecBArray);

        DenseVector denseVec = new DenseVector(oneArray);

        return denseVec;
    }
}
