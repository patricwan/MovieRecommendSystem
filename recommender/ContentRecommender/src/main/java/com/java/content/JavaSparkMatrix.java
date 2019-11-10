package com.java.content;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;

import org.apache.spark.ml.linalg.SparseMatrix;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

//https://spark.apache.org/docs/latest/api/java/org/apache/spark/mllib/linalg/Matrix.html#map-scala.Function1-
//https://spark.apache.org/docs/latest/mllib-data-types.html
public class JavaSparkMatrix {

    public static void main(String[] args) {
        System.out.println("This is the start of JavaSparkMatrix program");

    }

    public static void matrixTest() {
        double[] doubleValues = {1.0,2.0,4.0};

        Vector denseVec1 =new DenseVector(doubleValues);

        System.out.println("denseVec1" + denseVec1);

        //IndexedRow indexRow = new IndexedRow(2,denseVec1);

        double[] data1 = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0 };


        DenseMatrix denseMat1 = new DenseMatrix(3,3,data1);

        System.out.println("denseMat" + denseMat1);

        DenseMatrix denseMat1AllOnes = DenseMatrix.ones(3,3);

        DenseMatrix resMatrix = denseMat1.multiply(denseMat1AllOnes);

        System.out.println("resMatrix" + resMatrix);

        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{1.0, -2.0})),
                RowFactory.create(Vectors.dense(4.0, 5.0, 0.0, 3.0)),
                RowFactory.create(Vectors.dense(6.0, 7.0, 0.0, 8.0)),
                RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{9.0, 1.0}))
        );

        Map configMap = new HashMap();
        configMap.put("spark.cores", "local[*]");
        SparkConf sparkConf = new SparkConf().setMaster((String) configMap.get("spark.cores")).setAppName("ItemFilterDataset");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext context = new JavaSparkContext(sparkConf);

       /* JavaRDD<Vector> rows = context.parallelize(
                Arrays.asList(
                        Vectors.dense(1.0, 10.0, 100.0),
                        Vectors.dense(2.0, 20.0, 200.0),
                        Vectors.dense(3.0, 30.0, 300.0)
                )
        ); // an RDD of Vectors
        RowMatrix mat = new RowMatrix(rows.rdd());

        // Get its size.
        long m = mat.numRows();
        long n = mat.numCols();*/

    }



}
