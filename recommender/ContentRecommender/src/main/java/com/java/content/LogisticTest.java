package com.java.content;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import scala.collection.*;

import org.apache.spark.ml.linalg.Matrix;
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

public class LogisticTest {

    public static void main(String[] args) throws Exception  {
        System.out.println("This is the start of LogisticTest program");

        Matrix matrixData = loadFileToVector("/root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/logisticData.txt").transpose();
        System.out.println("matrix " + matrixData);

        //Get y vector, actually the third column of this matrix
        double[] doubleValues = {0.0,0.0,1.0};
        Vector denseVec1 =new DenseVector(doubleValues);

        DenseVector yVector = matrixData.multiply(denseVec1);
        System.out.println("yVector " + yVector);



    }

    public static Matrix loadFileToVector(String filePath) throws Exception {

        InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "UTF-8");
        BufferedReader bufferedreader = new BufferedReader(isr);
        String stemp;
        int i = 0;
        List<Double> allNumnbers = new ArrayList<Double>();
        while ((stemp = bufferedreader.readLine()) != null) {
            String[]  lineNumbers = stemp.split("\\s+");
            for (int j=0; j<lineNumbers.length; j++) {
                allNumnbers.add(new Double(lineNumbers[j]));
            }
            i++;
        }
        Object[]  allNumsArray = allNumnbers.toArray();
        double[] realData = new double[allNumsArray.length];
        for(int j=0; j<allNumsArray.length; j++) {
            realData[j] = ((Double)allNumsArray[j]).doubleValue();
        }

        Matrix denseMat1 = new DenseMatrix(3, allNumsArray.length/3,realData);

        return denseMat1;
    }

}
