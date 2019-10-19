package com.atguigu.content

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.math._

object cf {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("CF Spark")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("/root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/ratings.csv")
    val output_path = "/root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/cfoutput.csv"

    val max_prefs_per_user = 20
    val topn = 5

    //Step 1. normalization, Obtain UI Matrix
    val ui_rdd = lines.map { x =>
      val ss = x.split(",")
      val userid = ss(0).toString
      val itemid = ss(1).toString
      val score = ss(2).toDouble

      (userid, (itemid, score))
    }.groupByKey().flatMap { x =>
      val userid = x._1
      val is_list = x._2
      val is_arr = is_list.toArray
      var is_arr_len = is_arr.length
      if (is_arr_len > max_prefs_per_user) {
        is_arr_len = max_prefs_per_user
      }

      var i_us_arr = new ArrayBuffer[(String, (String, Double))]
      for (i <- 0 until is_arr_len) {
        val itemid = is_arr(i)._1
        val score = is_arr(i)._2
        i_us_arr += ((itemid, (userid, score)))
      }
      i_us_arr
    }.groupByKey().flatMap { x =>
      val itemid = x._1
      val us_list = x._2
      val us_arr = us_list.toArray
      var sum:Double = 0.0
      for (i <- 0 until us_arr.length) {
        sum += pow(us_arr(i)._2, 2)
      }
      sum = sqrt(sum)

      var u_is_arr = new ArrayBuffer[(String, (String, Double))]
      for (i <- 0 until us_arr.length) {
        val userid = us_arr(i)._1
        val score = us_arr(i)._2 / sum
        u_is_arr += ((userid, (itemid, score)))
      }
      u_is_arr
    }.groupByKey()

    //Step 2. pairs
    val pairs_rdd = ui_rdd.flatMap { x =>
      val is_arr = x._2.toArray

      var ii_s_arr = new ArrayBuffer[((String, String), Double)]()
      for (i <- 0 until is_arr.length - 1) {
        for (j <- i + 1 until is_arr.length) {
          val item_a = is_arr(i)._1
          val item_b = is_arr(j)._1
          val score_a = is_arr(i)._2
          val score_b = is_arr(j)._2

          ii_s_arr += (((item_a, item_b), score_a * score_b))
          ii_s_arr += (((item_b, item_a), score_a * score_b))
        }
      }
      ii_s_arr
    }.groupByKey()

    // Step3. sum and output
    pairs_rdd.map { x=>
      val ii_pair = x._1
      val s_list = x._2
      val s_arr = s_list.toArray
      val len = s_arr.length
      var score:Double = 0.0

      for (i <- 0 until len){
        score += s_arr(i)
      }
      val item_a = ii_pair._1
      val item_b = ii_pair._2
      (item_a, (item_b, score))
    }.groupByKey().map { x=>
      val item_a = x._1
      val rec_list = x._2
      val rec_arr = rec_list.toArray.sortWith(_._2 > _._2)

      var len = rec_arr.length

      if (len > topn) {
        len = topn
      }

      val s = new StringBuilder
      for (i <- 0 until len){
        val item = rec_arr(i)._1
        val score = "%1.3f" format rec_arr(i)._2
        s.append(item + ":" + score.toString)
        if (i != len -1) {
          s.append(",")
        }
      }

      item_a + "\t" + s
    }.saveAsTextFile(output_path)
  }
}

