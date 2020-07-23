package bd201

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val threshold = args(1).toInt

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))
    val file = sc.textFile(args(0))

    System.out.println(count(file, threshold).mkString(", "))
  }

  def count(rdd: RDD[String], threshold: Int): Array[(String, Int)] = {
    rdd
      .flatMap(line => line.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
//      .filter(_._2 >= threshold)
//      .flatMap(_._1.toCharArray)
//      .map((_, 1))
//      .reduceByKey(_ + _)
      .collect()
  }
}
