package bd201

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class WordCountTest {

  @Test def test() {
    // given
    val expected0 = new Tuple2[String, Int]("test2", 1)
    val expected1 = new Tuple2[String, Int]("test1", 2)
    val expected2 = new Tuple2[String, Int]("test", 3)

    // when
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))
    val file = sc.textFile("test.txt")
    val actual = WordCount.count(file, 0)

    // then
    assert(actual(0).equals(expected0))
    assert(actual(1).equals(expected1))
    assert(actual(2).equals(expected2))
  }

}
