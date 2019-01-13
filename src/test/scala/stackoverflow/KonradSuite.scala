package stackoverflow

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KonradSuite extends FunSuite with BeforeAndAfterAll with ShouldMatchers {
  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  ignore("toDebuString") {
    val largeList = (1 to 2000000).toList
    val wordRdd = sc.parallelize(largeList)
    val pairs = wordRdd.map(c => (c, 1))
      .groupByKey()
    val debugString = pairs.toDebugString

    val dep = pairs.dependencies

    println(debugString)
    println(dep)

  }

}
