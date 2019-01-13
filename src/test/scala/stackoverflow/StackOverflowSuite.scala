package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import stackoverflow.StackOverflow.{groupedPostings, rawPostings, sc, scoredPostings, vectorPostings}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll with ShouldMatchers {
  val qJava = Posting(1, 1, None, None, 8, Some("Java"))
  val anJava1 = Posting(2, 2, None, Some(1), 2, None)
  val anJava2 = Posting(2, 3, None, Some(1), 7, None)
  val qCpp = Posting(1, 4, None, None, 8, Some("C++"))
  val anCpp = Posting(2, 5, None, Some(4), 4, None)
  val postings = sc.makeRDD(List(qJava, qCpp, anJava1, anCpp, anJava2))

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("groupedPostings") {
    val result: Map[QID, Iterable[(Question, Answer)]] = testObject.groupedPostings(postings).collect().toMap

    result(1) should equal(List((qJava, anJava1), (qJava, anJava2)))
    result(4) should equal(List((qCpp, anCpp)))
  }

  test("scoredPostings") {
    val grouped: RDD[(QID, Iterable[(Question, Answer)])] = testObject.groupedPostings(postings)
    val result = testObject.scoredPostings(grouped).collect().toMap
    result(qJava) should equal(7)
    result(qCpp) should equal(4)
  }

  test("vectorPostings") {
    val grouped: RDD[(QID, Iterable[(Question, Answer)])] = testObject.groupedPostings(postings)
    val scored = testObject.scoredPostings(grouped)
    val result = testObject.vectorPostings(scored).collect().toMap

    result(50000) should equal(7)
    result(250000) should equal(4)
  }

  test("computeLangLabel") {
    testObject.computeLangLabel(List((50000, 10), (0, 2), (0, 4), (100000, 5))) should equal("JavaScript")
    testObject.computeLangLabel(List((50000, 10), (0, 2), (50000, 4), (100000, 5))) should equal("Java")
    //    testObject.computeLangLabel(List()) should equal("Java")
  }

  test("computeLangPercent") {
    testObject.computeLangPercent(List((50000, 10), (0, 2), (0, 4), (100000, 5))) should equal(50.0)
    testObject.computeLangPercent(List((50000, 10), (0, 2), (50000, 4), (100000, 5), (50000, 10))) should equal(60.0)
  }

  test("computeMedianScore") {
    testObject.computeMedianScore(List((50000, 10), (0, 2), (0, 4), (100000, 5))) should equal(4)
    testObject.computeMedianScore(List((50000, 10), (0, 2), (50000, 4), (100000, 5), (50000, 10))) should equal(5)
  }

  test("kmeans noUnexpectedElements") {
    val vectors = StackOverflow.sc.parallelize(List((150000, 2), (450000, 30), (150000, 22), (450000, 20), (450000, 25), (0, 10), (0, 14), (0, 30)))
    val means = Array((150000, 2), (450000, 20), (150000, 22), (150000, 2), (0, 14), (0, 30), (0, 14))
    val result = testObject.kmeans(means, vectors, 120)
    result should equal(Array((150000, 2), (450000, 25), (150000, 22), (150000, 2), (0, 12), (0, 30), (0, 14)))
  }

  test("cluster from forum") {
    val vectors = StackOverflow.sc.parallelize(List((450000, 39), (500000, 31), (150000, 1), (150000, 10), (500000, 55), (150000, 2), (150000, 22)))

    val means = Array((500000, 13), (150000, 10))

    var results: Array[(String, Double, Int, Int)] = testObject.clusterResults(means, vectors)

    testObject.printResults(results)

    println(results(0))

    println(results(1))

    assert(results.contains("Python", 100.0, 4, 6)) //I like python~!

    assert(results.contains("Scala", 66.66666666666667, 3, 39))

  }

  ignore("integration on big dataset very long") {
    val lines = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored = scoredPostings(grouped)

    val css = (Posting(1, 6, None, None, 140, Some("CSS")), 67)
    val php = (Posting(1, 42, None, None, 155, Some("PHP")), 89)
    val ruby = (Posting(1, 72, None, None, 16, Some("Ruby")), 3)
    val java = (Posting(1, 126, None, None, 33, Some("Java")), 30)
    val cSharp = (Posting(1, 174, None, None, 38, Some("C#")), 20)

    val all = Set(css, php, ruby, java, cSharp)

    val resultScored = scored.filter(all.contains).collect().toSet
    resultScored should equal(all)
    scored.count() should equal(2121822)

    val vectors = vectorPostings(scored)
    val expectedVectors = Set((350000, 67), (100000, 89), (300000, 3), (50000, 30), (200000, 20))
    val resultVectors = vectors.filter(expectedVectors.contains).collect().toSet
    resultVectors should equal(expectedVectors)
  }


}
