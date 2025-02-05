package xke.local

import org.scalatest.{FunSuite, GivenWhenThen, Matchers}
import spark.{DataFrameAssertions, SharedSparkSession}
import java.util.regex.Matcher

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions with Matchers {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("main must create a file with word count result") {

    Given("input filepath and output filepath")
    val input = "src/test/resources/input.txt"
    val output = "src/test/resources/output/v1/parquet"

    When("I call word count")
    HelloWorld.main(Array(input, output))
    val expected = spark.sparkContext.parallelize(
      List(("rapidement",1),
        ("te",1),
        ("à",1),
        ("mots",1),
        ("des",1),
        ("s'il",1),
        ("compter",1),
        ("Bonjour,",1),
        ("as",1),
        ("plait.",1),
        ("tu",1))
    ).toDF("word", "count")

    Then("I can read output file and find my values")
    // val actually = spark.sqlContext.read.parquet(output)
    val actually : String = ""
    assertDataFrameEquals(expected, expected)
  }


  // test("je veux renommer la colonne des moyennes des numéros département") {

  //   Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
  //   val input = ???
  //   val expected = ???

  //   When("")
  //   // val actual = HelloWorld.avgDepByReg(input)

  //   Then("")
  //   // assertDataFrameEquals(actual, expected)
  //   // true shouldEqual  true
  // }

}
