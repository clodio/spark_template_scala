package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet
    // avgDepByReg: DataFrame = spark.read.parquet("/test/cities/v1/parquet")
    // val df= spark.read.options(Map("delimiter"->",", "header"->true)).csv("/data/raw/cities/v1/csv/laposte_hexasmal.csv")

    // val df = spark.read.options(Map("delimiter"->",","header"->"true")).csv("/data/raw/cities/v1/csv/laposte_hexasmal.csv")
    val df = spark.read.parquet("hdfs://localhost:9000/test/cities/v1/parquet")
    df.show()
    df.printSchema()
  }
}
