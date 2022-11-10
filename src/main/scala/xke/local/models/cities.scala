import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expression._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object Cities {
  
  val CODE_POSTAL: String = "code_postal"
  val CODE_INSEE: String  = "code_commune_insee"
  val NOM_COMMUNE: String  = "nom_de_la_commune"
  val LIGNE5: String  = "ligne_5 "
  val LIBELLE_D_ACHEMINEMENT: String  = "libelle_d_acheminement "
  val COORDONNES_GPS: String  = "coordonnees_gps"

  // @staticmethod
  def read(spark: SparkSession): DataFrame = {
    //return spark.read.csv("/data/raw/cities/v1/csv/laposte_hexasmal.csv", header=True, sep=";")
    return spark.read.parquet("hdfs://localhost:9000//test/cities/v1/parquet")
  }
  // @staticmethod
  def write(df: DataFrame) = {
    df.write.mode("overwrite").parquet("hdfs://localhost:9000/refined/cities/v1/parquet")
  }
}