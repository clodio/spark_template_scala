// from pyspark.sql import functions as F
// from pyspark.sql.functions import udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object Departments {
  
  val DEPARTEMENT = "departement"
  val NB_LOCALITY = "nb_localites"
  
  // @staticmethod
  def read(spark: SparkSession): DataFrame = {
    return spark.read.parquet("hdfs://localhost:9000/refined/departement/v1/csv")
  }
  // @staticmethod
  def write(df: DataFrame) = {
    df.write.mode("overwrite").csv("hdfs://localhost:9000/refined/departement/v1/csv")
  }
  // @staticmethod
  def write_stats(df: DataFrame) = {
    df.write.mode("overwrite").csv("hdfs://localhost:9000/refined/departement/v3/csv")
  }
  
  // @staticmethod
  def sort_by_nb_locality(df: DataFrame): DataFrame = {
    return df.orderBy(col(Departments.NB_LOCALITY).asc)
  }
  // @staticmethod
  def group_by_departments(df: DataFrame):DataFrame  =  {
    return df.groupBy(Departments.DEPARTEMENT).agg(count("*").alias(Departments.NB_LOCALITY))
  }
  // // @staticmethod
  // def group_by_departments_with_corse_in_numeric(df: DataFrame): DataFrame = {
    
  //   // # @uf('string') remplace plus simplement get_department_with_corse_in_numericUDF = udf(lambda x:Departments.get_department_with_corse_in_numeric(x))
  //   @udf('string')
  //   def get_department_with_corse_in_numeric(department):
  //     return_departement = ""
  //     if department == "2A":
  //       return_departement= "20"
  //     elif department == "2B":
  //       return_departement= "20"
  //     else:
  //       return_departement = department
  //     return return_departement

  //   df_with_corse_numeric = dwithColumn(Departments.DEPARTEMENT, get_department_with_corse_in_numeric(df[Departments.DEPARTEMENT]))

  //   return df_with_corse_numeric.groupBy(Departments.DEPARTEMENT).agg(count("*").alias(Departments.NB_LOCALITY))
  // }
    
}