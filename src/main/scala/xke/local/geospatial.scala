// from tokenize import Double
// from pyspark.sql import functions as F
// from pyspark.sql.functions import udf
// from pyspark.sql.types import StringType, DoubleType,FloatType
// import pyspark.sql.types as T
// from spark_cities.models.cities import Cities
// from spark_cities.models.departments import Departments
// from pyspark.sql.functions import when
// from pyspark.sql.window import Window
// from pyspark.sql.functions import col
// import math
// import numpy as np

import scala.math 
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.expression._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.expressions.Window
import Math.pow

object  Geospatial {

  def split_lat_long(df: DataFrame, lat_long_col: String): DataFrame = {

    return df.withColumn("latitude", df(lat_long_col).split(",").getItem(0).toLong) 
          .withColumn("longitude", df(lat_long_col).split(",").getItem(1).toLong ) 
          .drop(lat_long_col)
  }

  def add_departement_column_from_postal_code(df: DataFrame): DataFrame = {

    val cities_clean_with_dept: DataFrame = df.withColumn("departement", 
          when ( ( (df(Cities.CODE_POSTAL).toInt >= 20000 ) & (df(Cities.CODE_POSTAL).toInt < 20200 )), "2A" ) 
          .when( ((df(Cities.CODE_POSTAL).toInt >= 20200) & (df(Cities.CODE_POSTAL).toInt < 30000 )),"2B") 
          .otherwise(substring(df(Cities.CODE_POSTAL), 1, 2)))

    val cities_clean_with_dept_string = cities_clean_with_dept.withColumn("departement", col("departement").toString)
    return cities_clean_with_dept_string
  }

  def get_department_from_postal_codeMyUDF(postal_code_to_analyse: String): String = {
      var return_departement: String = ""
      val postal_code: Int = postal_code_to_analyse.toInt
      if ( postal_code >= 20000 & postal_code < 20200 ) {
        return_departement= "2A"
      }
      else if ( postal_code >= 20000 & postal_code < 30000 ) {
        return_departement= "2B"
      }
      else {
          return_departement = postal_code_to_analyse.substring(2)
      }
      return return_departement
    }

  def add_departement_column_from_postal_codeUDF(df: DataFrame): DataFrame = {

    val cities_clean_with_dept: DataFrame = df.withColumn(Departments.DEPARTEMENT, get_department_from_postal_codeMyUDF(df(Cities.CODE_POSTAL)))

    val cities_clean_with_dept_string: DataFrame = cities_clean_with_dept.withColumn("departement", col("departement").toString)
    return cities_clean_with_dept_string
  }
  // @TODO faire des  udf
  def distance(a: String, b: String): Float = {
      // @TODO  transformer les var en val et revoir toute cette fonction
      var a_lat:Float ="-12.7812298".toFloat
      var a_lon:Float ="45.2304999".toFloat
      var b_lat:Float ="-12.7812298".toFloat
      var b_lon:Float ="45.2304999".toFloat
      if (  a == "".toString or b == "".toString) {
        // # cas de mayotte non geolocalisee dans le fichier
        a_lat = "-12.7812298".toFloat
        a_lon = "45.2304999".toFloat
        b_lat = "-12.7812298".toFloat
        b_lon = "45.2304999".toFloat
      }
      else {
        val a_array = a.split(",")
        val b_array = b.split(",")
        a_lat =  a_array(0).toFloat
        a_lon =  a_array(1).toFloat
        b_lat =  b_array(0).toFloat
        b_lon =  b_array(1).toFloat
      }
        // # Todo: A ameliorer ce n'est pas un calcul de distance parfait du fait de la rotondite de la terre
      return ((a_lat - b_lat) * (a_lat - b_lat)  + (a_lon - b_lon)*(a_lon - b_lon)).sqrt()
  }

  def add_prefecture_geoloc_and_distance(df: DataFrame): DataFrame = {

    val window = window.partitionBy(Departments.DEPARTEMENT).orderBy(Cities.CODE_POSTAL)

    val df_with_prefecture_geoloc: DataFrame  = df.withColumn("geoloc_prefecture", first(Cities.COORDONNES_GPS, true).over(window))

    val df_with_distance_from_prefecture: DataFrame  = df_with_prefecture_geoloc.withColumn("geoloc_prefecture", first(Cities.COORDONNES_GPS, true).over(window))

    val df_with_distance_from_prefecture: DataFrame = df_with_prefecture_geoloc.withColumn("distance", distance(df_with_prefecture_geoloc(Cities.COORDONNES_GPS),df_with_prefecture_geoloc("geoloc_prefecture")))
    
    return df_with_distance_from_prefecture
  }


  def get_distance_stats_from_prefecture(df: DataFrame): DataFrame = {

    // # Impossible avec agg et les fonctions par defaut car percentile_approx EXISTE seulement en spark > 3.1
    // # df_with_stats = dgroupBy(Departments.DEPARTEMENT) \
    // #     .agg(percentile_approx("distance", 0.5, 10000).alias("dist_mean"), \
    // #         avg("distance").alias("dist_avg") \
    // #     )
    // # percentile_approx("distance", 0.5, 10000).alias("dist_mean") --> EXISTE seulement en spark > 3.1
    // # df_with_stats.show()

    // # avec Window
    val window = Window.partitionBy(Departments.DEPARTEMENT)
    val df_with_avg = df.withColumn("dist_avg", avg("distance").over(window))

    // # percentile_approx(distance, 0.5) --> calcule la mediane
    val magic_percentile = expr("percentile_approx(distance, 0.5)").over(window)
    val df_with_avg_and_mean: DataFrame = df_with_avg.withColumn("dist_mean", magic_percentile)

    // # Menage
    val df_cleaned: DataFrame = df_with_avg_and_mean.select("departement","dist_mean","dist_avg").drop_duplicates()
    df_cleaned.show()

    return df_cleaned
    // # return df_with_stats
  }
}