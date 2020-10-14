package com.newyorker

import com.newyorker.Functions._
import com.newyorker.Unpacker.unzipTar
import org.apache.commons.io.FilenameUtils.removeExtension
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession


object MainRunner {

  val spark = SparkSession.builder()
    .appName("New Yorker Parser")
    // .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {


    val (readPath, unzippedPath): (String, String) = args.size match {

      case 1 => {
        val readPath = args(0) + "yelp_dataset.tgz"
        val unzippedPath = args(0) + "output/"
        (readPath, unzippedPath)
      }
      case 2 => {
        val readPath = args(0) + "yelp_dataset.tgz"
        val unzippedPath = args(1)
        (readPath, unzippedPath)
      }
      case _ => {
        println("Please provide the 1st Argument as InputPath/ and 2nd Argument as OutputPath/ to application")
        ("1st Argumet", "2nd Argument")
      }
    }
    // val unzippedPath = "D:/PracticeProjects/newyorker_scripts/yelp_dataset/output/"

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    unzipTar(readPath, unzippedPath, fs)
    println("All json files are unzipped")

    val userJson = "yelp_academic_dataset_user.json"
    val businessJson = "yelp_academic_dataset_business.json"
    val checkinJson = "yelp_academic_dataset_checkin.json"
    val reviewJson = "yelp_academic_dataset_review.json"
    val tipJson = "yelp_academic_dataset_tip.json"

    val checkinDF = spark.read.json(unzippedPath + checkinJson)
    writePostgress(flatten_df(checkinDF), removeExtension(checkinJson))
    println("checkin data is available in postgress with table name as yelp_academic_dataset_checkin")

    val businessDF = spark.read.json(unzippedPath + businessJson)
    writePostgress(flatten_df(businessDF), removeExtension(businessJson))
    println("business data is available in postgress with table name as yelp_academic_dataset_business")

    val tipDF = spark.read.json(unzippedPath + tipJson)
    writePostgress(flatten_df(tipDF), removeExtension(tipJson))
    println("tip data is available in postgress with table name as yelp_academic_dataset_tip")

    val userDF = spark.read.json(unzippedPath + userJson)
    writePostgress(flatten_df(userDF), removeExtension(userJson))
    println("user data is available in postgress with table name as yelp_academic_dataset_user")

    val reviewDF = spark.read.json(unzippedPath + reviewJson)
    writePostgress(flatten_df(reviewDF), removeExtension(reviewJson))
    println("review data is available in postgress with table name as yelp_academic_dataset_review")
  }

}
