package com.newyorker

import com.newyorker.MainRunner.{spark}
import org.apache.spark.sql.{DataFrame, SaveMode}

object Functions {
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/newyorker"
  val user = "docker"
  val password = "docker" // not to be done in production environment

  def readJson(jsonName: String) = {
    //spark.read.json(writePath+jsonName)
  }

  def writePostgress(df: DataFrame, tableName: String) = {
    df.write
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Overwrite)
      .option("dbtable", tableName)
      .save()
  }

  val totalMainArrayBuffer = collection.mutable.ArrayBuffer[String]()

  def flatten_df_Struct(dfTemp: org.apache.spark.sql.DataFrame, dfTotalOuter: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    //dfTemp.printSchema
    val totalStructCols = dfTemp.dtypes.map(x => x.toString.substring(1, x.toString.size - 1)).filter(_.split(",", 2)(1).contains("Struct")) // in case i the column names come with the word Struct embedded in it
    val mainArrayBuffer = collection.mutable.ArrayBuffer[String]()
    for (totalStructCol <- totalStructCols) {
      val tempArrayBuffer = collection.mutable.ArrayBuffer[String]()
      tempArrayBuffer += s"${totalStructCol.split(",")(0)}.*"
      //tempArrayBuffer.toSeq.toDF.show(false)
      val columnsInside = dfTemp.selectExpr(tempArrayBuffer: _*).columns
      for (column <- columnsInside)
        mainArrayBuffer += s"${totalStructCol.split(",")(0)}.${column} as ${totalStructCol.split(",")(0)}_${column}"
      //mainArrayBuffer.toSeq.toDF.show(false)
    }
    //dfTemp.selectExpr(mainArrayBuffer:_*).printSchema
    val nonStructCols = dfTemp.selectExpr(mainArrayBuffer: _*).dtypes.map(x => x.toString.substring(1, x.toString.size - 1)).filter(!_.split(",", 2)(1).contains("Struct")) // in case i the column names come with the word Struct embedded in it
    for (nonStructCol <- nonStructCols)
      totalMainArrayBuffer += s"${nonStructCol.split(",")(0).replace("_", ".")} as ${nonStructCol.split(",")(0)}" // replacing _ by . in origial select clause if its an already nested column
    dfTemp.selectExpr(mainArrayBuffer: _*).dtypes.map(x => x.toString.substring(1, x.toString.size - 1)).filter(_.split(",", 2)(1).contains("Struct")).size
    match {
      case value if value == 0 => dfTotalOuter.selectExpr(totalMainArrayBuffer: _*)
      case _ => flatten_df_Struct(dfTemp.selectExpr(mainArrayBuffer: _*), dfTotalOuter)
    }
  }

  def flatten_df(dfTemp: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    var totalArrayBuffer = collection.mutable.ArrayBuffer[String]()
    val totalNonStructCols = dfTemp.dtypes.map(x => x.toString.substring(1, x.toString.size - 1)).filter(!_.split(",", 2)(1).contains("Struct")) // in case i the column names come with the word Struct embedded in it
   // println("totalNonStructCols")
   // totalNonStructCols.foreach(println)
    for (totalNonStructCol <- totalNonStructCols) {
      totalArrayBuffer += s"${totalNonStructCol.split(",")(0)}"
    }
  //  totalArrayBuffer.foreach(println)
    totalMainArrayBuffer.clear
    flatten_df_Struct(dfTemp, dfTemp) // flattened schema is now in totalMainArrayBuffer
  //  flatten_df_Struct(dfTemp) // flattened schema is now in totalMainArrayBuffer
    totalArrayBuffer = totalArrayBuffer ++ totalMainArrayBuffer // combined non-struct and flattened struct column names
    dfTemp.selectExpr(totalArrayBuffer: _*) // select all columns from dataframe
  }
}
