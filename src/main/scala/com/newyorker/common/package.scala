package com.newyorker

import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

package object common {

  val checkinSchema = StructType(Array(
    StructField("business_id", StringType, true)
    , StructField("date", StringType, true)
  ))

  val businessSchema = StructType(Array(
    StructField("address",StringType,true)
     , StructField("attributes",
      StructType(Array(StructField("AcceptsInsurance",StringType,true), StructField("AgesAllowed",StringType,true)
        , StructField("Alcohol",StringType,true), StructField("Ambience",StringType,true)
        , StructField("BYOB",StringType,true), StructField("BYOBCorkage",StringType,true)
        , StructField("BestNights",StringType,true), StructField("BikeParking",StringType,true)
        , StructField("BusinessAcceptsBitcoin",StringType,true), StructField("BusinessAcceptsCreditCards",StringType,true)
        , StructField("BusinessParking",StringType,true), StructField("ByAppointmentOnly",StringType,true)
        , StructField("Caters",StringType,true), StructField("CoatCheck",StringType,true), StructField("Corkage",StringType,true)
        , StructField("DietaryRestrictions",StringType,true), StructField("DogsAllowed",StringType,true)
        , StructField("DriveThru",StringType,true), StructField("GoodForDancing",StringType,true), StructField("GoodForKids",StringType,true)
        , StructField("GoodForMeal",StringType,true), StructField("HairSpecializesIn",StringType,true)
        , StructField("HappyHour",StringType,true), StructField("HasTV",StringType,true), StructField("Music",StringType,true)
        , StructField("NoiseLevel",StringType,true), StructField("Open24Hours",StringType,true)
        , StructField("OutdoorSeating",StringType,true), StructField("RestaurantsAttire",StringType,true)
        , StructField("RestaurantsCounterService",StringType,true), StructField("RestaurantsDelivery",StringType,true)
        , StructField("RestaurantsGoodForGroups",StringType,true), StructField("RestaurantsPriceRange2",StringType,true)
        , StructField("RestaurantsReservations",StringType,true), StructField("RestaurantsTableService",StringType,true)
        , StructField("RestaurantsTakeOut",StringType,true), StructField("Smoking",StringType,true)
        , StructField("WheelchairAccessible",StringType,true), StructField("WiFi",StringType,true))),true)
      ,StructField("business_id",StringType,true)
      ,StructField("categories",StringType,true)
      ,StructField("city",StringType,true)
      ,StructField("hours",StructType(Array(StructField("Friday",StringType,true), StructField("Monday",StringType,true)
      , StructField("Saturday",StringType,true), StructField("Sunday",StringType,true), StructField("Thursday",StringType,true)
      , StructField("Tuesday",StringType,true), StructField("Wednesday",StringType,true))),true)
      ,StructField("is_open",LongType,true)
      ,StructField("latitude",DoubleType,true)
      ,StructField("longitude",DoubleType,true)
      ,StructField("name",StringType,true)
      ,StructField("postal_code",StringType,true)
      ,StructField("review_count",LongType,true)
      ,StructField("stars",DoubleType,true)
      ,StructField("state",StringType,true)
  ))
}
