package com.newyorker.common

case class Business (
                      address: Option[String],
                      attributes: Option[String],
                      business_id: Option[String],
                      categories: Option[String],
                      city: Option[String],
                      hours: Option[String],
                      is_open: Option[Long],
                      latitude: Option[Double],
                      longitude: Option[Double],
                      name: Option[String],
                      postal_code: Option[String],
                      review_count: Option[Long],
                      stars: Option[Double],
                      state: Option[String]
                    )
