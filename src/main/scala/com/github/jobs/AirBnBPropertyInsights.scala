package com.github.jobs

import org.apache.spark.sql.{SQLContext, functions}
import org.apache.spark.{SparkConf, SparkContext, sql}

/**
 * This class performs calculations against an AirBnB dataset -
 * to devise statistical insights on different AirBnB
 * listed properties.
 */
object AirBnBPropertyInsights {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark RDD - AirBnB Calculations").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val sqlContext: SQLContext = new SQLContext(sc)
    val sfAirBnbCleanDF = sqlContext.read.parquet("resources/sf-airbnb-clean.parquet")

    calculateAndWritePriceStatsToCSVFile(sfAirBnbCleanDF)
    calculateAvgNoOfBathroomsAndBedrooms(sfAirBnbCleanDF)
    calNoOfPeopleCateredToInLowestPriceHighestRatingProperty(sfAirBnbCleanDF)

    sfAirBnbCleanDF.printSchema()
    sc.stop()
  }

  /**
   * This method calculates the maximum and minimum property prices and
   * the total count number of properties listed in the AirBnB dataset.
   *
   * These results are then written to a file.
   *
   * @param sfAirBnbCleanDF the airbnb dataset as a dataframe.
   */
  def calculateAndWritePriceStatsToCSVFile(sfAirBnbCleanDF: sql.DataFrame) = {
    sfAirBnbCleanDF
      .select(
        functions.min("price").as("min_price"),
        functions.max("price").as("max_price"),
        functions.count("price").as("row_count")
      ).coalesce(1)
      .write
      .option("header", true)
      .mode("overwrite")
      .csv("out/out_2_2.csv")
  }

  /**
   * This method selects all the properties where the price is
   * greater than 500 AND the review scores are greater than 10. For
   * all the properties which match this criteria - the average
   * number of bedrooms and bathrooms for all those properties,
   * is then calculated and written to a file
   *
   * @param sfAirBnbCleanDF the airbnb dataset as a dataframe.
   */
  def calculateAvgNoOfBathroomsAndBedrooms(sfAirBnbCleanDF: sql.DataFrame) = {
    sfAirBnbCleanDF.filter("price > 500 and review_scores_value = 10")
      .select(
        //I have taken 'bathrooms' instead of 'bathrooms_na' (can be reviewed - same for bedrooms).
        //I assumed it stood for bathrooms being 'not applicable'
        functions.avg("bathrooms").as("avg_bathrooms"),
        functions.avg("bedrooms").as("avg_bedrooms")
      ).coalesce(1)
      .write
      .option("header", true)
      .mode("overwrite")
      .csv("out/out_2_3.csv")
  }

  /**
   * This method calculates the number of people accommodated by
   * the house with the lowest price and highest property
   * rating. The result is then written to a file.
   *
   * @param sfAirBnbCleanDF the airbnb dataset as a dataframe.
   */
  def calNoOfPeopleCateredToInLowestPriceHighestRatingProperty(sfAirBnbCleanDF: sql.DataFrame) = {
    sfAirBnbCleanDF.createOrReplaceTempView("air_bnb_stats")
    val weightedReviewsAndPrices = sfAirBnbCleanDF
      .sqlContext
      .sql("select price," +
        "review_scores_rating, " +
        "accommodates, " +
        "RANK() OVER ( ORDER BY price desc) weighted_price, " +
        "RANK() OVER ( ORDER BY review_scores_rating asc) weighted_review_scores_rating " +
        "from air_bnb_stats " +
        "ORDER BY weighted_price")

    weightedReviewsAndPrices.show()


    weightedReviewsAndPrices.createOrReplaceTempView("air_bnb_stats_prices_n_ratings_weighted")
    val noAccommodatedByPropWithLowestPriceAndHighestWeighting =
      weightedReviewsAndPrices
        .sqlContext
        .sql("select weighted_price + weighted_review_scores_rating/2 as weighted_result, " +
          "accommodates " +
          "from air_bnb_stats_prices_n_ratings_weighted")
    noAccommodatedByPropWithLowestPriceAndHighestWeighting.show()


    noAccommodatedByPropWithLowestPriceAndHighestWeighting
      .createOrReplaceTempView("weighted_results_n_accommodates")
    val noOfPeopleForHighestWeightProperty = noAccommodatedByPropWithLowestPriceAndHighestWeighting
      .sqlContext
      .sql("select accommodates from weighted_results_n_accommodates " +
        "where (weighted_result) in (select max(weighted_result) from weighted_results_n_accommodates)")
    noOfPeopleForHighestWeightProperty.show()


    noOfPeopleForHighestWeightProperty.coalesce(1)
      .write
      .option("header", false)
      .mode("overwrite")
      .csv("out/out_2_4.csv")
  }
}
