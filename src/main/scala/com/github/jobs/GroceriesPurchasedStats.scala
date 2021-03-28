package com.github.jobs

import com.github.io.FlatFileWriter
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This class performs an analysis against a groceries dataset.
 *
 */

object GroceriesPurchasedStats {
  val Comma = ","
  val NewLine = "\n"
  val One = 1
  val GitHubGroceries = "https://raw.githubusercontent" +
    ".com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv"


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark RDD - Groceries Calculations").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val groceriesTransactionsList = scala.io.Source.fromURL(GitHubGroceries).mkString.split(NewLine).toList
    val groceriesTransactionsListRDD = sc.parallelize(groceriesTransactionsList)

    val distinctProducts = groceriesTransactionsListRDD.flatMap(list => list.split(Comma)).distinct()
    val wordMapWithCounts = groceriesTransactionsListRDD.flatMap(list => list.split(Comma))
      .map(word => (word, One)).reduceByKey(_ + _).sortBy(_._2).collect().takeRight(5).sortBy(-_._2)

    findDistinctProductsInListAndWriteToFile(distinctProducts.collect())
    countDistinctProductsInListAndWriteToFile(distinctProducts.collect())
    writeTopFivePurchasedProductsToFile(wordMapWithCounts)

    sc.stop()
  }

  /**
   * This method takes a list of products and writes them
   * a file.
   *
   * @param distinctProducts a list of all the distinct products, amongst those purchased
   */
  def findDistinctProductsInListAndWriteToFile(distinctProducts: Array[String]) = {
    val flatFileWriter = new FlatFileWriter("out/out_1_2a.txt")

    flatFileWriter.writeListToFile(distinctProducts)
  }

  /**
   * Counts the list of unique products, throughout all
   * the grocery transactions and writes the count to a file
   *
   * @param distinctProducts a list of all the distinct products
   */
  def countDistinctProductsInListAndWriteToFile(distinctProducts: Array[String]) = {
    val numberOfProducts = distinctProducts.size;
    val flatFileWriter = new FlatFileWriter("out/out_1_2b.txt")

    flatFileWriter.writeValueToFile(numberOfProducts.toString);
  }

  /**
   * This calculates the frequency of purchases
   * for the top 5 sold items and writes it to a
   * file
   *
   * @param wordMapWithCounts a list with the frequency of sales for the top 5 sold products.
   */
  def writeTopFivePurchasedProductsToFile(wordMapWithCounts: Array[(String, Int)]) {
    val flatFileWriter = new FlatFileWriter("out/out_1_3.txt")
    flatFileWriter.writeMapToFile(wordMapWithCounts)
  }
}
