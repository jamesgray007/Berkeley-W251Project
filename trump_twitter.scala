package org.apache.spark.examples.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

/**
  *
  * Run this on your local machine as
  * $SPARK_HOME/bin/spark-submit $SPARK_OPTS twitter_test.scala totalRunTime samplingInterval numberOfHashtags > output.log 2> info.log
  */
object TrumpPopularTags {
  def main(args: Array[String]) {
    if (args.length != 2){
      System.err.println("Provide window length and sampling interval\n")
      System.exit(1)
    }
    val consumerKey = " HIDDEN "
    val consumerSecret = " HIDDEN "
    val accessToken = " HIDDEN "
    val accessTokenSecret = " HIDDEN "
    //    println("%s".format(args.length))

    // set window length and sampling interval of the program, return top n hashtags
    val totalRunTime = args(0).toInt
    val samplingInterval = args(1).toInt
    val n = 5

    // define filter string
    // twitterUtils will return tweets matching the words in the filter
    val filter = Array("donald", "trump")

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TrumpPopularTagsv2")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filter)

    //-------------------------------------------------------------------------------------------------

    // extract data from tweets
    // (hashtags, Place Name, coordinates, text, timestamp)
    val data = stream.flatMap(status => {
      val s = status.getPlace()
      val place = if (s != null){
        Set(status.getPlace.getName)}
      else{ Set()}
      val loc = status.getGeoLocation()
      val location = if (loc != null){
        Set((status.getGeoLocation.getLatitude, status.getGeoLocation.getLongitude))
      }
      else{
        Set()
      }
      status.getText.split(" ").filter(_.startsWith("#")).map(tag => (tag, place, location, Set(status.getText.toLowerCase), Set(status.getCreatedAt)))
    })
    data.persist()



    // Aggregate the data
    val hashtag_count = data.window(Seconds(totalRunTime), Seconds(samplingInterval)).map{case(tags, places, locations, texts, timestamps) => (tags, 1)}
      .reduceByKey(_ + _)
      .map{case(tag, count) => (count, tag)}
      .transform(_.sortByKey(false))

    val Places = data.window(Seconds(totalRunTime), Seconds(samplingInterval)).map{case(tags, places, locations, texts, timestamps) => places}

    val Locations = data.window(Seconds(totalRunTime), Seconds(samplingInterval)).map{case(tags, places, locations, texts, timestamps) => locations}



    // Print/store results
    Places.foreachRDD(rdd =>{
      rdd.saveAsTextFile("Places")
    })
    Locations.foreachRDD(rdd =>{
      rdd.saveAsTextFile("Locations")
    })
    hashtag_count.foreachRDD(rdd =>{
      println("Analyzed %s tweets\n".format(rdd.count()))
      val tops = rdd.take(n)
      tops.foreach{case(count, tag) => println("%s\t%s".format(tag, count))}
    })

    //-------------------------------------------------------------------------------------------------
    ssc.start()
    ssc.awaitTerminationOrTimeout((totalRunTime + samplingInterval) * 2000 )
  }
}
