//package org.apache.spark.examples.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import java.util.Calendar
import scala.util.Random
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder

/**
  *  Do not run this script by itself
  *  Instead use run_spark.sh for this purpose
  */
object TrumpPopularTags {
  def main(args: Array[String]) {

    val consumerKey = ""
    val consumerSecret = ""
    val accessToken = ""
    val accessTokenSecret = ""
    //    println("%s".format(args.length))

    // set window length and sampling interval of the program, return top n hashtags
    // totalRunTime is actually the window duration. The tweets are processed for this duration
    // samplingInterval is sliding window duration. The results are updated every samplingInterval seconds
    val batchSize = args(0).toInt
    val windowDuration = args(1).toInt
    val slidingWindow = args(2).toInt
    val nTags = args(3).toInt
    val nTweets = args(4).toInt
    val nPlaces = args(5).toInt

    // for the following outputBaseDir to work, run as user 'hadoop' if saving on hdfs
    // CHANGE the output directory
    val outputBaseDir = "hdfs://dhaval01:8020/final/output/"


    // define filter string
    // twitterUtils will return tweets matching the words in the filter
    val filter = args.takeRight(args.length - 6)
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


    val sparkConf = new SparkConf().setAppName("Trumpets")
    
    // read static stopwords file before starting streaming context
    val sc = new SparkContext(sparkConf)
    val stopwords = sc.textFile("stopwords.txt").filter(line => !line.isEmpty).flatMap(line => line.toLowerCase.split("\\s+")).collect().toSet ++ filter.toSet

    // start streaming context from SparkContext
    // CHANGE checkpoint directory
    val ssc = new StreamingContext(sc, Seconds(batchSize))
    ssc.checkpoint("hdfs://spark1:8020/final/.checkpoint/")
    val stream = TwitterUtils.createStream(ssc, None, filter)


    
    // CHANGE api key
    val api_key = ""
    //    val base_url = "http://access.alchemyapi.com/calls/text/"
    val base_url = "http://gateway-a.watsonplatform.net/calls/text/"
    //val r = scala.util.Random

    //-------------------------------------------------------------------------------------------------
    // Alchemyapi sentiment analysis
    def getSentiment(text: String, target: String = ""): String = {
      val url = if (target == ""){
        val call_url = base_url + "TextGetTextSentiment"
        call_url + "?apikey=" + api_key + "&text=" + URLEncoder.encode(text, "utf-8") + "&showSourceText=1"
      }
      else {
        val call_url = base_url + "TextGetTargetedSentiment"
        call_url + "?apikey=" + api_key + "&text=" + URLEncoder.encode(text, "utf-8") + "&showSourceText=1" + "&targets=" + target
      }
      val result = scala.io.Source.fromURL(url).mkString
      if (result contains "<score>"){
        val scoreBegin = result indexOf "<score>"
        val scoreEnd = result indexOf "</score>"
        return result.substring(scoreBegin + 7, scoreEnd)
      }
      else{
        return "0.0"
      }
    }

    // dummy drandom sentiment maker. return a uniformly random output betwee [-1, 1]
    def getRandomSentiment(text: String, target: String = "None"): String = {
      val r = scala.util.Random
      val score = 1 - 2 * r.nextFloat
      score.toString
    }

    //filter text to find keywords
    def getWords(tweet: String): Array[String] = {
      tweet.toLowerCase.replaceAll("[^0-9a-z#@]", " ").split("\\s+")
        .filter(word => !(word.startsWith("#") || word.startsWith("http") || word.startsWith("@") || stopwords.contains(word)))
        .filter(_.length > 1)
    }

    //-------------------------------------------------------------------------------------------------
    // extract data from tweets
    // (hashtags, Place Name, text)
    val data = stream.flatMap(status => {
      val s = status.getPlace()
      val place = if (s != null){
        s.getFullName + s.getCountryCode()}
      else{ "None"}
      val text = status.getText
      status.getText.split(" ").filter(_.startsWith("#")).map(tag => (tag, place, status.getText.toLowerCase))
    })
    data.persist()



    // Aggregate the data

    //1. keywords
    val keywords = data.window(Seconds(windowDuration), Seconds(slidingWindow)).map{case(tags, places, texts) => texts}.flatMap(getWords(_))
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .map{case(word, count) => (count, word)}
      .transform(_.sortByKey(false))


    //2. Places
    val Places = data.window(Seconds(windowDuration), Seconds(slidingWindow)).map{case(tags, places, texts) => (places, 1)}
      .reduceByKey(_ + _)
      .map{case(place, count) => (count, place)}
      .transform(_.sortByKey(false))

    //3. Tweets
    val tweets = data.window(Seconds(windowDuration), Seconds(slidingWindow))
      .map{case(tags, places, texts) => (texts, 1)}
      .reduceByKey(_ + _)
      .map{case(text, count) => (count, text)}
      .transform(_.sortByKey(false))

    //4. Hashtags
    val hashtag_count = data.window(Seconds(windowDuration), Seconds(slidingWindow)).map{case(tags, places, texts) => (tags, 1)}
      .reduceByKey(_ + _)
      .map{case(tag, count) => (count, tag)}
      .transform(_.sortByKey(false))

    
    // save output to output directories
    // the argument in the following function is prefix for the output directory
    // the directory name for output is `prefix-timestamp`. timestamp is in milliseconds
    // the output file names start with "part-"
    /* */
    keywords.map{case(count, word) => "%s\t%s".format(word, count)}.saveAsTextFiles(outputBaseDir + "keywords/k")

    tweets.map{case(count, text) => "%s\t%s ".format(text.replaceAll("\n", " "), count)}.saveAsTextFiles(outputBaseDir + "tweets/t")

    Places.map{case(count, place) => "%s\t%s".format(place, count)}.saveAsTextFiles(outputBaseDir + "places/p")

    hashtag_count.map{case(count, tag) => "%s\t%s".format(tag, count)}.saveAsTextFiles(outputBaseDir + "hashtags/h")
    /*
     println(stopwords)
     */

    // Write out the count of unique elements in each aggregation
    // Also collect and post sentiment for top 40 tweets
    hashtag_count.foreachRDD(rdd => {
      println("\n")
      println(Calendar.getInstance.getTime())
      println("hashtags\t%s".format(rdd.count()))
    })
    Places.foreachRDD(rdd => {
      println("places\t%s".format(rdd.count()))
    })
    keywords.foreachRDD(rdd => {
      println("keywords\t%s".format(rdd.count()))
    })

    tweets.foreachRDD(rdd => {
      println("tweets\t%s".format(rdd.count()))
      val tees = rdd.take(40)
      tees.foreach{case(count, tweet) => {
        println("%s\t%s\t%s".format(tweet, count, getSentiment(tweet)))
      }}
      println("\n")
    })

    //-------------------------------------------------------------------------------------------------
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
