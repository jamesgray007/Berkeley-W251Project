//package org.apache.spark.examples.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

import java.util.Calendar
import scala.util.Random
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder

/**
  *
  * Run this on your local machine as
  * $SPARK_HOME/bin/spark-submit $SPARK_OPTS twitter_test.scala totalRunTime samplingInterval numberOfHashtags > output.log 2> info.log
  */
object TrumpPopularTags {
  def main(args: Array[String]) {

    val consumerKey = "hidden"
    val consumerSecret = "hidden"
    val accessToken = "40264066-hidden"
    val accessTokenSecret = "hidden"
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

    val outputBaseDir = "hdfs://spark1:8020/output/"

    // define filter string
    // twitterUtils will return tweets matching the words in the filter
    //    val filter = Array("donald", "trump")
    val filter = args.takeRight(args.length - 6)
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TrumpPopularTagsv2")
    val ssc = new StreamingContext(sparkConf, Seconds(batchSize))
    val stream = TwitterUtils.createStream(ssc, None, filter)

    val api_key = "hidden"
    //    val base_url = "http://access.alchemyapi.com/calls/text/"
    //val base_url = "http://gateway-a.watsonplatform.net/calls/text/"
    //val r = scala.util.Random
    //-------------------------------------------------------------------------------------------------
    // Alchemyapi sentiment analysis
    def getSentiment(text: String, target: String = ""): String = {
      val base_url = "http://gateway-a.watsonplatform.net/calls/text/"
      val url = if (target == ""){
        val call_url = base_url + "TextGetTextSentiment"
        call_url + "?apikey=" + api_key + "&text=" + URLEncoder.encode(text, "utf-8") + "&showSourceText=1"
      }
      else {
        val call_url = base_url + "TextGetTargetedSentiment"
        call_url + "?apikey=" + api_key + "&text=" + URLEncoder.encode(text, "utf-8") + "&showSourceText=1" + "&targets=" + target
      }
      //      println("\n%s\n".format(url))
      val result = scala.io.Source.fromURL(url).mkString
      //     println(result)
      if (result contains "<score>"){
        val scoreBegin = result indexOf "<score>"
        val scoreEnd = result indexOf "</score>"
        return result.substring(scoreBegin + 7, scoreEnd)
      }
      else{
        return "0.0"
      }
    }

    def getRandomSentiment(text: String, target: String = "None"): String = {
      val r = scala.util.Random
      val score = 1 - 2 * r.nextFloat
      score.toString
    }


    //-------------------------------------------------------------------------------------------------
    // extract data from tweets
    // (hashtags, Place Name, coordinates, text, timestamp)
    val data = stream.flatMap(status => {
      val s = status.getPlace()
      val place = if (s != null){
        status.getPlace.getFullName}
      else{ "None"}
      val loc = status.getGeoLocation()
      val location = if (loc != null){
        (status.getGeoLocation.getLatitude, status.getGeoLocation.getLongitude)
      }
      else{
        (360.0, 360.0)
      }
      val text = status.getText
      status.getText.split(" ").filter(_.startsWith("#")).map(tag => (tag, place, location, status.getText.toLowerCase, Set(status.getCreatedAt)))
    })
    data.persist()



    // Aggregate the data
    val hashtag_count = data.window(Seconds(windowDuration), Seconds(slidingWindow)).map{case(tags, places, locations, texts, timestamps) => (tags, 1)}
      .reduceByKey(_ + _)
      .map{case(tag, count) => (count, tag)}
      .transform(_.sortByKey(false))

    val Places = data.window(Seconds(windowDuration), Seconds(slidingWindow)).map{case(tags, places, locations, texts, timestamps) => (places, (texts, 1))}
      .filter{case(place, (text, count)) => place != "None"}
      .map{case(place, (text, count)) => (place, (count, getSentiment(text).toDouble, getSentiment("trump").toDouble))}
      .combineByKey(x => x,
        (combiner1: (Int, Double, Double), textcount: (Int, Double, Double)) => (combiner1._1 + textcount._1, combiner1._2 + textcount._2, combiner1._3 + textcount._3),
        (combiner1: (Int, Double, Double), combiner2: (Int, Double, Double)) => (combiner1._1 + combiner2._1, combiner1._2 + combiner2._2, combiner1._3 + combiner2._3),
        new org.apache.spark.HashPartitioner(6)
    )
      .map{case(place, (count, sent, tsent)) => (count, (place, sent, tsent))}.transform(_.sortByKey(false))

    val lcations = data.window(Seconds(windowDuration), Seconds(slidingWindow)).map{case(tags, places, locations, texts, timestamps) => (locations, 1)}
      .reduceByKey(_ + _).map{case(location, count) => (count, location)}.transform(_.sortByKey(false))

    val tweets = data.window(Seconds(windowDuration), Seconds(slidingWindow))
      .map{case(tags, places, locations, texts, timestamps) => (texts, 1)}
      .reduceByKey(_ + _)
      .map{case(text, count) => (count, text)}
      .transform(_.sortByKey(false))

    val distScript = "alchemyplot.py"
    //tweets.foreachRDD(_.pipe(Seq(SparkFiles.get(distScript))))
    //val tp = tweets.transform(_.pipe("./alchemyplot.py"))
    //println(tp.collect().toList)


    // save output to output directories
    // the argument in the following function is prefix for the output directory
    // the directory name for output is `prefix-timestamp`. timestamp is in milliseconds
    // the output file names start with 'part-'
    tweets.map{case(count, text) => "%s\t%s\t%s\t%s ".format(text.replaceAll("\n", " "), getSentiment(text), getSentiment(text, "trump"), count)}.saveAsTextFiles(outputBaseDir + "tweets/t")
    lcations.map{case(count, location) => "%s\t%s".format(location, count)}.saveAsTextFiles(outputBaseDir + "locations/l")
    Places.map{case(count, (place, sent, tsent)) => "%s\t%s\t%s\t%s".format(place, sent/count, tsent/count, count)}.saveAsTextFiles(outputBaseDir + "places/p")
    hashtag_count.map{case(count, tag) => "%s\t%s".format(tag, count)}.saveAsTextFiles(outputBaseDir + "hashtags/h")

    /*   Write out each output to stdout
    tweets.foreachRDD(rdd =>{
      val u = rdd.take(nTweets)
      println(Calendar.getInstance.getTime)
      println("Tweet Sentiments for top %s tweets (total %s)".format(u.length, rdd.count()))

      u.foreach{case(count, (tweet, score, target_score)) => {
        val text = tweet.toLowerCase.replaceAll("[^a-zA-Z0-9]", " ")
        println("%s\t%s\t%s\t%s".format(tweet.replaceAll("\n", " "), count, score, target_score))
      }}
    })

    Places.foreachRDD(rdd =>{
      val p = rdd.take(nPlaces)
      println("top %s places the tweets belonged to (total %s)".format(p.length,rdd.count()))
      p.foreach{case(count, place) => println("%s\t%s".format(place,count))}
      //p.foreach(cp => println(cp))
    })

    lcations.foreachRDD(rdd=>{
      val l = rdd.take(200)
      println("top %s locations (total %s)".format(l.length, rdd.count()))
      l.foreach{case(count, location) => println("%s\t%s".format(location, count))}
    })


    hashtag_count.foreachRDD(rdd =>{
      val tops = rdd.take(nTags)
      println("top %s hashtags (total %s)".format(tops.length, rdd.count()))
      tops.foreach{case(count, tag) => println("%s\t%s".format(tag, count))}
    })
 */
    //-------------------------------------------------------------------------------------------------
    ssc.start()
    ssc.awaitTerminationOrTimeout((windowDuration + slidingWindow) * 3000 )
    //    ssc.awaitTerminationOrTimeout((slidingWindow)*2000)
    //    ssc.awaitTermination()
    ssc.stop()
  }
}
