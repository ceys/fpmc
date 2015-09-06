package test.scala.com.jd.bdp.fpmc.produce.offline

import scopt.OptionParser
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by zhengchen on 2015/8/26.
 */
object TestJDQ {

  case class Params(
                     kafkaZK: String = null,
                     kafkaGroup: String = null,
                     topics: String = null,
                     output: String = null,
                     interval: Int = 100,
                     numThreads: Int = 5,
                     ABRatio: Double = 0.2,
                     predictorHost: String = null,
                     predictorPort: Int = 8100)

  def main(args: Array[String]) {

    val defaultParams = Params()
    val parser = new OptionParser[Params]("Personalized_Blend") {
      opt[String]("kafkaZK")
        .action((x, c) => c.copy(kafkaZK = x))
      opt[String]("kafkaGroup")
        .action((x, c) => c.copy(kafkaGroup = x))
      opt[String]("topics")
        .action((x, c) => c.copy(topics = x))
      opt[String]("output")
        .action((x, c) => c.copy(output = x))
      opt[Int]("interval")
        .action((x, c) => c.copy(interval = x))
      opt[Int]("numThreads")
        .action((x, c) => c.copy(numThreads = x))
      opt[Double]("ABRatio")
        .action((x, c) => c.copy(ABRatio = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {
    val sparkConf = new SparkConf()
      .setAppName("Personalized_Blend")
      .set("spark.streaming.blockInterval", "1000")
    val ssc = new StreamingContext(sparkConf, Seconds(params.interval))

    val topicMap = params.topics.split(",").map((_, params.numThreads)).toMap
    val kafkaStream = KafkaUtils.createStream(ssc, params.kafkaZK, params.kafkaGroup, topicMap).map(_._2)

    val result = kafkaStream.map { line =>
      val begin = line.indexOf('@') + 1
      val reqRec = line.substring(begin).parseJson

    }

  }
}
