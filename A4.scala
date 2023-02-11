package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}



object A4 {
  // Function that generates co-occurance pairs of words that appear in the same line
  def pairGen(wordArray: List[String]): List[(String, String)] = {
    val pairs = for (x <- wordArray; y <- wordArray if x != y) yield (x, y)
    return pairs
  }
  // Function used to update co-occurance frequencies
  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = runningCount.getOrElse(0) + newValues.sum
    Some(newCount)

  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: TaskA <directory>")
      System.exit(1)
    }


    val sparkConf = new SparkConf().setAppName("HdfsTaskA").setMaster("local")

    // Create the context- the batch interval is 5 seconds)
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("./RddCheckPoint/")

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))

    //TASK A - Word counts

    val wordsnospec = words.map(_.replaceAll("[^a-zA-Z]+","")) // Remove all non letter characters
    val re = """^[a-zA-Z]+$""".r
    val letterwords = wordsnospec.flatMap(re.findAllIn(_)) //Find all words that match the regex expression
    val wordCounts = letterwords.map(x => (x, 1)).reduceByKey(_ + _) // map words to value of 1 and same all pairs with the same key

    //TASK B - Co-occurance counts

    val Cooccurance = lines.map(_.split(" ").map(_.replaceAll("[^a-zA-Z]+","")).flatMap(re.findAllIn(_))
      .filter(word => word.length >= 5).toList) // convert records to List[String] which contains strings that have a length greater than 5 with special characters removed
      .flatMap(x => pairGen(x)) // take all combinations of two words (with no enforced ordering)
      .map(x => (x, 1)) // prepare for reducing - starting with 1 for each combination
      .reduceByKey(_ + _) //Combine pairs to get count

    //TASK C - Update Co-occurance counts

    val Cooccuranceupdated = Cooccurance.updateStateByKey(updateFunction) //Update co-occurance counts from previous checkpoint



    //Print values on terminal
    wordCounts.print()
    Cooccurance.print()
    Cooccuranceupdated.print()

    //OUTPUTS

      var counter = 0
      wordCounts.foreachRDD(rdd => {
        if (!rdd.isEmpty) {
          counter += 1
          rdd.saveAsTextFile("hdfs:///outputA4/TaskA-00".concat(counter.toString))

        }
      })
    Cooccurance.foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        rdd.saveAsTextFile("hdfs:///outputA4/TaskB-00".concat(counter.toString))

      }
    })
    Cooccuranceupdated.foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        rdd.saveAsTextFile("hdfs:///outputA4/TaskC")

      }
    })




      ssc.start()
      ssc.awaitTermination()
    }
}
