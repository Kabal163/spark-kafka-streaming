package streaming.advanced

import org.apache.spark.sql.SparkSession

object App {

    def main(args: Array[String]): Unit = {
        val topicName = args(0)

        SparkSession.builder()
                .appName("streaming-advanced")
                .getOrCreate()

        VkEventsConsumer.start(topicName)
    }
}
