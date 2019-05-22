package streaming.advanced

import org.apache.spark.sql.SparkSession


object VkEventsConsumer {

    def start(topicName: String): Unit = {
        val spark: SparkSession = SparkSession
                .getActiveSession
                .getOrElse(throw new IllegalStateException("There is no active sessions"))

        val df = spark.readStream
                .format("kafka")
                .option("subscribe", topicName)
                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
                .load()

        VkEventsProcessor.process(df)
    }
}
