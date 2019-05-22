package streaming.advanced

import org.apache.spark.sql.SparkSession
import streaming.advanced.serde.Bytes2MessageDeserializer


object VkEventsConsumer {

    def start(topicName: String): Unit = {
        val spark: SparkSession = SparkSession
                .getActiveSession
                .getOrElse(throw new IllegalStateException("There is no active sessions"))

        spark.udf.register(
            "decode_message",
            (bytes: Array[Byte]) => Bytes2MessageDeserializer.deserialize(bytes))

        val df = spark.readStream
                .format("kafka")
                .option("subscribe", topicName)
                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
                .option("startingOffsets", "earliest")
                .load()

        VkEventsProcessor.process(df)
    }
}
