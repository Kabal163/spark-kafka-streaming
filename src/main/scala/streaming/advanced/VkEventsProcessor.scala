package streaming.advanced

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.advanced.serde.Bytes2MessageDeserializer

object VkEventsProcessor {

    def process(df: DataFrame): Unit ={
        val spark: SparkSession = SparkSession
                .getActiveSession
                .getOrElse(throw new IllegalStateException("There is no active sessions"))
        import spark.implicits._

        //we need to decode kafka's message value to our message
        spark.udf.register(
            "decode_message",
            (bytes: Array[Byte]) => Bytes2MessageDeserializer.deserialize(bytes))

        val df1 = df.selectExpr("""decode_message(value) as message""")
                        .select(
                            $"message.creationTime".cast(DataTypes.TimestampType).as("timestamp"),
                            explode($"message.hashtags").alias("hashtag"))
                        .withColumn(
                            "hashtag",
                            lower(
                                regexp_replace($"hashtag", " ", "")
                            ).alias("hashtag"))
                        .withWatermark("timestamp", "20 second")
                        .where($"hashtag".isNotNull)
                        .groupBy(
                            window($"timestamp", "1 minute"),
                            $"hashtag")
                        .agg(count($"hashtag"))
                        .coalesce(1)

        ProcessedVkEventsProducer.toKafka(df1)
    }
}
