package streaming.advanced

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.advanced.entity.StreamingResult

object ProcessedVkEventsProducer {

    def toCSV(df: DataFrame): Unit = {
        df.writeStream
                .format("csv")
                .option("header", "true")
                .option("format", "append")
                .option("path", "/user/raj_ops/streaming_result.csv")
                .option("checkpointLocation", "/tmp/raj_ops/spark/stream_result")
                .outputMode("append")
                .start()
                .awaitTermination()
    }

    def toConsole(df: DataFrame): Unit = {
        df.writeStream
                .format("console")
                .option("truncate", "false")
                .outputMode("append")
                .start()
                .awaitTermination()
    }

    def toKafka(df: DataFrame): Unit = {
        val spark: SparkSession = SparkSession
                .getActiveSession
                .getOrElse(throw new IllegalStateException("There is no active sessions"))
        import spark.implicits._

        spark.udf.register(
            "to_kafka_value",
            (startTime: BigInt, endTime: BigInt, hashtag: String, count: Int) =>
                    StreamingResult(startTime, endTime, hashtag, count)
        )

        df.select(unix_timestamp($"window.start").as("startTime"),
                  unix_timestamp($"window.end").as("endTime"),
                  $"hashtag",
                  $"count(hashtag)")
                .selectExpr("""to_kafka_value(startTime, endTime, hashtag, count(hashtag)) as value""")
                .writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
                .option("topic", "streaming-result")
                .option("checkpointLocation", "/tmp/raj_ops/spark/stream_result")
                .start()
                .awaitTermination()
    }
}
