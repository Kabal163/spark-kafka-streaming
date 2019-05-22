package streaming.advanced

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.advanced.entity.StreamingResult
import streaming.advanced.serde.StreamingResult2ByteSerializer

object ProcessedVkEventsProducer {
    val spark: SparkSession = SparkSession
            .getActiveSession
            .getOrElse(throw new IllegalStateException("There is no active sessions"))

    val to_kafka_value: (String, String, String, Int) =>
            Array[Byte] = (startTime: String,
                            endTime: String,
                            hashtag: String,
                            count: Int) =>
        StreamingResult2ByteSerializer.serialize(
            StreamingResult(
                startTime,
                endTime,
                hashtag,
                count))

    val to_kafka_value_UDF: UserDefinedFunction = udf(to_kafka_value)


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
        import spark.implicits._

        df.withColumn("startTime",  $"window.start".cast(DataTypes.StringType))
                .withColumn("endTime", $"window.end".cast(DataTypes.StringType))
                .withColumn("value", to_kafka_value_UDF($"startTime", $"endTime", $"hashtag", $"count(hashtag)"))
                .drop("startTime", "endTime", "hashtag", "window", "count(hashtag)")
                .writeStream
                .format("console")
                .option("truncate", "false")
                .outputMode("append")
                .start()
                .awaitTermination()
    }

    def toKafka(df: DataFrame): Unit = {
        import spark.implicits._

        df.withColumn("startTime",  $"window.start".cast(DataTypes.StringType))
                .withColumn("endTime", $"window.end".cast(DataTypes.StringType))
                .withColumn("value", to_kafka_value_UDF($"startTime", $"endTime", $"hashtag", $"count(hashtag)"))
                .drop("startTime", "endTime", "hashtag", "window", "count(hashtag)")
                .writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
                .option("topic", "streaming-result")
                .option("checkpointLocation", "/tmp/raj_ops/spark/stream_result")
                .start()
                .awaitTermination()
    }
}
