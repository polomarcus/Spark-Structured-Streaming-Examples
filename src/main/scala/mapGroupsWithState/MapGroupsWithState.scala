package mapGroupsWithState

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{struct, to_json, _}
import org.apache.spark.sql.types.StringType
import spark.SparkHelper
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import radio.SimpleSongAggregation

object MapGroupsWithState {
  private val spark = SparkHelper.getSparkSession()

  import spark.implicits._

  /*
  def updateAcrossEvents(user:String,
                         inputs: Iterator[InputRow],
                         oldState: GroupState[UserState]):UserState = {
    var state:UserState = if (oldState.exists) oldState.get else UserState(user,
      "",
      new java.sql.Timestamp(6284160000000L),
      new java.sql.Timestamp(6284160L)
    )
    // we simply specify an old date that we can compare against and
    // immediately update based on the values in our data

    for (input <- inputs) {
      state = updateUserStateWithEvent(state, input)
      oldState.update(state)
    }
    state
  }*/

  /*def write(ds: Dataset[SimpleSongAggregation] ) = {
    ds
      .groupByKey(_.artist)
         .mapGroupsWithState()
      .writeStream
      .outputMode("complete")
      .format("console")
      .queryName("Kafka - Count number of broadcasts for a title/artist by radio")
      .option("topic", "test")
      .start()
  }*/
}
