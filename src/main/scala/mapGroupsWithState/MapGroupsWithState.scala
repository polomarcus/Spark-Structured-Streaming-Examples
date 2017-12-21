package mapGroupsWithState

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{struct, to_json, _}
import org.apache.spark.sql.types.StringType
import spark.SparkHelper
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import radio.{ArtistAggregationState, SimpleSongAggregation, SimpleSongAggregationKafka}

object MapGroupsWithState {
  private val spark = SparkHelper.getSparkSession()

  import spark.implicits._


  def updateArtistStateWithEvent(state: ArtistAggregationState, artistCount : SimpleSongAggregation) = {
    println("MapGroupsWithState - updateArtistStateWithEvent")
    if(state.artist == artistCount.artist) {
      ArtistAggregationState(state.artist, state.count + artistCount.count)
    } else {
      state
    }
  }

  def updateAcrossEvents(artist:String,
                         inputs: Iterator[SimpleSongAggregation],
                         oldState: GroupState[ArtistAggregationState]): ArtistAggregationState = {

    var state: ArtistAggregationState = if (oldState.exists)
      oldState.get
    else
      ArtistAggregationState(artist, 1L)

    // for every rows, let's count by artist the number of broadcast, instead of counting by artist, title and radio
    for (input <- inputs) {
      state = updateArtistStateWithEvent(state, input)
      oldState.update(state)
    }

    state
  }


  /**
    *
    * @return
    *
    * Batch: 4
      -------------------------------------------
      +------+-----+
      |artist|count|
      +------+-----+
      | Drake| 4635|
      +------+-----+

      Batch: 5
      -------------------------------------------
      +------+-----+
      |artist|count|
      +------+-----+
      | Drake| 4710|
      +------+-----+
    */
  def write(ds: Dataset[SimpleSongAggregationKafka] ) = {
    ds.select($"radioCount.title", $"radioCount.artist", $"radioCount.radio", $"radioCount.count")
      .as[SimpleSongAggregation]
      .groupByKey(_.artist)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents) //we can control what should be done with the state when no update is received after a timeout.
      .writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .queryName("mapGroupsWithState - counting artist broadcast")
      .start()
  }
}
