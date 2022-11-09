package pTransforms;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import models.Player;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import utils.GsonUTCDateAdapter;

import java.util.Date;

public class JsonToPlayerPipeline extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<Player>> {

  static class JsonToPlayer extends DoFn<String, Player> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      // Use OutputReceiver.output to emit the output element.
      Gson gson = new GsonBuilder().registerTypeAdapter(Date.class, new GsonUTCDateAdapter()).create();
      System.out.println("received JSON object: " + c.element());
      Player player = gson.fromJson(c.element(), new TypeToken<Player>(){}.getType());

      c.outputWithTimestamp(player, new Instant());
    }
  }

  @Override
  public PCollection<Player> expand(PCollection<FileIO.ReadableFile> input) {

    // Number files read in parallel
    PCollection<Player> players = input.apply("FileReadConcurrency",
        Reshuffle.<FileIO.ReadableFile>viaRandomKey().withNumBuckets(1))
        .apply("ReadFiles", TextIO.readFiles())
        // Because we split each line to a single event we cen get a high fan-out.
        .apply("ReshuffleRecords", Reshuffle.viaRandomKey())
        .apply("Parse Json To Player", ParDo.of(new JsonToPlayer()));

    return players;
  }
}
