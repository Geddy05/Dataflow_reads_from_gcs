import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import models.Player;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import pTransforms.JsonToPlayerPipeline;
import pTransforms.PollingGCSPipeline;


import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class DataPipeLineToFirestore {
  static class PlayerToTableRow extends DoFn<Player, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      // Use OutputReceiver.output to emit the output element.
      Player player = c.element();
      System.out.println("Process player with ID: " + player.userId);
      TableRow row = new TableRow();
      row.set("username", player.username);
      row.set("userId", player.userId);
      row.set("teamId", player.teamId);
      row.set("points", player.points);
      row.set("timestamp", player.timestamp);

      c.output(row);
    }
  }


  private static final class CreateDeleteOperation extends DoFn<Player, Write> {
    @ProcessElement
    public void processElement(ProcessContext c) throws IllegalAccessException {
      Player player = c.element();

      Map<String, Value> map = new HashMap<>();

      Value.newBuilder().setStringValue(player.username).build();
      map.put("username", Value.newBuilder().setStringValue(player.username).build());
      map.put("points", Value.newBuilder().setIntegerValue(player.points).build());
      map.put("teamId", Value.newBuilder().setIntegerValue(player.teamId).build());
      map.put("timestamp", Value.newBuilder().setStringValue(player.timestamp.toString()).build());
      map.put("userId", Value.newBuilder().setIntegerValue(player.userId).build());


      Document.Builder builder = Document
          .newBuilder()
          .setName("projects/geddy-dataflow-playground/databases/(default)/documents/score/" + player.teamId)
          .putAllFields(map);
      c.output(Write.newBuilder().setUpdate(builder.build()).build());
    }
  }

  private static final class PlayerToKV extends DoFn<Player, KV<Integer, Player>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      Player player = c.element();
      c.output(KV.of(player.teamId, player));
    }
  }

  private static final class CreateTeamBoard extends DoFn<KV<Integer, Iterable<Player>>, Player> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<Integer, Iterable<Player>> team = c.element();
      Integer teamId = team.getKey();
      int teamScore = 0;

      for (Player player: team.getValue()) {
        teamScore += player.points;
      }

      Player teamObject = new Player();
      teamObject.username = "team";
      teamObject.userId = 0;
      teamObject.teamId = teamId;
      teamObject.points = teamScore;
      teamObject.timestamp = new Date();
      c.output(teamObject);
    }
  }

  public void buildPipeline(GCSPipelineOptions options){
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Player>playerScore = pipeline
        // rfcStartDateTime: Only read files with an updated timestamp greater than the rfcStartDateTime.
        .apply("Read files from Cloud Storage",
            new PollingGCSPipeline(options.getInput(),null))
        // File content to Player objects
        .apply("File to Players", new JsonToPlayerPipeline())
        .apply("Window" ,Window.<Player>into(FixedWindows.of(Duration.standardSeconds(10)))
            .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(10))))
            .withAllowedLateness(Duration.standardSeconds(20))
            .discardingFiredPanes());

    PCollection<KV<Integer, Iterable<Player>>> teams = playerScore.apply("lala" , ParDo.of(new PlayerToKV()))
            .apply(GroupByKey.create());

    PCollection<Player> scores = teams.apply("Create board",ParDo.of(new CreateTeamBoard()));

    scores.apply(ParDo.of(new CreateDeleteOperation()))
            .apply("shuffle writes", Reshuffle.viaRandomKey())
            .apply(FirestoreIO.v1().write().batchWrite().build());

    pipeline.run().waitUntilFinish();

  }

  public static void main(String[] args) throws IOException {

    GCSPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSPipelineOptions.class);
    options.setStreaming(true);

    new DataPipeLineToFirestore().buildPipeline(options);

  }
}
