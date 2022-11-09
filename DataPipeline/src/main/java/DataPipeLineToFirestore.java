import com.google.api.services.bigquery.model.TableRow;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import models.Player;
import models.TeamScore;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import pTransforms.JsonToPlayerPipeline;
import pTransforms.PollingGCSPipeline;


import java.io.IOException;
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


  private static final class CreateDeleteOperation extends DoFn<TeamScore, Write> {
    @ProcessElement
    public void processElement(ProcessContext c) throws IllegalAccessException {
      TeamScore teamScore = c.element();

      Map<String, Value> teamMap = new HashMap<>();

      teamMap.put("totalScore", Value.newBuilder().setIntegerValue(teamScore.getTotalScore()).build());
      teamMap.put("points", Value.newBuilder().setIntegerValue(teamScore.getTeamId()).build());
      teamMap.put("timestamp", Value.newBuilder().setStringValue(teamScore.getTimestamp().toString()).build());

      Document.Builder builder = Document
          .newBuilder()
          .setName("projects/geddy-dataflow-playground/databases/(default)/documents/score/" + teamScore.getTeamId()+teamScore.getTimestamp().toString())
          .putAllFields(teamMap);
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

  private static final class CreateTeamBoard extends DoFn<KV<Integer, Iterable<Player>>, TeamScore> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<Integer, Iterable<Player>> team = c.element();
      Integer teamId = team.getKey();
      int totalScore = 0;

      TeamScore teamScore = new TeamScore();
      teamScore.setTeamId(teamId);

      for (Player player: team.getValue()) {
        totalScore += player.points;
        teamScore.addPlayer(player);
      }

      teamScore.setTotalScore(totalScore);
      teamScore.setTimestamp( new Date());
      c.output(teamScore);
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
        .apply("Window" ,Window.<Player>into(FixedWindows.of(Duration.standardMinutes(5)))
            .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(5))))
            .withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes());

    PCollection<KV<Integer, Iterable<Player>>> teams = playerScore.apply("ExtractTeamId" , ParDo.of(new PlayerToKV()))
            .apply(GroupByKey.create());

    PCollection<TeamScore> scores = teams.apply("Create board",ParDo.of(new CreateTeamBoard()));

    scores.apply(ParDo.of(new CreateDeleteOperation()))
        .apply("shuffle writes", Reshuffle.viaRandomKey())
        .apply(Window.into(new GlobalWindows()))
        .apply(FirestoreIO.v1().write().batchWrite().withDeadLetterQueue().build());

    pipeline.run().waitUntilFinish();

  }

  public static void main(String[] args) throws IOException {

    GCSPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSPipelineOptions.class);
    options.setStreaming(true);

    new DataPipeLineToFirestore().buildPipeline(options);

  }
}
