
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import models.Player;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.Duration;
import pTransforms.JsonToPlayerPipeline;
import pTransforms.PollingGCSPipeline;

public class DataPipelineBigTable {

  static class PlayerToBigtablePut extends DoFn<Player, Mutation> {
    @ProcessElement
    public void processElement(@Element Player player, OutputReceiver<Mutation> out) {
      long timestamp = System.currentTimeMillis();
      Put row = new Put(Bytes.toBytes(player.teamId+"#"+player.userId+"#"+player.timestamp.toString()));

      row.addColumn(
          Bytes.toBytes("stats_summary"),
          Bytes.toBytes("username"),
          timestamp,
          Bytes.toBytes(player.username));

      row.addColumn(
          Bytes.toBytes("stats_summary"),
          Bytes.toBytes("userId"),
          timestamp,
          Bytes.toBytes(player.userId));

      row.addColumn(
          Bytes.toBytes("stats_summary"),
          Bytes.toBytes("teamId"),
          timestamp,
          Bytes.toBytes(player.teamId));

      row.addColumn(
          Bytes.toBytes("stats_summary"),
          Bytes.toBytes("points"),
          timestamp,
          Bytes.toBytes(player.points));

      out.output(row);
    }
  }

  public static void main(String[] args) {

    BigtableOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtableOptions.class);
    options.setStreaming(true);


    CloudBigtableTableConfiguration bigtableTableConfig =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId())
            .build();
    Pipeline p = Pipeline.create();

    PCollection<Player> playerScore = p
            // rfcStartDateTime: Only read files with an updated timestamp greater than the rfcStartDateTime.
            .apply("Read files from Cloud Storage",
                new PollingGCSPipeline(options.getInput(),null))
            // File content to Player objects
            .apply("File to Players", new JsonToPlayerPipeline());

    playerScore
        .apply("window", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply("Player to Put Operation", ParDo.of(new PlayerToBigtablePut()))
        .apply(CloudBigtableIO.writeToTable(bigtableTableConfig));

    p.run().waitUntilFinish();
  }
}