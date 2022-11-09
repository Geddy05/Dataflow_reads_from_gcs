
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import models.Player;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import pTransforms.JsonToPlayerPipeline;
import pTransforms.PollingGCSPipeline;

public class DataPipelineBigTable {

  static class PlayerToBigtablePut extends DoFn<Player, Mutation> {
    @ProcessElement
    public void processElement(@Element Player player, OutputReceiver<Mutation> out) {
      long timestamp = System.currentTimeMillis();
      // Create the row key for Bigtable. For this example we are using the key format teamId#UserId#Timestamp
      // This is not according the best practices for the row-key design more about the row-key design can be found here:
      // https://cloud.google.com/bigtable/docs/schema-design#row-keys
      Put row = new Put(Bytes.toBytes(player.teamId+"#"+player.userId+"#"+player.timestamp.toString()));

      // Add the column to the Row we created earlier.
      // The add column function needs te following parameter family, qualifier, timestamp, value
      // more about schema design: https://cloud.google.com/bigtable/docs/schema-design
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

    // Create BigTable configurations.
    // We need to set the Project ID, Instance ID and TableId.
    CloudBigtableTableConfiguration bigtableTableConfig =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId())
            .build();

    // Create the pipeline with the given options.
    Pipeline p = Pipeline.create(options);

    PCollection<Player> playerScore = p
            // rfcStartDateTime: Only read files with an updated timestamp greater than the rfcStartDateTime.
            .apply("Read files from Cloud Storage",
                new PollingGCSPipeline(options.getInput(),null))
            // File content to Player objects
            .apply("File to Players", new JsonToPlayerPipeline());

    playerScore
        // Create a put Operation based on the input
        .apply("Player to Put Operation", ParDo.of(new PlayerToBigtablePut()))
        // Write the given put operations. It used the configurations given earlier.
        .apply(CloudBigtableIO.writeToTable(bigtableTableConfig));

    p.run().waitUntilFinish();
  }
}