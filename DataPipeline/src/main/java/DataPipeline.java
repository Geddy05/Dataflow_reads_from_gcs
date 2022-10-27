import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import models.Player;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import utils.GsonUTCDateAdapter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class DataPipeline {

    static class JsonToPlayer extends DoFn<String, Player> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            // Use OutputReceiver.output to emit the output element.
            Gson gson = new GsonBuilder().registerTypeAdapter(Date.class, new GsonUTCDateAdapter()).create();
            System.out.println("received JSON object: " + c.element());
            List<Player> players = gson.fromJson(c.element(), new TypeToken<List<Player>>(){}.getType());
            for (Player player: players) {
                c.output(player);
            }
        }
    }

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

    private TableReference createLogTabelReference(){
        TableReference tableRef = new TableReference();
        tableRef.setProjectId("geddy-playground");
        tableRef.setDatasetId("playerDemo");
        tableRef.setTableId("leaderboard");
        return tableRef;
    }

    private List<TableFieldSchema> createLogTabelSchema(){
        List<TableFieldSchema> fieldDefs = new ArrayList<>();
        fieldDefs.add(new TableFieldSchema().setName("username").setType("STRING"));
        fieldDefs.add(new TableFieldSchema().setName("userId").setType("INTEGER"));
        fieldDefs.add(new TableFieldSchema().setName("teamId").setType("INTEGER"));
        fieldDefs.add(new TableFieldSchema().setName("points").setType("INTEGER"));
        fieldDefs.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
        return fieldDefs;
    }

    public void buildPipeline(GCSPipelineOptions options){
        Pipeline pipeline = Pipeline.create(options);

        TableReference tableRef = createLogTabelReference();
        List<TableFieldSchema> logTabelSchema = createLogTabelSchema();

        PCollection<Player> playerScore = pipeline
            // rfcStartDateTime: Only read files with an updated timestamp greater than the rfcStartDateTime.
            .apply("Read files from Cloud Storage",
                new PollingGCSPipeline(options.getInput(),null))
            // Number files read in parallel
            .apply("FileReadConcurrency",
                        Reshuffle.<FileIO.ReadableFile>viaRandomKey().withNumBuckets(1))
            .apply("ReadFiles", TextIO.readFiles())
            // Because we split each line to a single event we cen get a high fan-out.
            .apply("ReshuffleRecords", Reshuffle.viaRandomKey())
            .apply("Parse Json", ParDo.of(new JsonToPlayer()));

        // Write individual scores in to BigQuery
        WriteResult result = playerScore
            .apply("Player to TableRow", ParDo.of(new PlayerToTableRow()))
            .apply("WriteScoresToBigQuery", BigQueryIO.writeTableRows()
                .to(tableRef)
                .withSchema(new TableSchema().setFields(logTabelSchema))
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                .withExtendedErrorInfo()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        // Apply dead-letter pattern for bigquery
        result
            .getFailedInsertsWithErr()
            .apply("Deadletter Bigquery",
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        x -> {
                            System.out.println(" The table was " + x.getTable());
                            System.out.println(" The row was " + x.getRow());
                            System.out.println(" The error was " + x.getError());
                            return "";
                        }));

        pipeline.run().waitUntilFinish();

    }

    public static void main(String[] args) throws IOException {

        GCSPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSPipelineOptions.class);
        // For cloud execution, set the Google Cloud project, staging location,
        // and set DataflowRunner.
        options.setInput("gs://files2809");
        options.setStreaming(true);

        new DataPipeline().buildPipeline(options);

    }
}
