import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class DataPipeline {

    @DefaultCoder(AvroCoder.class)
    class Player implements Serializable {
        int userId;
        int teamId;
        int points;
    }

    static class JsonToPlayer extends DoFn<String, Player> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            // Use OutputReceiver.output to emit the output element.
            Gson gson = new GsonBuilder().create();
            Player player = gson.fromJson(c.element(), Player.class);
            c.output(player);
        }
    }

    static class PlayerToTableRow extends DoFn<Player, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            // Use OutputReceiver.output to emit the output element.
            Player player = c.element();
            TableRow row = new TableRow();
            if(player != null){
                row.set("userId", player.userId);
                row.set("teamId", player.teamId);
                row.set("points", player.points);

                c.output(row);
            }
        }
    }

    public void buildPipeline(GCSPipelineOptions options){
        Pipeline pipeline = Pipeline.create(options);

        TableReference tableRef = new TableReference();
        tableRef.setProjectId("geddy-playground");
        tableRef.setDatasetId("playerDemo");
        tableRef.setTableId("scores");

        List<TableFieldSchema> fieldDefs = new ArrayList<>();
        fieldDefs.add(new TableFieldSchema().setName("userId").setType("INTEGER"));
        fieldDefs.add(new TableFieldSchema().setName("teamId").setType("INTEGER"));
        fieldDefs.add(new TableFieldSchema().setName("points").setType("INTEGER"));

        pipeline.apply("Read files from Cloud Storage",
                new PollingGCSPipeline(options.getInput(),null))
            .apply("FileReadConcurrency",
                        Reshuffle.<FileIO.ReadableFile>viaRandomKey().withNumBuckets(1))
            .apply("ReadFiles", TextIO.readFiles())
            .apply("ReshuffleRecords", Reshuffle.viaRandomKey())
            .apply("Parse Json", ParDo.of(new JsonToPlayer()))
            .apply("Player to TableRow", ParDo.of(new PlayerToTableRow()))
            .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                .to(tableRef)
                .withSchema(new TableSchema().setFields(fieldDefs))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

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
