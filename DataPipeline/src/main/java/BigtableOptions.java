import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Description;


public interface BigtableOptions extends GCSPipelineOptions {
  @Description("The Bigtable project ID, this can be different than your Dataflow project")
  String getBigtableProjectId();

  void setBigtableProjectId(String bigtableProjectId);

  @Description("The Bigtable instance ID")
  String getBigtableInstanceId();

  void setBigtableInstanceId(String bigtableInstanceId);

  @Description("The Bigtable table ID in the instance.")
  String getBigtableTableId();

  void setBigtableTableId(String bigtableTableId);
}
