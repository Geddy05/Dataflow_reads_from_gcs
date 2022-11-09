import org.apache.beam.sdk.options.Description;

public interface BigqueryOptions extends GCSPipelineOptions {
  @Description("The Bigtable project ID, this can be different than your Dataflow project")
  String getBigqueryProjectId();

  void setBigqueryProjectId(String bigqueryProjectId);

  @Description("The Bigtable instance ID")
  String getBigqueryDatasetId();

  void setBigqueryDatasetId(String bigqueryDatasetId);

  @Description("The Bigtable table ID in the instance.")
  String getBigqueryTableId();

  void setBigqueryTableId(String bigqueryTableId);

}
