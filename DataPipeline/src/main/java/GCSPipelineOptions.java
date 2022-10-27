import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;

public interface GCSPipelineOptions extends PipelineOptions, StreamingOptions {

    @Description("Path of the input file including its filename prefix.")
    @Validation.Required
    String getInput();

    void setInput(String value);
}
