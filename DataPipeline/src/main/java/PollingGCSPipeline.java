import avro.shaded.com.google.common.collect.Lists;
import com.google.api.client.util.DateTime;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;

public class PollingGCSPipeline extends PTransform<PBegin, PCollection<FileIO.ReadableFile>> {

    private String inputFilePattern;
    private String rfcStartDateTime;
    PCollection<String> directories = null;

    public PollingGCSPipeline(
            String inputFilePattern,
            String rfcStartDateTime) {
        this.inputFilePattern = inputFilePattern;
        this.rfcStartDateTime = rfcStartDateTime;

    };

    static class GCSPollFn extends Watch.Growth.PollFn<String, String> {
        private transient GcsUtil util;
        private final Integer minDepth;
        private final Integer maxDepth;
        private final DateTime dateFrom;
        private final String delimiter;

        public GCSPollFn(Integer minDepth, Integer maxDepth, String dateFrom, String delimiter){
            this.maxDepth = maxDepth;
            this.minDepth = minDepth;
            this.delimiter = delimiter;
            if (dateFrom != null) {
                this.dateFrom = DateTime.parseRfc3339(dateFrom);
            } else {
                this.dateFrom = DateTime.parseRfc3339("1970-01-01T00:00:00.00Z");
            }
        }

        private boolean shouldFilterObject(StorageObject object) {
            DateTime updatedDateTime = object.getUpdated();
            if (updatedDateTime.getValue() < this.dateFrom.getValue()) {
                return true;
            }
            return false;
        }

        private Integer getObjectDepth(String objectName) {
            int depthCount = 1;
            for (char i : objectName.toCharArray()) {
                if (i == '/') {
                    depthCount += 1;
                }
            }
            return depthCount;
        }

        private GcsUtil getUtil() {
            if (util == null) {
                util = new GcsUtil.GcsUtilFactory().create(PipelineOptionsFactory.create());
            }
            return util;
        }

        private List<TimestampedValue<String>> getMatchingObjects(GcsPath path) throws IOException {
            List<TimestampedValue<String>> result = new ArrayList<>();
            Integer baseDepth = getObjectDepth(path.getObject());
            GcsUtil util = getUtil();
            String pageToken = null;
            do {
                Objects objects =
                        util.listObjects(path.getBucket(), path.getObject(), pageToken, delimiter);
                pageToken = objects.getNextPageToken();
                List<StorageObject> items = firstNonNull(objects.getItems(), Lists.newArrayList());
                if (objects.getPrefixes() != null) {
                    for (String prefix : objects.getPrefixes()) {
                        result.add(
                                TimestampedValue.of("gs://" + path.getBucket() + "/" + prefix, Instant.EPOCH));
                    }
                }
                for (StorageObject object : items) {
                    String fullName = "gs://" + object.getBucket() + "/" + object.getName();
                    if (object.getName().equals(path.getObject())) {
                        // Output only direct children and not the directory itself.
                        continue;
                    }
                    if (shouldFilterObject(object)) {
                        // Skip file due to initial timestamp
                        continue;
                    }
                    Integer newDepth = getObjectDepth(object.getName());
                    if (baseDepth + minDepth <= newDepth && newDepth <= baseDepth + maxDepth) {
                        Instant fileUpdatedInstant = Instant.ofEpochMilli(object.getUpdated().getValue());
                        result.add(TimestampedValue.of(fullName, fileUpdatedInstant));
                    }
                }
            } while (pageToken != null);
            return result;
        }

        @Override
        public Watch.Growth.PollResult<String> apply(String element, Context c) throws Exception {
            GcsPath path = GcsPath.fromUri(element);
            return Watch.Growth.PollResult.incomplete(getMatchingObjects(path));
        }
    }

    @Override
    public PCollection<FileIO.ReadableFile> expand(PBegin input) {

        // Find the buckets in the given path:
        directories =
                input
                        .apply("StartPoll", Create.of(inputFilePattern))
                        .apply("FindFiles",
                                Watch.growthOf(new GCSPollFn(1, 1, this.rfcStartDateTime, "/"))
                                        .withPollInterval(Duration.standardSeconds(60)))
                    .apply(Values.create());

        return directories
                .apply(
                        "GetDirectoryGlobs",
                        MapElements.into(TypeDescriptors.strings()).via(path -> path + "**"))
                .apply(
                        "MatchFiles",
                        FileIO.matchAll()
                                .continuously(
                                        Duration.standardSeconds(5),
                                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardMinutes(10))))
                .apply("ReadFiles", FileIO.readMatches());
    }
}
