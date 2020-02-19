import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serde.RawBytesDeserializationSchema;

import java.net.URI;
import java.util.concurrent.TimeUnit;


public class PravegaReadBenchmarkJob {

    private static final String JOB_NAME = PravegaReadBenchmarkJob.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(JOB_NAME);
    private static final String READ_TIMEOUT_MILLIS_PARAM = "readTimeoutMillis";
    private static final long DEFAULT_READ_TIMEOUT_MILLIS = Long.MAX_VALUE;


    private static Options setCLIOptions() {
        Options options = new Options();
        options.addOption(Constants.SCOPE_PARAM, false, "display current time");
        options.addOption(Constants.STREAM_PARAM, false, "display current time");
        options.addOption(Constants.CONTROLLER_PARAM, false, "display current time");
        options.addOption(READ_TIMEOUT_MILLIS_PARAM, false, "display current time");
        return options;
    }

    public static void main(String[] args) throws ParseException {
        LOG.info("Starting the " + JOB_NAME + " with args: " + (args.toString()));
        // initialize the parameter utility tool in order to retrieve input parameters

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(setCLIOptions(), args);
        String scope = cmd.getOptionValue(Constants.SCOPE_PARAM, Constants.DEFAULT_SCOPE);
        String stream = cmd.getOptionValue(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM);
        String controllerUri = cmd.getOptionValue(Constants.CONTROLLER_PARAM, Constants.DEFAULT_CONTROLLER);
        long readTimeoutMillis = cmd.getOptionValue(READ_TIMEOUT_MILLIS_PARAM, DEFAULT_READ_TIMEOUT_MILLIS);
        //
        PravegaConfig pravegaConfig = PravegaUtil.config(scope, URI.create(controllerUri));
        PravegaUtil.prepare(scope, stream, pravegaConfig);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // create the Pravega source to read a stream of text
        FlinkPravegaReader src = FlinkPravegaReader
                .<byte[]>builder()
                .withPravegaConfig(pravegaConfig)
                .withEventReadTimeout(Time.of(readTimeoutMillis, TimeUnit.MILLISECONDS))
                .forStream(new StreamImpl(scope, stream))
                .withDeserializationSchema(new RawBytesDeserializationSchema())
                .build();

        // count each message over a 1 second time period
        val dataStream = env
                .addSource(src)
                .map((_:Array[Byte]) =>1L)
                .timeWindowAll(Time.seconds(1))
                .reduce(Long.sum())
        dataStream.print
        env execute JOB_NAME
    }
}
