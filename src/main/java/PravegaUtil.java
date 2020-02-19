import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.connectors.flink.PravegaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class PravegaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PravegaUtil.class.getSimpleName());


    public PravegaConfig config(String scope, URI controllerUri) {
        return PravegaConfig
                .fromDefaults()
                .withDefaultScope(scope)
                .withControllerURI(controllerUri);
    }

    public static void prepare(String scope, String stream, PravegaConfig pravegaConfig) {
        ControllerImplConfig controllerConfig = ControllerImplConfig.builder()
                .clientConfig(pravegaConfig.getClientConfig())
                .build();
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(pravegaConfig.getClientConfig());
        Controller controller = new ControllerImpl(controllerConfig, connectionFactory.getInternalExecutor());
        try {
            controller
                    .createScope(scope)
                    .get(100, TimeUnit.SECONDS);
        } catch (Throwable e) {
            LOG.warn("Scope creating failure: " + e);
        }
        try {
            StreamConfiguration streamConfig = StreamConfiguration.builder().build();
            controller
                    .createStream(scope, stream, streamConfig)
                    .get(100, TimeUnit.SECONDS);
        } catch (Throwable e) {
            LOG.warn("Stream creating failure: " + e);
        }
        if (controller != null) {
            try {
                controller.close();
            } catch (Throwable e) {
                LOG.warn("Controller closing failure: " + e);
            }
        }
    }
}