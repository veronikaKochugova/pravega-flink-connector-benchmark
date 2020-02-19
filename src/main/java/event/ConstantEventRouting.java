package event;

import io.pravega.connectors.flink.PravegaEventRouter;

public class ConstantEventRouting implements PravegaEventRouter {

    private final String routingKey;

    ConstantEventRouting(final String routingKey) {
        this.routingKey = routingKey;
    }

    @Override
    public String getRoutingKey(final Object o) {
        return routingKey;
    }
}