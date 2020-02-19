package event;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.concurrent.locks.LockSupport;

public class EventPayloadGeneratorSource extends RichParallelSourceFunction {

    boolean cancelled = false;
    double rate;
    int size;

    EventPayloadGeneratorSource(final int size, final double rate) {
        this.rate = rate;
        this.size = size;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        long nextTimeNanos = 0L;
        long delayNanos = (rate > 0) ? new Double(1e9 / rate).longValue() : 0L;
        while (!cancelled) {
            if (nextTimeNanos < System.nanoTime()) {
                LockSupport.parkNanos(1L);
            } else {
                nextTimeNanos = System.nanoTime() + delayNanos;
                ctx.collect(new ArrayList<Byte>(size));
            }
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}