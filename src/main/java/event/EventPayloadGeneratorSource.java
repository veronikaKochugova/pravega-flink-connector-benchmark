package event;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

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
        long delayNanos = (rate > 0) ? Long((1e9 / rate).toLong : 0L;
        while (!cancelled) {
            if (nextTimeNanos < System.nanoTime()) {
                LockSupport parkNanos 1L
            } else {
                nextTimeNanos = System.nanoTime() + delayNanos
                ctx.collect(new Array[Byte] (size))
            }
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}

class EventPayloadGeneratorSource(size:Int,rate:Double)
        extends RichParallelSourceFunction[Array[Byte]]{

        var cancelled=false

        override def run(ctx:SourceFunction.SourceContext[Array[Byte]]):Unit={

        }

        override def cancel():Unit={

        }
        }
