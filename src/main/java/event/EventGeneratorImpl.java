package event;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventGeneratorImpl implements EventGenerator<Event> {
    private static Logger LOG = LoggerFactory.getLogger(EventGeneratorImpl.class);

    final Random rand = new Random();
    final Clock startTime = Clock.fixed(Instant.now(), ZoneId.systemDefault());
    final int hotkeyMovementMins = 10; // Note pravega splits/merges every 10 mins by default

    private int payloadSize;
    private boolean produceHotKeys;
    private int numberOfKeys;
    private List<String> keys;
    private long timestamp;
    private HashMap<String, Long> sequenceCounters;

    public EventGeneratorImpl(final int numberOfKeys, final int payloadSize, final boolean produceHotKeys) {
        this.payloadSize = payloadSize;
        this.produceHotKeys = produceHotKeys;
        this.numberOfKeys = numberOfKeys - 1;

        // Generate Keyspace
        this.keys = Stream.generate(() -> UUID.randomUUID().toString())
                .limit(numberOfKeys)
                .collect(Collectors.toList());

        this.sequenceCounters = new HashMap<>();
    }

    @Override
    public Event nextEvent(final long taskindex) {
        String routingKeykey = getRoutingKey(taskindex);
        long sequenceNumber = getRoutingKeySequence(routingKeykey);
        return new Event(routingKeykey, RandomStringUtils.randomAscii(payloadSize), timestamp++, sequenceNumber, taskindex);
    }

    @Override
    public EventGeneratorState snapshotState() {
        return new SnapshotState(timestamp, sequenceCounters);
    }

    @Override
    public void restoreState(final EventGeneratorState state) {
        if (!(state instanceof SnapshotState)) {
            throw new IllegalStateException(state.getClass().getName() + " is not of expected type " + SnapshotState.class.getName());
        }

        timestamp = ((SnapshotState) state).getTimestamp();
        sequenceCounters = ((SnapshotState) state).getSequenceMap();
    }

    private long getRoutingKeySequence(final String key) {
        long sequenceNum = 0;
        if (sequenceCounters.containsKey(key)) {
            sequenceNum = sequenceCounters.get(key) + 1;
        }

        sequenceCounters.put(key, sequenceNum);
        return sequenceNum;
    }

    private String getRoutingKey(final long taskindex) {
        if (!produceHotKeys) {
            return keys.get(rand.nextInt(keys.size())) + "_" + String.valueOf(taskindex);
        }

        // get number between [0,1], transform to int in [0, numberOfKeys)
        // Since this is a normal random number, the values in the middle are 'hotter' (more likely to be picked)
        Double normalRandom = getNormalRandom();
        long normalRoutingKey = Math.round(Math.floor(numberOfKeys * normalRandom));

        Instant timeNow = Instant.now();

        // every hotkeyMovementMins, the 'hottest' key moves over by one w/ wrap around
        int skew = Long.valueOf(startTime.instant().until(timeNow, ChronoUnit.SECONDS)).intValue() / (60 * hotkeyMovementMins);
        int skewedRoutingKey = Long.valueOf((normalRoutingKey + skew) % numberOfKeys).intValue();

        try {
            return keys.get(skewedRoutingKey) + "_" + String.valueOf(taskindex);
        } catch (Exception e) {
            LOG.error("Numbers: ${normalRandom} ${normalRoutingKey} ${skew} ${skewedRoutingKey} ${keys.size} ${numberOfKeys}");
            throw e;
        }
    }

    private Double getNormalRandom() {
        // Clip into [-2, 2]
        Double randomSample;
        do {
            randomSample = rand.nextGaussian();
        } while (Math.abs(randomSample) > 2.0);

        //convert it to [0, 1]
        return (randomSample + 2.0) / 4.0;
    }

    public static class SnapshotState implements EventGeneratorState {
        private static final long serialVersionUID = 7308672753814875921L;

        private long timestamp;
        private HashMap<String, Long> sequenceCounters;

        public SnapshotState(long timestamp, HashMap<String, Long> sequenceCounters) {
            this.timestamp = timestamp;
            this.sequenceCounters = sequenceCounters;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public HashMap<String, Long> getSequenceMap() {
            return sequenceCounters;
        }
    }
}