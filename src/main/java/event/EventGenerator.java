package event;

import java.io.Serializable;

public interface EventGenerator<T extends Event> extends Serializable {
    T nextEvent(final long taskindex);

    EventGeneratorState snapshotState();

    void restoreState(final EventGeneratorState state);
}
