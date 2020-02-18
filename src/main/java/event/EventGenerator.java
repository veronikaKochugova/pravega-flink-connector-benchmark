package event;

import java.io.Serializable;

public interface EventGenerator<T extends Event> extends Serializable {
    T nextEvent(long taskindex);

    EventGeneratorState snapshotState();

    void restoreState(EventGeneratorState state);
}
