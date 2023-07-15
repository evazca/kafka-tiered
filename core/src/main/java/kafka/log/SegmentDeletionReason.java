package kafka.log;

import java.util.List;

public interface SegmentDeletionReason {
    void logReason(List<LogSegment> toDelete);
}
