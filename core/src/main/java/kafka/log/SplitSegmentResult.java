package kafka.log;

import java.util.Collection;


/**
 * Holds the result of splitting a segment into one or more segments, see LocalLog.splitOverflowedSegment().
 *
 */
public class SplitSegmentResult {
    private final Collection<LogSegment> deletedSegments;
    private final Collection<LogSegment> newSegments;

    /**
     *
     * @param deletedSegments segments deleted when splitting a segment
     * @param newSegments new segments created when splitting a segment
     */
    SplitSegmentResult(Collection<LogSegment> deletedSegments, Collection<LogSegment> newSegments) {
        this.deletedSegments = deletedSegments;
        this.newSegments = newSegments;
    }

}
