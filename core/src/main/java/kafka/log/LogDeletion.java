package kafka.log;

import org.slf4j.Logger;

import java.util.List;

public class LogDeletion implements SegmentDeletionReason {

    private final Logger logger;

    public LogDeletion(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void logReason(List<LogSegment> toDelete) {
        logger.info("Deleting segments as the log has been deleted: {}", LocalLog.mkString(toDelete, ", "));
    }
}
