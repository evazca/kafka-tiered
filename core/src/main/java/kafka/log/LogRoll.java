package kafka.log;

import org.slf4j.Logger;

import java.util.List;

public class LogRoll implements SegmentDeletionReason {

    private final Logger logger;

    public LogRoll(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void logReason(List<LogSegment> toDelete) {
        logger.info("Deleting segments as part of log roll: {}", LocalLog.mkString(toDelete, ", "));
    }
}
