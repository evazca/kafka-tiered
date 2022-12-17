/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.log.internals;

import org.apache.kafka.common.KafkaException;

import java.util.Objects;

/**
 * A log offset structure, including:
 * 1. the message offset
 * 2. the base message offset of the located segment
 * 3. the physical position on the located segment
 */
public class LogOffsetMetadata {
    public static final LogOffsetMetadata UNKNOWN_OFFSET_METADATA = new LogOffsetMetadata(-1, 0, 0);
    public static final int UNKNOWN_FILE_POSITION = -1;
    public static final long UNKNOWN_OFFSET = -1L;

    public final long messageOffset;
    public final long segmentBaseOffset;
    public final int relativePositionInSegment;

    public LogOffsetMetadata(long messageOffset) {
        this(messageOffset, UNKNOWN_OFFSET, UNKNOWN_FILE_POSITION);
    }

    public LogOffsetMetadata(long messageOffset, long segmentBaseOffset, int relativePositionInSegment) {
        this.messageOffset = messageOffset;
        this.segmentBaseOffset = segmentBaseOffset;
        this.relativePositionInSegment = relativePositionInSegment;
    }

    /**
     * Returns true if this offset is already on an older segment compared with the given offset
     */
    public boolean onOlderSegment(LogOffsetMetadata that) {
        if (messageOffsetOnly())
            throw new KafkaException(this + " cannot compare its segment info with " + that + " since it only has message offset info");

        return this.segmentBaseOffset < that.segmentBaseOffset;
    }

    /**
     * Returns true if this offset is on the same segment with the given offset
     *
     * @param that
     * @return
     */
    public boolean onSameSegment(LogOffsetMetadata that) {
        if (messageOffsetOnly())
            throw new KafkaException(this + " cannot compare its segment info with " + that + " since it only has message offset info");

        return this.segmentBaseOffset == that.segmentBaseOffset;
    }

    /**
     * Returns the number of messages between this instance offset to the given offset
     */
    public long offsetDiff(LogOffsetMetadata that) {
        return this.messageOffset - that.messageOffset;
    }

    /**
     * Compute the number of bytes between this offset to the given offset if they are on the same segment and this
     * offset precedes the given offset
     */
    public int positionDiff(LogOffsetMetadata that) {
        if (!onSameSegment(that))
            throw new KafkaException(this + " cannot compare its segment position with " + that + " since they are not on the same segment");
        if (messageOffsetOnly())
            throw new KafkaException(this + " cannot compare its segment position with " + that + " since it only has message offset info");

        return this.relativePositionInSegment - that.relativePositionInSegment;
    }

    /**
     * Returns true if the offset metadata only contains message offset info
     */
    public boolean messageOffsetOnly() {
        return segmentBaseOffset == UNKNOWN_OFFSET && relativePositionInSegment == LogOffsetMetadata.UNKNOWN_FILE_POSITION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogOffsetMetadata that = (LogOffsetMetadata) o;
        return messageOffset == that.messageOffset && segmentBaseOffset == that.segmentBaseOffset && relativePositionInSegment == that.relativePositionInSegment;
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageOffset, segmentBaseOffset, relativePositionInSegment);
    }

    @Override
    public String toString() {
        return "LogOffsetMetadata{" + "messageOffset=" + messageOffset + ", segmentBaseOffset=" + segmentBaseOffset + ", relativePositionInSegment=" + relativePositionInSegment + '}';
    }
}
