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
package org.apache.kafka.common.log.remote.storage;

import java.io.File;

public class LogSegmentData {

    private final File logSegment;
    private final File offsetIndex;
    private final File timeIndex;
    //todo add other required indexes like txnIndex

    public LogSegmentData(File logSegment, File offsetIndex, File timeIndex) {
        this.logSegment = logSegment;
        this.offsetIndex = offsetIndex;
        this.timeIndex = timeIndex;
    }

    public File logSegment() {
        return logSegment;
    }

    public File offsetIndex() {
        return offsetIndex;
    }

    public File timeIndex() {
        return timeIndex;
    }
}
