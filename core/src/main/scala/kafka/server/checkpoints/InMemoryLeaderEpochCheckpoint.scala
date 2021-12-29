/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.checkpoints

import kafka.server.epoch.EpochEntry
import org.apache.kafka.server.common.CheckpointFile.CheckpointWriteBuffer

import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

class InMemoryLeaderEpochCheckpoint extends LeaderEpochCheckpoint {
  private var epochs: Seq[EpochEntry] = Seq()

  override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq

  override def read(): Seq[EpochEntry] = this.epochs

  def readAsByteBuffer(): ByteBuffer = {
    val stream = new ByteArrayOutputStream()
    val writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8))
    val writeBuffer = new CheckpointWriteBuffer[EpochEntry](writer, 0, LeaderEpochCheckpointFile.Formatter)
    try {
      writeBuffer.write(epochs.asJava)
      writer.flush()
      ByteBuffer.wrap(stream.toByteArray)
    } finally {
      writer.close()
    }
  }
}