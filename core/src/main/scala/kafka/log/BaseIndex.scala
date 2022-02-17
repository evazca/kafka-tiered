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
package kafka.log

import java.io.{Closeable, File}
import java.nio.file.Path

import org.apache.kafka.common.utils.Utils

/**
 * This class represents a common abstraction for operations like delete and rename of the index files.
 *
 * @param _file index file
 * @see [[AbstractIndex]], [[OffsetIndex]], [[TimeIndex]], [[TransactionIndex]]
 */
abstract class BaseIndex(@volatile var _file: File) extends Closeable {

  /**
   * Rename the file that backs this index
   *
   * @throws IOException if rename fails
   */
  def renameTo(toBeRenamedFile: File): Unit = {
    try
      if (_file.exists) Utils.atomicMoveWithFallback(_file.toPath, toBeRenamedFile.toPath, false)
    finally _file = toBeRenamedFile
  }

  def file: File = _file

  def path: Path = if (file.exists()) file.toPath else null

  def deleteIfExists(): Boolean
}