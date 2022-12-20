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
package org.apache.kafka.utils;

import org.apache.kafka.server.log.internals.StorageAction;

import java.util.List;

public final class ServerUtils {

    /**
     * Replace the given string suffix with the new suffix. If the string doesn't end with the given suffix throw an exception.
     */
    public static String replaceSuffix(String name, String oldSuffix, String newSuffix) {
        if (!name.endsWith(oldSuffix))
            throw new IllegalArgumentException("Expected string to end with " + oldSuffix + " but string is " + name);
        return name.substring(0, name.length() - oldSuffix.length()) + newSuffix;
    }


    /**
     * Invokes every function in `all` even if one or more functions throws an exception.
     * If any of the functions throws an exception, the first one will be rethrown at the end with subsequent exceptions
     * added as suppressed exceptions.
     */
    public static void tryAll(List<StorageAction<Void, Throwable>> all) throws Throwable {
        Throwable exception = null;

        for(StorageAction<Void, Throwable> runnable: all) {
            try {
                runnable.execute();
            } catch (Throwable th) {
                if (exception != null)
                    exception.addSuppressed(th);
                else
                    exception = th;
            }
        }
        if (exception != null)
            throw exception;
    }
}
