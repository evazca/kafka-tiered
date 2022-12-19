/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.utils;

import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class ShutdownableThread extends Thread {

  public final String name;
  public final boolean isInterruptible;
  private final CountDownLatch shutdownInitiated = new CountDownLatch(1);
  private final CountDownLatch shutdownComplete = new CountDownLatch(1);
  private final Logger log;
  private volatile boolean isStarted = false;

  public ShutdownableThread(String name, boolean isInterruptible) {
    super(name);
    this.name = name;
    this.isInterruptible = isInterruptible;
    this.setDaemon(false);
    LogContext logContext = new LogContext("[" + name + "]: ");
    this.log = logContext.logger(getClass());
  }

  public ShutdownableThread(String name) {
    this(name, true);
  }

  /**
   * Causes the current thread to wait until the shutdown is initiated,
   * or the specified waiting time elapses.
   *
   * @param timeout
   * @param unit
   */
  public void pause(long timeout, TimeUnit unit) throws InterruptedException {
    if (shutdownInitiated.await(timeout, unit))
      log.trace("shutdownInitiated latch count reached zero. Shutdown called.");
  }

  /**
   * This method is repeatedly invoked until the thread shuts down or this method throws an exception
   */
  public abstract void doWork();

  @Override
  public void run() {
    isStarted = true;
    log.info("Starting");
    try {
      while (isRunning())
        doWork();
    } catch (FatalExitError ex) {
      shutdownInitiated.countDown();
      shutdownComplete.countDown();
      log.info("Stopped");
      Exit.exit(ex.statusCode());
    } catch (Throwable th) {
      if (isRunning())
        log.error("Error due to", th);
    } finally {
      shutdownComplete.countDown();
    }
    log.info("Stopped");
  }

  public boolean isRunning() {
    return !isShutdownInitiated();
  }

  public void shutdown() throws InterruptedException {
    initiateShutdown();
    awaitShutdown();
  }

  public boolean isShutdownInitiated() {
    return shutdownInitiated.getCount() == 0;
  }

  public boolean isShutdownComplete() {
    return shutdownComplete.getCount() == 0;
  }

  /**
   * @return true if there has been an unexpected error and the thread shut down
   */
  // mind that run() might set both when we're shutting down the broker
  // but the return value of this function at that point wouldn't matter
  public boolean isThreadFailed() {
    return isShutdownComplete() && !isShutdownInitiated();
  }

  public boolean initiateShutdown() {
    synchronized (this) {
      if (isRunning()) {
        log.info("Shutting down");
        shutdownInitiated.countDown();
        if (isInterruptible)
          interrupt();
        return true;
      } else
        return false;
    }
  }

  /**
   * After calling initiateShutdown(), use this API to wait until the shutdown is complete
   */
  public void awaitShutdown() throws InterruptedException {
    if (!isShutdownInitiated())
      throw new IllegalStateException("initiateShutdown() was not called before awaitShutdown()");
    else {
      if (isStarted)
        shutdownComplete.await();
      log.info("Shutdown completed");
    }
  }


}
