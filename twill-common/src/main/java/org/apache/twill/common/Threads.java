/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.common;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public final class Threads {

  /**
   * A executor that execute task from the submitter thread.
   */
  public static final Executor SAME_THREAD_EXECUTOR = new Executor() {
    @Override
    public void execute(Runnable command) {
      command.run();
    }
  };

  /**
   * Handy method to create {@link ThreadFactory} that creates daemon threads with the given name format.
   *
   * @param nameFormat Name format for the thread names. It should be a format string compatible
   *                   with the {@link String#format(String, Object...)} that takes a single number as the format
   *                   argument.
   * @return A {@link ThreadFactory}.
   */
  public static ThreadFactory createDaemonThreadFactory(final String nameFormat) {
    final AtomicLong id = new AtomicLong(0);
    return new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName(String.format(nameFormat, id.getAndIncrement()));
        return t;
      }
    };
  }

  private Threads() {
  }
}
