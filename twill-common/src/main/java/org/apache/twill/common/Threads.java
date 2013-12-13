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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 *
 */
public final class Threads {

  /**
   * A executor that execute task from the submitter thread.
   */
  public static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();

  /**
   * Handy method to create {@link ThreadFactory} that creates daemon threads with the given name format.
   *
   * @param nameFormat Name format for the thread names
   * @return A {@link ThreadFactory}.
   * @see ThreadFactoryBuilder
   */
  public static ThreadFactory createDaemonThreadFactory(String nameFormat) {
    return new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat(nameFormat)
      .build();
  }

  private Threads() {
  }
}
