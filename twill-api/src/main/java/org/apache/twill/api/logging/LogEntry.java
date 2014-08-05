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
package org.apache.twill.api.logging;

/**
 * Represents a log entry emitted by application.
 */
public interface LogEntry {

  /**
   * Log level.
   */
  enum Level {
    FATAL,
    ERROR,
    WARN,
    INFO,
    DEBUG,
    TRACE
  }

  /**
   * Returns name of the logger.
   */
  String getLoggerName();

  /**
   * Returns hostname of where the log emitted.
   */
  String getHost();

  /**
   * Returns timestamp of the log.
   */
  long getTimestamp();

  /**
   * Returns the log {@link Level} of the log.
   */
  Level getLogLevel();

  /**
   * Returns the class name where the log emitted.
   */
  String getSourceClassName();

  /**
   * Returns the method name where the log emitted.
   */
  String getSourceMethodName();

  /**
   * Returns the source file name where the log emitted.
   */
  String getFileName();

  /**
   * Returns the line number in the source file where the log emitted.
   */
  int getLineNumber();

  /**
   * Returns the name of the thread where the log emitted.
   */
  String getThreadName();

  /**
   * Returns the log message.
   */
  String getMessage();

  /**
   * Returns the runnable name.
   */
  String getRunnableName();

  /**
   * Returns the {@link Throwable} information emitted with the log.
   *
   * @return A {@link LogThrowable} or {@code null} if {@link Throwable} information is not available.
   */
  LogThrowable getThrowable();

  /**
   * Returns the stack trace of the throwable information emitted with the log.
   *
   * @return the stack trace information or an empty array if {@link Throwable} information is not available.
   * @deprecated Use {@link #getThrowable()} instead.
   */
  @Deprecated
  StackTraceElement[] getStackTraces();
}
