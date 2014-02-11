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
 * Carries {@link Throwable} information in a {@link LogEntry}.
 */
public interface LogThrowable {

  /**
   * Returns the name of the Throwable class.
   */
  String getClassName();

  /**
   * Returns the message contained inside the Throwable.
   *
   * @return A {@link String} message or {@code null} if such message is not available.
   */
  String getMessage();

  /**
   * Returns the stack trace of the Throwable.
   */
  StackTraceElement[] getStackTraces();

  /**
   * Returns the cause of this {@link LogThrowable}.
   *
   * @return The {@link LogThrowable} cause or {@code null} if no cause is available.
   */
  LogThrowable getCause();
}
