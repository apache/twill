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
package org.apache.twill.internal.json;

import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import org.apache.twill.api.logging.LogThrowable;

/**
 * Default implementation of the {@link LogThrowable} interface.
 */
final class DefaultLogThrowable implements LogThrowable {

  private String className;
  private String message;
  private StackTraceElement[] stackTraces;
  private LogThrowable cause;

  DefaultLogThrowable(IThrowableProxy throwableProxy) {
    this.className = throwableProxy.getClassName();
    this.message = throwableProxy.getMessage();

    StackTraceElementProxy[] stackTraceElementProxyArray = throwableProxy.getStackTraceElementProxyArray();
    this.stackTraces = new StackTraceElement[stackTraceElementProxyArray.length];
    for (int i = 0; i < stackTraceElementProxyArray.length; i++) {
      stackTraces[i] = stackTraceElementProxyArray[i].getStackTraceElement();
    }

    cause = (throwableProxy.getCause() == null) ? null : new DefaultLogThrowable(throwableProxy.getCause());
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public StackTraceElement[] getStackTraces() {
    return stackTraces;
  }

  @Override
  public LogThrowable getCause() {
    return cause;
  }
}
