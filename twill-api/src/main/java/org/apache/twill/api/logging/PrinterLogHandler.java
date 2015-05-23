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

import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Formatter;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * A {@link LogHandler} that prints the {@link LogEntry} through a {@link PrintWriter}.
 */
public final class PrinterLogHandler implements LogHandler {

  // A regex for splitting string by ".".
  private static final Pattern DOT_SPLIT = Pattern.compile("\\.");

  private static final ThreadLocal<DateFormat> DATE_FORMAT = new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,SSS'Z'");
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      return format;
    }
  };

  private final PrintWriter writer;
  private final Formatter formatter;

  /**
   * Creates a {@link PrinterLogHandler} which has {@link LogEntry} written to the given {@link PrintWriter}.
   * @param writer The write that log entries will write to.
   */
  public PrinterLogHandler(PrintWriter writer) {
    this.writer = writer;
    this.formatter = new Formatter(writer);
  }

  @Override
  public void onLog(LogEntry logEntry) {
    String utc = timestampToUTC(logEntry.getTimestamp());

    formatter.format("%s %-5s %s [%s] [%s] %s:%s(%s:%d) - %s\n",
                     utc,
                     logEntry.getLogLevel().name(),
                     getShortenLoggerName(logEntry.getLoggerName()),
                     logEntry.getHost(),
                     logEntry.getThreadName(),
                     getSimpleClassName(logEntry.getSourceClassName()),
                     logEntry.getSourceMethodName(),
                     logEntry.getFileName(),
                     logEntry.getLineNumber(),
                     logEntry.getMessage());
    formatter.flush();

    // Prints the throwable and stack trace.
    LogThrowable throwable = logEntry.getThrowable();
    while (throwable != null) {
      writer.append(throwable.getClassName()).append(": ").append(throwable.getMessage());
      writer.println();

      StackTraceElement[] stackTraces = throwable.getStackTraces();
      if (stackTraces != null) {
        for (StackTraceElement stackTrace : stackTraces) {
          writer.append("\tat ").append(stackTrace.toString());
          writer.println();
        }
      }

      throwable = throwable.getCause();
      if (throwable != null) {
        writer.append("Caused by: ");
      }
    }

    writer.flush();
  }

  private String timestampToUTC(long timestamp) {
    return DATE_FORMAT.get().format(new Date(timestamp));
  }

  private String getShortenLoggerName(String loggerName) {
    StringBuilder builder = new StringBuilder();
    String previous = null;
    for (String part : DOT_SPLIT.split(loggerName)) {
      if (previous != null) {
        builder.append(previous.charAt(0)).append('.');
      }
      previous = part;
    }
    return builder.append(previous).toString();
  }

  private String getSimpleClassName(String className) {
    return className.substring(className.lastIndexOf('.') + 1);
  }
}
