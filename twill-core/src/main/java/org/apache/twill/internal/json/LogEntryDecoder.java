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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogThrowable;

import java.lang.reflect.Type;

/**
 * A Gson decoder for {@link LogEntry}.
 */
public final class LogEntryDecoder implements JsonDeserializer<LogEntry> {

  private static final StackTraceElement[] EMPTY_STACK_TRACES = new StackTraceElement[0];

  @Override
  public LogEntry deserialize(JsonElement json, Type typeOfT,
                              JsonDeserializationContext context) throws JsonParseException {
    if (!json.isJsonObject()) {
      return null;
    }
    JsonObject jsonObj = json.getAsJsonObject();

    final String name = JsonUtils.getAsString(jsonObj, "name");
    final String host = JsonUtils.getAsString(jsonObj, "host");
    final long timestamp = JsonUtils.getAsLong(jsonObj, "timestamp", 0);
    final LogEntry.Level logLevel = LogEntry.Level.valueOf(JsonUtils.getAsString(jsonObj, "level"));
    final String className = JsonUtils.getAsString(jsonObj, "className");
    final String method = JsonUtils.getAsString(jsonObj, "method");
    final String file = JsonUtils.getAsString(jsonObj, "file");
    final String line = JsonUtils.getAsString(jsonObj, "line");
    final String thread = JsonUtils.getAsString(jsonObj, "thread");
    final String message = JsonUtils.getAsString(jsonObj, "message");
    final String runnableName = JsonUtils.getAsString(jsonObj, "runnableName");
    final LogThrowable logThrowable = context.deserialize(jsonObj.get("throwable"), LogThrowable.class);

    return new LogEntry() {
      @Override
      public String getLoggerName() {
        return name;
      }

      @Override
      public String getHost() {
        return host;
      }

      @Override
      public long getTimestamp() {
        return timestamp;
      }

      @Override
      public Level getLogLevel() {
        return logLevel;
      }

      @Override
      public String getSourceClassName() {
        return className;
      }

      @Override
      public String getSourceMethodName() {
        return method;
      }

      @Override
      public String getFileName() {
        return file;
      }

      @Override
      public int getLineNumber() {
        if (line.equals("?")) {
          return -1;
        } else {
          return Integer.parseInt(line);
        }
      }

      @Override
      public String getThreadName() {
        return thread;
      }

      @Override
      public String getMessage() {
        return message;
      }

      @Override
      public String getRunnableName() {
        return runnableName;
      }

      @Override
      public LogThrowable getThrowable() {
        return logThrowable;
      }

      @Override
      public StackTraceElement[] getStackTraces() {
        LogThrowable throwable = getThrowable();
        return (throwable == null) ? EMPTY_STACK_TRACES : throwable.getStackTraces();
      }
    };
  }
}
