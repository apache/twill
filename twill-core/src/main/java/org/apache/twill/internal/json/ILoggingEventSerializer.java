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

import ch.qos.logback.classic.pattern.ClassOfCallerConverter;
import ch.qos.logback.classic.pattern.FileOfCallerConverter;
import ch.qos.logback.classic.pattern.LineOfCallerConverter;
import ch.qos.logback.classic.pattern.MethodOfCallerConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.twill.api.logging.LogThrowable;

import java.lang.reflect.Type;

/**
 * Gson serializer for {@link ILoggingEvent}.
 */
public final class ILoggingEventSerializer implements JsonSerializer<ILoggingEvent> {

  private final ClassOfCallerConverter classNameConverter = new ClassOfCallerConverter();
  private final MethodOfCallerConverter methodConverter = new MethodOfCallerConverter();
  private final FileOfCallerConverter fileConverter = new FileOfCallerConverter();
  private final LineOfCallerConverter lineConverter = new LineOfCallerConverter();
  private final String hostname;
  private final String runnableName;

  public ILoggingEventSerializer(String hostname, String runnableName) {
    this.hostname = hostname;
    this.runnableName = runnableName;
  }

  @Override
  public JsonElement serialize(ILoggingEvent event, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("name", event.getLoggerName());
    json.addProperty("host", hostname);
    json.addProperty("timestamp", Long.toString(event.getTimeStamp()));
    json.addProperty("level", event.getLevel().toString());
    json.addProperty("className", classNameConverter.convert(event));
    json.addProperty("method", methodConverter.convert(event));
    json.addProperty("file", fileConverter.convert(event));
    json.addProperty("line", lineConverter.convert(event));
    json.addProperty("thread", event.getThreadName());
    json.addProperty("message", event.getFormattedMessage());
    json.addProperty("runnableName", runnableName);
    if (event.getThrowableProxy() == null) {
      json.add("throwable", JsonNull.INSTANCE);
    } else {
      json.add("throwable", context.serialize(new DefaultLogThrowable(event.getThrowableProxy()), LogThrowable.class));
    }

    return json;
  }
}
