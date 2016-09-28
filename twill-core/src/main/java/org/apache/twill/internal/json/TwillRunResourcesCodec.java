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
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.internal.DefaultTwillRunResources;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Codec for serializing and deserializing a {@link org.apache.twill.api.TwillRunResources} object using json.
 */
public final class TwillRunResourcesCodec implements JsonSerializer<TwillRunResources>,
                                              JsonDeserializer<TwillRunResources> {
  private static final String CONTAINER_ID = "containerId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String HOST = "host";
  private static final String MEMORY_MB = "memoryMB";
  private static final String VIRTUAL_CORES = "virtualCores";
  private static final String DEBUG_PORT = "debugPort";
  private static final String LOG_LEVEL = "rootLogLevel";
  private static final String LOG_LEVEL_ARGUMENTS = "logLevelArguments";

  @Override
  public JsonElement serialize(TwillRunResources src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();

    json.addProperty(CONTAINER_ID, src.getContainerId());
    json.addProperty(INSTANCE_ID, src.getInstanceId());
    json.addProperty(HOST, src.getHost());
    json.addProperty(MEMORY_MB, src.getMemoryMB());
    json.addProperty(VIRTUAL_CORES, src.getVirtualCores());
    if (src.getDebugPort() != null) {
      json.addProperty(DEBUG_PORT, src.getDebugPort());
    }
    if (src.getRootLogLevel() != null) {
      json.addProperty(LOG_LEVEL, src.getRootLogLevel().toString());
    }
    json.add(LOG_LEVEL_ARGUMENTS, context.serialize(src.getLogLevelArguments(),
                                                    new TypeToken<Map<String, String>>() { }.getType()));

    return json;
  }

  @Override
  public TwillRunResources deserialize(JsonElement json, Type typeOfT,
                                           JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    Map<String, LogEntry.Level> logLevelArguments =
      context.deserialize(jsonObj.get("logLevelArguments"), new TypeToken<Map<String, LogEntry.Level>>() { }.getType());
    return new DefaultTwillRunResources(jsonObj.get("instanceId").getAsInt(),
                                        jsonObj.get("containerId").getAsString(),
                                        jsonObj.get("virtualCores").getAsInt(),
                                        jsonObj.get("memoryMB").getAsInt(),
                                        jsonObj.get("host").getAsString(),
                                        jsonObj.has("debugPort") ? jsonObj.get("debugPort").getAsInt() : null,
                                        jsonObj.has("rootLogLevel") ? LogEntry.Level.valueOf(
                                          jsonObj.get("rootLogLevel").getAsString()) : LogEntry.Level.INFO,
                                        logLevelArguments);
  }
}
