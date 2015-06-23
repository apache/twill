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
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.internal.DefaultTwillRunResources;

import java.lang.reflect.Type;

/**
 * Codec for serializing and deserializing a {@link org.apache.twill.api.TwillRunResources} object using json.
 */
public final class TwillRunResourcesCodec implements JsonSerializer<TwillRunResources>,
                                              JsonDeserializer<TwillRunResources> {
  private final String CONTAINER_ID = "containerId";
  private final String INSTANCE_ID = "instanceId";
  private final String HOST = "host";
  private final String MEMORY_MB = "memoryMB";
  private final String VIRTUAL_CORES = "virtualCores";
  private final String DEBUG_PORT = "debugPort";
  private final String LOG_LEVEL = "logLevel";

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
    if (src.getLogLevel() != null) {
      json.addProperty(LOG_LEVEL, src.getLogLevel().toString());
    }

    return json;
  }

  @Override
  public TwillRunResources deserialize(JsonElement json, Type typeOfT,
                                           JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    return new DefaultTwillRunResources(jsonObj.get("instanceId").getAsInt(),
                                        jsonObj.get("containerId").getAsString(),
                                        jsonObj.get("virtualCores").getAsInt(),
                                        jsonObj.get("memoryMB").getAsInt(),
                                        jsonObj.get("host").getAsString(),
                                        jsonObj.has("debugPort") ? jsonObj.get("debugPort").getAsInt() : null,
                                        jsonObj.has("logLevel") ? LogEntry.Level.valueOf(
                                          jsonObj.get("logLevel").getAsString()) : LogEntry.Level.INFO);
  }
}
