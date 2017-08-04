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
  private static final String MAX_HEAP_MEMORY_MB = "maxHeapMemoryMB";
  private static final String VIRTUAL_CORES = "virtualCores";
  private static final String DEBUG_PORT = "debugPort";
  private static final String LOG_LEVELS = "logLevels";

  @Override
  public JsonElement serialize(TwillRunResources src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();

    json.addProperty(CONTAINER_ID, src.getContainerId());
    json.addProperty(INSTANCE_ID, src.getInstanceId());
    json.addProperty(HOST, src.getHost());
    json.addProperty(MEMORY_MB, src.getMemoryMB());
    json.addProperty(MAX_HEAP_MEMORY_MB, src.getMaxHeapMemoryMB());
    json.addProperty(VIRTUAL_CORES, src.getVirtualCores());
    if (src.getDebugPort() != null) {
      json.addProperty(DEBUG_PORT, src.getDebugPort());
    }
    json.add(LOG_LEVELS, context.serialize(src.getLogLevels(),
                                           new TypeToken<Map<String, LogEntry.Level>>() { }.getType()));

    return json;
  }

  @Override
  public TwillRunResources deserialize(JsonElement json, Type typeOfT,
                                           JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    Map<String, LogEntry.Level> logLevels =
      context.deserialize(jsonObj.get(LOG_LEVELS), new TypeToken<Map<String, LogEntry.Level>>() { }.getType());
    int memoryMB = jsonObj.get(MEMORY_MB).getAsInt();
    return new DefaultTwillRunResources(
      jsonObj.get(INSTANCE_ID).getAsInt(),
      jsonObj.get(CONTAINER_ID).getAsString(),
      jsonObj.get(VIRTUAL_CORES).getAsInt(),
      memoryMB,
      // For backward compatibility when a newer Twill client re-attached to running app started with older version.
      jsonObj.has(MAX_HEAP_MEMORY_MB) ? jsonObj.get(MAX_HEAP_MEMORY_MB).getAsInt() : memoryMB,
      jsonObj.get(HOST).getAsString(),
      jsonObj.has(DEBUG_PORT) ? jsonObj.get(DEBUG_PORT).getAsInt() : null,
      logLevels);
  }
}
