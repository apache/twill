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

import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.TwillRuntimeSpecification;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.Map;

/**
 * Codec for serializing and deserializing a {@link TwillRuntimeSpecification} object using json.
 */
final class TwillRuntimeSpecificationCodec implements JsonSerializer<TwillRuntimeSpecification>,
                                                         JsonDeserializer<TwillRuntimeSpecification> {

  private static final String FS_USER = "fsUser";
  private static final String TWILL_APP_DIR = "twillAppDir";
  private static final String ZK_CONNECT_STR = "zkConnectStr";
  private static final String TWILL_RUNID = "twillRunId";
  private static final String TWILL_APP_NAME = "twillAppName";
  private static final String RESERVED_MEMORY = "reservedMemory";
  private static final String RM_SCHEDULER_ADDR = "rmSchedulerAddr";
  private static final String TWILL_SPEC = "twillSpecification";
  private static final String LOG_LEVELS = "logLevels";

  @Override
  public JsonElement serialize(TwillRuntimeSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty(FS_USER, src.getFsUser());
    json.addProperty(TWILL_APP_DIR, src.getTwillAppDir().toASCIIString());
    json.addProperty(ZK_CONNECT_STR, src.getZkConnectStr());
    json.addProperty(TWILL_RUNID, src.getTwillAppRunId().getId());
    json.addProperty(TWILL_APP_NAME, src.getTwillAppName());
    json.addProperty(RESERVED_MEMORY, src.getReservedMemory());
    if (src.getRmSchedulerAddr() != null) {
      json.addProperty(RM_SCHEDULER_ADDR, src.getRmSchedulerAddr());
    }
    json.add(TWILL_SPEC,
             context.serialize(src.getTwillSpecification(), new TypeToken<TwillSpecification>() { }.getType()));
    json.add(LOG_LEVELS,
             context.serialize(src.getLogLevels(), new TypeToken<Map<String, Map<String, String>>>() { }.getType()));
    return json;
  }

  @Override
  public TwillRuntimeSpecification deserialize(JsonElement json, Type typeOfT,
                                               JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    TwillSpecification twillSpecification = context.deserialize(
      jsonObj.get(TWILL_SPEC), new TypeToken<TwillSpecification>() { }.getType());
    Map<String, Map<String, String>> logLevels =
      context.deserialize(jsonObj.get(LOG_LEVELS), new TypeToken<Map<String, Map<String, String>>>() { }.getType());
    return new TwillRuntimeSpecification(twillSpecification,
                                         jsonObj.get(FS_USER).getAsString(),
                                         URI.create(jsonObj.get(TWILL_APP_DIR).getAsString()),
                                         jsonObj.get(ZK_CONNECT_STR).getAsString(),
                                         RunIds.fromString(jsonObj.get(TWILL_RUNID).getAsString()),
                                         jsonObj.get(TWILL_APP_NAME).getAsString(),
                                         jsonObj.get(RESERVED_MEMORY).getAsInt(),
                                         jsonObj.has(RM_SCHEDULER_ADDR) ?
                                           jsonObj.get(RM_SCHEDULER_ADDR).getAsString() : null,
                                         logLevels);
  }
}
