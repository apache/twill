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
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.internal.DefaultResourceReport;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Codec for serializing and deserializing a {@link ResourceReport} object using json.
 */
public final class ResourceReportCodec implements JsonSerializer<ResourceReport>,
                                           JsonDeserializer<ResourceReport> {

  @Override
  public JsonElement serialize(ResourceReport src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();

    json.addProperty("appMasterId", src.getApplicationId());
    json.add("appMasterResources", context.serialize(src.getAppMasterResources(), TwillRunResources.class));
    json.add("runnableResources", context.serialize(
      src.getResources(), new TypeToken<Map<String, Collection<TwillRunResources>>>() { }.getType()));
    json.add("services", context.serialize(
      src.getServices(), new TypeToken<List<String>>() { }.getType()));
    return json;
  }

  @Override
  public ResourceReport deserialize(JsonElement json, Type typeOfT,
                                           JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    String appMasterId = jsonObj.get("appMasterId").getAsString();
    TwillRunResources masterResources = context.deserialize(
      jsonObj.get("appMasterResources"), TwillRunResources.class);
    Map<String, Collection<TwillRunResources>> resources = context.deserialize(
      jsonObj.get("runnableResources"), new TypeToken<Map<String, Collection<TwillRunResources>>>() { }.getType());
    List<String> services = context.deserialize(
      jsonObj.get("services"), new TypeToken<List<String>>() { }.getType());

    return new DefaultResourceReport(appMasterId, masterResources, resources, services);
  }
}
