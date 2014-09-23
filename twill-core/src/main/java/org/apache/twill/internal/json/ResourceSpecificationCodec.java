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
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.internal.DefaultResourceSpecification;

import java.lang.reflect.Type;
import java.util.List;

/**
 *
 */
final class ResourceSpecificationCodec implements JsonSerializer<ResourceSpecification>,
                                                  JsonDeserializer<ResourceSpecification> {

  @Override
  public JsonElement serialize(ResourceSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("cores", src.getVirtualCores());
    json.addProperty("memorySize", src.getMemorySize());
    json.addProperty("instances", src.getInstances());
    json.addProperty("uplink", src.getUplink());
    json.addProperty("downlink", src.getDownlink());
    return json;
  }

  @Override
  public ResourceSpecification deserialize(JsonElement json, Type typeOfT,
                                           JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    final List<String> hosts = context.deserialize(jsonObj.getAsJsonArray("hosts"), List.class);
    final List<String> racks = context.deserialize(jsonObj.getAsJsonArray("racks"), List.class);
    return new DefaultResourceSpecification(jsonObj.get("cores").getAsInt(),
                                            jsonObj.get("memorySize").getAsInt(),
                                            jsonObj.get("instances").getAsInt(),
                                            jsonObj.get("uplink").getAsInt(),
                                            jsonObj.get("downlink").getAsInt());
  }

}
