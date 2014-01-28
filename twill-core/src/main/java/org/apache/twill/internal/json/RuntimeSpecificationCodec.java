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
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.internal.DefaultRuntimeSpecification;

import java.lang.reflect.Type;
import java.util.Collection;

/**
 * Gson codec for {@link RuntimeSpecification}.
 */
final class RuntimeSpecificationCodec implements JsonSerializer<RuntimeSpecification>,
                                                 JsonDeserializer<RuntimeSpecification> {

  @Override
  public JsonElement serialize(RuntimeSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("name", src.getName());
    json.add("runnable", context.serialize(src.getRunnableSpecification(), TwillRunnableSpecification.class));
    json.add("resources", context.serialize(src.getResourceSpecification(), ResourceSpecification.class));
    json.add("files", context.serialize(src.getLocalFiles(), new TypeToken<Collection<LocalFile>>() { }.getType()));

    return json;
  }

  @Override
  public RuntimeSpecification deserialize(JsonElement json, Type typeOfT,
                                          JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String name = jsonObj.get("name").getAsString();
    TwillRunnableSpecification runnable = context.deserialize(jsonObj.get("runnable"),
                                                               TwillRunnableSpecification.class);
    ResourceSpecification resources = context.deserialize(jsonObj.get("resources"),
                                                          ResourceSpecification.class);
    Collection<LocalFile> files = context.deserialize(jsonObj.get("files"),
                                                      new TypeToken<Collection<LocalFile>>() { }.getType());

    return new DefaultRuntimeSpecification(name, runnable, resources, files);
  }
}
