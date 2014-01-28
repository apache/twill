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
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.internal.DefaultTwillRunnableSpecification;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Gson codec for {@link TwillRunnableSpecification}.
 */
final class TwillRunnableSpecificationCodec implements JsonSerializer<TwillRunnableSpecification>,
                                                       JsonDeserializer<TwillRunnableSpecification> {

  @Override
  public JsonElement serialize(TwillRunnableSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();

    json.addProperty("classname", src.getClassName());
    json.addProperty("name", src.getName());
    json.add("arguments", context.serialize(src.getConfigs(), new TypeToken<Map<String, String>>() { }.getType()));

    return json;
  }

  @Override
  public TwillRunnableSpecification deserialize(JsonElement json, Type typeOfT,
                                                JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("classname").getAsString();
    String name = jsonObj.get("name").getAsString();
    Map<String, String> arguments = context.deserialize(jsonObj.get("arguments"),
                                                        new TypeToken<Map<String, String>>() { }.getType());

    return new DefaultTwillRunnableSpecification(className, name, arguments);
  }
}
