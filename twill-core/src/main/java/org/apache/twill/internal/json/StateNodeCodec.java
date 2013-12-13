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

import org.apache.twill.api.ServiceController;
import org.apache.twill.internal.state.StateNode;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 *
 */
public final class StateNodeCodec implements JsonSerializer<StateNode>, JsonDeserializer<StateNode> {

  @Override
  public StateNode deserialize(JsonElement json, Type typeOfT,
                               JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    ServiceController.State state = ServiceController.State.valueOf(jsonObj.get("state").getAsString());
    String errorMessage = jsonObj.has("errorMessage") ? jsonObj.get("errorMessage").getAsString() : null;

    return new StateNode(state, errorMessage,
                         context.<StackTraceElement[]>deserialize(jsonObj.get("stackTraces"), StackTraceElement[].class));
  }

  @Override
  public JsonElement serialize(StateNode src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty("state", src.getState().name());
    if (src.getErrorMessage() != null) {
      jsonObj.addProperty("errorMessage", src.getErrorMessage());
    }
    if (src.getStackTraces() != null) {
      jsonObj.add("stackTraces", context.serialize(src.getStackTraces(), StackTraceElement[].class));
    }
    return jsonObj;
  }
}
