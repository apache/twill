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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.twill.internal.JvmOptions;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.Set;

/**
 * Gson codec for {@link JvmOptions}.
 */
public class JvmOptionsCodec implements JsonSerializer<JvmOptions>, JsonDeserializer<JvmOptions> {

  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(JvmOptions.class, new JvmOptionsCodec())
                                                    .registerTypeAdapter(JvmOptions.DebugOptions.class,
                                                                         new DebugOptionsCodec())
                                                    .create();

  public static void encode(JvmOptions jvmOptions, OutputSupplier<? extends Writer> writerSupplier) throws IOException {
    try (Writer writer = writerSupplier.getOutput()) {
      GSON.toJson(jvmOptions, writer);
    }
  }

  public static JvmOptions decode(InputSupplier<? extends Reader> readerSupplier) throws IOException {
    try (Reader reader = readerSupplier.getInput()) {
      return GSON.fromJson(reader, JvmOptions.class);
    }
  }

  @Override
  public JvmOptions deserialize(JsonElement json, Type type, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    String extraOptions = context.deserialize(jsonObj.get("extraOptions"), String.class);
    JvmOptions.DebugOptions debugOptions = context.deserialize(jsonObj.get("debugOptions"),
                                                               JvmOptions.DebugOptions.class);
    return new JvmOptions(extraOptions, debugOptions);
  }

  @Override
  public JsonElement serialize(JvmOptions jvmOptions, Type type, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.add("extraOptions", context.serialize(jvmOptions.getExtraOptions()));
    json.add("debugOptions", context.serialize(jvmOptions.getDebugOptions()));
    return json;
  }

  private static class DebugOptionsCodec
    implements JsonSerializer<JvmOptions.DebugOptions>, JsonDeserializer<JvmOptions.DebugOptions> {

    @Override
    public JvmOptions.DebugOptions deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      Boolean doDebug = context.deserialize(jsonObj.get("doDebug"), Boolean.class);
      if (!doDebug) {
        return JvmOptions.DebugOptions.NO_DEBUG;
      }
      Boolean doSuspend = context.deserialize(jsonObj.get("doSuspend"), Boolean.class);
      Set<String> runnables = context.deserialize(jsonObj.get("runnables"),
                                                  new TypeToken<Set<String>>() { }.getType());
      return new JvmOptions.DebugOptions(true, doSuspend, runnables == null ? null : ImmutableSet.copyOf(runnables));
    }

    @Override
    public JsonElement serialize(JvmOptions.DebugOptions src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.add("doDebug", context.serialize(src.doDebug()));
      json.add("doSuspend", context.serialize(src.doSuspend()));
      if (src.getRunnables() != null) {
        json.add("runnables", context.serialize(src.getRunnables()));
      }
      return json;
    }
  }
}


