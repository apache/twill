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

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.json.TwillSpecificationCodec.EventHandlerSpecificationCoder;
import org.apache.twill.internal.json.TwillSpecificationCodec.TwillSpecificationOrderCoder;
import org.apache.twill.internal.json.TwillSpecificationCodec.TwillSpecificationPlacementPolicyCoder;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public final class TwillRuntimeSpecificationAdapter {

  private final Gson gson;

  public static TwillRuntimeSpecificationAdapter create() {
    return new TwillRuntimeSpecificationAdapter();
  }

  private TwillRuntimeSpecificationAdapter() {
    gson = new GsonBuilder()
              .serializeNulls()
              .registerTypeAdapter(TwillRuntimeSpecification.class, new TwillRuntimeSpecificationCodec())
              .registerTypeAdapter(TwillSpecification.class, new TwillSpecificationCodec())
              .registerTypeAdapter(TwillSpecification.Order.class, new TwillSpecificationOrderCoder())
              .registerTypeAdapter(TwillSpecification.PlacementPolicy.class,
                                   new TwillSpecificationPlacementPolicyCoder())
              .registerTypeAdapter(EventHandlerSpecification.class, new EventHandlerSpecificationCoder())
              .registerTypeAdapter(RuntimeSpecification.class, new RuntimeSpecificationCodec())
              .registerTypeAdapter(TwillRunnableSpecification.class, new TwillRunnableSpecificationCodec())
              .registerTypeAdapter(ResourceSpecification.class, new ResourceSpecificationCodec())
              .registerTypeAdapter(LocalFile.class, new LocalFileCodec())
              .registerTypeAdapterFactory(new TwillSpecificationTypeAdapterFactory())
              .create();
  }

  public String toJson(TwillRuntimeSpecification spec) {
    return gson.toJson(spec, TwillRuntimeSpecification.class);
  }

  public void toJson(TwillRuntimeSpecification spec, Writer writer) {
    gson.toJson(spec, TwillRuntimeSpecification.class, writer);
  }

  public void toJson(TwillRuntimeSpecification spec, File file) throws IOException {
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      toJson(spec, writer);
    }
  }

  public TwillRuntimeSpecification fromJson(String json) {
    return gson.fromJson(json, TwillRuntimeSpecification.class);
  }

  public TwillRuntimeSpecification fromJson(Reader reader) {
    return gson.fromJson(reader, TwillRuntimeSpecification.class);
  }

  public TwillRuntimeSpecification fromJson(File file) throws IOException {
    try (Reader reader = Files.newReader(file, Charsets.UTF_8)) {
      return fromJson(reader);
    }
  }

  // This is to get around gson ignoring of inner class
  private static final class TwillSpecificationTypeAdapterFactory implements TypeAdapterFactory {

    @SuppressWarnings("unchecked")
    @Override
    @Nullable
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
      Class<?> rawType = type.getRawType();
      if (!Map.class.isAssignableFrom(rawType)) {
        return null;
      }
      Type[] typeArgs = ((ParameterizedType) type.getType()).getActualTypeArguments();
      TypeToken<?> keyType = TypeToken.get(typeArgs[0]);
      TypeToken<?> valueType = TypeToken.get(typeArgs[1]);
      if (keyType.getRawType() != String.class) {
        return null;
      }
      return (TypeAdapter<T>) mapAdapter(gson, valueType);
    }

    private <V> TypeAdapter<Map<String, V>> mapAdapter(Gson gson, TypeToken<V> valueType) {
      final TypeAdapter<V> valueAdapter = gson.getAdapter(valueType);

      return new TypeAdapter<Map<String, V>>() {
        @Override
        public void write(JsonWriter writer, Map<String, V> map) throws IOException {
          if (map == null) {
            writer.nullValue();
            return;
          }
          writer.beginObject();
          for (Map.Entry<String, V> entry : map.entrySet()) {
            writer.name(entry.getKey());
            valueAdapter.write(writer, entry.getValue());
          }
          writer.endObject();
        }

        @Override
        @Nullable
        public Map<String, V> read(JsonReader reader) throws IOException {
          if (reader.peek() == JsonToken.NULL) {
            reader.nextNull();
            return null;
          }
          if (reader.peek() != JsonToken.BEGIN_OBJECT) {
            return null;
          }
          Map<String, V> map = Maps.newHashMap();
          reader.beginObject();
          while (reader.peek() != JsonToken.END_OBJECT) {
            map.put(reader.nextName(), valueAdapter.read(reader));
          }
          reader.endObject();
          return map;
        }
      };
    }
  }
}
