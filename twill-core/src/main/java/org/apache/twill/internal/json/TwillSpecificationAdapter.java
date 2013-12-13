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
import org.apache.twill.internal.json.TwillSpecificationCodec.EventHandlerSpecificationCoder;
import org.apache.twill.internal.json.TwillSpecificationCodec.TwillSpecificationOrderCoder;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 */
public final class TwillSpecificationAdapter {

  private final Gson gson;

  public static TwillSpecificationAdapter create() {
    return new TwillSpecificationAdapter();
  }

  private TwillSpecificationAdapter() {
    gson = new GsonBuilder()
              .serializeNulls()
              .registerTypeAdapter(TwillSpecification.class, new TwillSpecificationCodec())
              .registerTypeAdapter(TwillSpecification.Order.class, new TwillSpecificationOrderCoder())
              .registerTypeAdapter(EventHandlerSpecification.class, new EventHandlerSpecificationCoder())
              .registerTypeAdapter(RuntimeSpecification.class, new RuntimeSpecificationCodec())
              .registerTypeAdapter(TwillRunnableSpecification.class, new TwillRunnableSpecificationCodec())
              .registerTypeAdapter(ResourceSpecification.class, new ResourceSpecificationCodec())
              .registerTypeAdapter(LocalFile.class, new LocalFileCodec())
              .registerTypeAdapterFactory(new TwillSpecificationTypeAdapterFactory())
              .create();
  }

  public String toJson(TwillSpecification spec) {
    return gson.toJson(spec, TwillSpecification.class);
  }

  public void toJson(TwillSpecification spec, Writer writer) {
    gson.toJson(spec, TwillSpecification.class, writer);
  }

  public void toJson(TwillSpecification spec, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      toJson(spec, writer);
    } finally {
      writer.close();
    }
  }

  public TwillSpecification fromJson(String json) {
    return gson.fromJson(json, TwillSpecification.class);
  }

  public TwillSpecification fromJson(Reader reader) {
    return gson.fromJson(reader, TwillSpecification.class);
  }

  public TwillSpecification fromJson(File file) throws IOException {
    Reader reader = Files.newReader(file, Charsets.UTF_8);
    try {
      return fromJson(reader);
    } finally {
      reader.close();
    }
  }

  // This is to get around gson ignoring of inner class
  private static final class TwillSpecificationTypeAdapterFactory implements TypeAdapterFactory {

    @Override
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
