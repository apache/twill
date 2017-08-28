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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillRunResources;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * This class provides utility to help encode/decode {@link ResourceReport} to/from Json.
 */
public final class ResourceReportAdapter {

  public static final Type RUNNABLES_RESOURCES_TYPE =
    new TypeToken<Map<String, Collection<TwillRunResources>>>() { }.getType();
  public static final Type INSTANCES_RESOURCES_TYPE =
    new TypeToken<Collection<TwillRunResources>>() { }.getType();

  public static final Gson GSON = new GsonBuilder()
    .serializeNulls()
    .setPrettyPrinting()
    .registerTypeAdapter(TwillRunResources.class, new TwillRunResourcesCodec())
    .registerTypeAdapter(ResourceReport.class, new ResourceReportCodec())
    .registerTypeAdapterFactory(new TypeAdapterFactory() {
      @SuppressWarnings("unchecked")
      @Override
      public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
        Class<? super T> rawType = type.getRawType();

        // For any sub-type of ResourceReport, use the TypeAdapter of ResourceReport
        // For any sub-type of TwillRunResources, use the TypeAdapter of TwillRunResources
        for (Class<?> cls : Arrays.asList(ResourceReport.class, TwillRunResources.class)) {
          if (!cls.equals(rawType) && cls.isAssignableFrom(rawType)) {
            return (TypeAdapter<T>) gson.getAdapter(cls);
          }
        }

        // For all other types, use the default
        return null;
      }
    })
    .create();
}
