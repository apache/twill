/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.yarn;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.twill.api.LocalFile;
import org.apache.twill.internal.appmaster.ApplicationMasterLiveNodeData;
import org.apache.twill.internal.json.LocalFileCodec;
import org.apache.twill.zookeeper.NodeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * A package local class to help decoding {@link NodeData} to {@link ApplicationMasterLiveNodeDecoder}.
 * This is only used to have a central for the decoding logic, that is shared between {@link YarnTwillRunnerService}
 * and {@link YarnTwillController}.
 */
final class ApplicationMasterLiveNodeDecoder {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterLiveNodeDecoder.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(LocalFile.class, new LocalFileCodec())
    .create();


  /**
   * Decodes the {@link ApplicationMasterLiveNodeData} from the given ZK node data.
   *
   * @return the {@link ApplicationMasterLiveNodeData} or {@code null} if failed to decode.
   */
  @Nullable
  static ApplicationMasterLiveNodeData decode(@Nullable NodeData nodeData) {
    byte[] data = nodeData == null ? null : nodeData.getData();
    if (data == null) {
      return null;
    }

    JsonElement json = GSON.fromJson(new String(data, Charsets.UTF_8), JsonElement.class);
    if (!json.isJsonObject()) {
      LOG.warn("Unable to decode live data node.");
      return null;
    }

    JsonObject jsonObj = json.getAsJsonObject();
    json = jsonObj.get("data");
    if (json == null || !json.isJsonObject()) {
      LOG.warn("Property data not found in live data node.");
      return null;
    }

    try {
      return GSON.fromJson(json, ApplicationMasterLiveNodeData.class);
    } catch (Exception e) {
      LOG.warn("Failed to decode application live node data.", e);
      return null;
    }
  }

  private ApplicationMasterLiveNodeDecoder() {
    // no-op
  }
}
