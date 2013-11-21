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
package org.apache.twill.internal.yarn;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.Records;

/**
 *
 */
public final class Hadoop21YarnLocalResource implements YarnLocalResource {

  private final LocalResource localResource;

  public Hadoop21YarnLocalResource() {
    this.localResource = Records.newRecord(LocalResource.class);
  }

  @Override
  public <T> T getLocalResource() {
    return (T) localResource;
  }

  @Override
  public URL getResource() {
    return localResource.getResource();
  }

  @Override
  public void setResource(URL resource) {
    localResource.setResource(resource);
  }

  @Override
  public long getSize() {
    return localResource.getSize();
  }

  @Override
  public void setSize(long size) {
    localResource.setSize(size);
  }

  @Override
  public long getTimestamp() {
    return localResource.getTimestamp();
  }

  @Override
  public void setTimestamp(long timestamp) {
    localResource.setTimestamp(timestamp);
  }

  @Override
  public LocalResourceType getType() {
    return localResource.getType();
  }

  @Override
  public void setType(LocalResourceType type) {
    localResource.setType(type);
  }

  @Override
  public LocalResourceVisibility getVisibility() {
    return localResource.getVisibility();
  }

  @Override
  public void setVisibility(LocalResourceVisibility visibility) {
    localResource.setVisibility(visibility);
  }

  @Override
  public String getPattern() {
    return localResource.getPattern();
  }

  @Override
  public void setPattern(String pattern) {
    localResource.setPattern(pattern);
  }
}
