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

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;

/**
 * A adapter interface for the LocalResource class/interface in different Hadoop version.
 */
public interface YarnLocalResource {

  /**
   * Returns the actual LocalResource object in Yarn.
   */
  <T> T getLocalResource();

  /**
   * Get the <em>location</em> of the resource to be localized.
   * @return <em>location</em> of the resource to be localized
   */
  URL getResource();

  /**
   * Set <em>location</em> of the resource to be localized.
   * @param resource <em>location</em> of the resource to be localized
   */
  void setResource(URL resource);

  /**
   * Get the <em>size</em> of the resource to be localized.
   * @return <em>size</em> of the resource to be localized
   */
  long getSize();

  /**
   * Set the <em>size</em> of the resource to be localized.
   * @param size <em>size</em> of the resource to be localized
   */
  void setSize(long size);

  /**
   * Get the original <em>timestamp</em> of the resource to be localized, used
   * for verification.
   * @return <em>timestamp</em> of the resource to be localized
   */
  long getTimestamp();

  /**
   * Set the <em>timestamp</em> of the resource to be localized, used
   * for verification.
   * @param timestamp <em>timestamp</em> of the resource to be localized
   */
  void setTimestamp(long timestamp);

  /**
   * Get the <code>LocalResourceType</code> of the resource to be localized.
   * @return <code>LocalResourceType</code> of the resource to be localized
   */
  LocalResourceType getType();

  /**
   * Set the <code>LocalResourceType</code> of the resource to be localized.
   * @param type <code>LocalResourceType</code> of the resource to be localized
   */
  void setType(LocalResourceType type);

  /**
   * Get the <code>LocalResourceVisibility</code> of the resource to be
   * localized.
   * @return <code>LocalResourceVisibility</code> of the resource to be
   *         localized
   */
  LocalResourceVisibility getVisibility();

  /**
   * Set the <code>LocalResourceVisibility</code> of the resource to be
   * localized.
   * @param visibility <code>LocalResourceVisibility</code> of the resource to be
   *                   localized
   */
  void setVisibility(LocalResourceVisibility visibility);

  /**
   * Get the <em>pattern</em> that should be used to extract entries from the
   * archive (only used when type is <code>PATTERN</code>).
   * @return <em>pattern</em> that should be used to extract entries from the
   * archive.
   */
  String getPattern();

  /**
   * Set the <em>pattern</em> that should be used to extract entries from the
   * archive (only used when type is <code>PATTERN</code>).
   * @param pattern <em>pattern</em> that should be used to extract entries
   * from the archive.
   */
  void setPattern(String pattern);
}
