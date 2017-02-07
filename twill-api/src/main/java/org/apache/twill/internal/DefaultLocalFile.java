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
package org.apache.twill.internal;

import org.apache.twill.api.LocalFile;

import java.net.URI;
import javax.annotation.Nullable;

/**
 * A straightforward implementation of {@link LocalFile}.
 */
public final class DefaultLocalFile implements LocalFile {

  private final String name;
  private final URI uri;
  private final long lastModified;
  private final long size;
  private final boolean archive;
  private final String pattern;

  public DefaultLocalFile(String name, URI uri, long lastModified,
                          long size, boolean archive, @Nullable String pattern) {
    this.name = name;
    this.uri = uri;
    this.lastModified = lastModified;
    this.size = size;
    this.archive = archive;
    this.pattern = pattern;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public URI getURI() {
    return uri;
  }

  @Override
  public long getLastModified() {
    return lastModified;
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public boolean isArchive() {
    return archive;
  }

  @Override
  public String getPattern() {
    return pattern;
  }

  @Override
  public String toString() {
    return "DefaultLocalFile{" +
      "name='" + name + '\'' +
      ", uri=" + uri +
      ", lastModified=" + lastModified +
      ", size=" + size +
      ", archive=" + archive +
      ", pattern='" + pattern + '\'' +
      '}';
  }
}
