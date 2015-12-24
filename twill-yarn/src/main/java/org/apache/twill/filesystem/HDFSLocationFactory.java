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
package org.apache.twill.filesystem;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

/**
 * A {@link LocationFactory} that creates HDFS {@link Location} using {@link FileSystem}.
 *
 * @deprecated Deprecated since 0.7.0. Use {@link FileContextLocationFactory} instead.
 */
@Deprecated
public final class HDFSLocationFactory implements LocationFactory {

  private final FileSystem fileSystem;
  private final String pathBase;

  public HDFSLocationFactory(Configuration configuration) {
    this(getFileSystem(configuration));
  }
  
  public HDFSLocationFactory(Configuration configuration, String pathBase) {
    this(getFileSystem(configuration), pathBase);
  }

  public HDFSLocationFactory(FileSystem fileSystem) {
    this(fileSystem, "/");
  }

  public HDFSLocationFactory(FileSystem fileSystem, String pathBase) {
    String base = pathBase.equals("/") ? "" : pathBase;
    base = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;

    this.fileSystem = fileSystem;
    this.pathBase = base;
  }

  @Override
  public Location create(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return new HDFSLocation(this, new Path(fileSystem.getUri() + "/" + pathBase + "/" + path));
  }

  @Override
  public Location create(URI uri) {
    URI fsURI = fileSystem.getUri();
    if (Objects.equals(fsURI.getScheme(), uri.getScheme())
      && Objects.equals(fsURI.getAuthority(), uri.getAuthority())) {
      // It's a full URI
      return new HDFSLocation(this, new Path(uri));
    }

    if (uri.isAbsolute()) {
      // Needs to be of the same scheme
      Preconditions.checkArgument(Objects.equals(fsURI.getScheme(), uri.getScheme()),
                                  "Only URI with '%s' scheme is supported", fsURI.getScheme());
      return new HDFSLocation(this, new Path(fileSystem.getUri() + uri.getPath()));
    }

    return create(uri.getPath());
  }

  @Override
  public Location getHomeLocation() {
    return new HDFSLocation(this, fileSystem.getHomeDirectory());
  }

  /**
   * Returns the underlying {@link FileSystem} object.
   */
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  private static FileSystem getFileSystem(Configuration configuration) {
    try {
      return FileSystem.get(configuration);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
