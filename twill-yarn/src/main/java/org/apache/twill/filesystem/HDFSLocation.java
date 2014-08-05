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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.UUID;

/**
 * A concrete implementation of {@link Location} for the HDFS filesystem.
 */
final class HDFSLocation implements Location {
  private final FileSystem fs;
  private final Path path;
  private final HDFSLocationFactory locationFactory;

  /**
   * Constructs a HDFSLocation.
   *
   * @param locationFactory The {@link HDFSLocationFactory} that creates this instance.
   * @param path of the file.
   */
  HDFSLocation(HDFSLocationFactory locationFactory, Path path) {
    this.fs = locationFactory.getFileSystem();
    this.path = path;
    this.locationFactory = locationFactory;
  }

  /**
   * Checks if this location exists on HDFS.
   *
   * @return true if found; false otherwise.
   * @throws IOException
   */
  @Override
  public boolean exists() throws IOException {
    return fs.exists(path);
  }

  /**
   * @return An {@link InputStream} for this location on HDFS.
   * @throws IOException
   */
  @Override
  public InputStream getInputStream() throws IOException {
    return fs.open(path);
  }

  /**
   * @return An {@link OutputStream} for this location on HDFS.
   * @throws IOException
   */
  @Override
  public OutputStream getOutputStream() throws IOException {
    return fs.create(path);
  }

  @Override
  public OutputStream getOutputStream(String permission) throws IOException {
    Configuration conf = fs.getConf();
    return fs.create(path,
                     new FsPermission(permission),
                     true,
                     conf.getInt("io.file.buffer.size", 4096),
                     fs.getDefaultReplication(path),
                     fs.getDefaultBlockSize(path),
                     null);
  }

  /**
   * Appends the child to the current {@link Location} on HDFS.
   * <p>
   * Returns a new instance of Location.
   * </p>
   *
   * @param child to be appended to this location.
   * @return A new instance of {@link Location}
   * @throws IOException
   */
  @Override
  public Location append(String child) throws IOException {
    if (child.startsWith("/")) {
      child = child.substring(1);
    }
    return new HDFSLocation(locationFactory, new Path(URI.create(path.toUri() + "/" + child)));
  }

  @Override
  public Location getTempFile(String suffix) throws IOException {
    Path path = new Path(
      URI.create(this.path.toUri() + "." + UUID.randomUUID() + (suffix == null ? TEMP_FILE_SUFFIX : suffix)));
    return new HDFSLocation(locationFactory, path);
  }

  /**
   * @return Returns the name of the file or directory denoteed by this abstract pathname.
   */
  @Override
  public String getName() {
    return path.getName();
  }

  @Override
  public boolean createNew() throws IOException {
    return fs.createNewFile(path);
  }

  /**
   * @return A {@link URI} for this location on HDFS.
   */
  @Override
  public URI toURI() {
    return path.toUri();
  }

  /**
   * Deletes the file or directory denoted by this abstract pathname. If this
   * pathname denotes a directory, then the directory must be empty in order
   * to be deleted.
   *
   * @return true if and only if the file or directory is successfully deleted; false otherwise.
   */
  @Override
  public boolean delete() throws IOException {
    return fs.delete(path, false);
  }

  @Override
  public boolean delete(boolean recursive) throws IOException {
    return fs.delete(path, true);
  }

  @Override
  public Location renameTo(Location destination) throws IOException {
    // Destination will always be of the same type as this location.
    if (fs instanceof DistributedFileSystem) {
      ((DistributedFileSystem) fs).rename(path, ((HDFSLocation) destination).path, Options.Rename.OVERWRITE);
      return new HDFSLocation(locationFactory, new Path(destination.toURI()));
    }

    if (fs.rename(path, ((HDFSLocation) destination).path)) {
      return new HDFSLocation(locationFactory, new Path(destination.toURI()));
    } else {
      return null;
    }
  }

  /**
   * Creates the directory named by this abstract pathname, including any necessary
   * but nonexistent parent directories.
   *
   * @return true if and only if the renaming succeeded; false otherwise
   */
  @Override
  public boolean mkdirs() throws IOException {
    return fs.mkdirs(path);
  }

  /**
   * @return Length of file.
   */
  @Override
  public long length() throws IOException {
    return fs.getFileStatus(path).getLen();
  }

  @Override
  public long lastModified() throws IOException {
    return fs.getFileStatus(path).getModificationTime();
  }

  @Override
  public boolean isDirectory() throws IOException {
    return fs.isDirectory(path);
  }

  @Override
  public List<Location> list() throws IOException {
    FileStatus[] statuses = fs.listStatus(path);
    ImmutableList.Builder<Location> result = ImmutableList.builder();
    if (statuses != null) {
      for (FileStatus status : statuses) {
        if (!Objects.equal(path, status.getPath())) {
          result.add(new HDFSLocation(locationFactory, status.getPath()));
        }
      }
    }
    return result.build();
  }

  @Override
  public LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HDFSLocation other = (HDFSLocation) o;
    return Objects.equal(path, other.path);
  }

  @Override
  public int hashCode() {
    return path.hashCode();
  }
}
