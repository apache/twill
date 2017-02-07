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
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

/**
 * A concrete implementation of {@link Location} for the HDFS filesystem using {@link FileSystem}.
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
    FsPermission fsPermission = parsePermissions(permission);
    OutputStream os = fs.create(path,
                                fsPermission,
                                true,
                                fs.getConf().getInt("io.file.buffer.size", 4096),
                                fs.getDefaultReplication(path),
                                fs.getDefaultBlockSize(path),
                                null);
    // Set the permission explicitly again to skip the umask
    fs.setPermission(path, fsPermission);
    return os;
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
   * @return Returns the name of the file or directory denoted by this abstract pathname.
   */
  @Override
  public String getName() {
    return path.getName();
  }

  @Override
  public boolean createNew() throws IOException {
    return fs.createNewFile(path);
  }

  @Override
  public boolean createNew(String permission) throws IOException {
    try {
      FsPermission fsPermission = parsePermissions(permission);
      fs.create(path, fsPermission, EnumSet.of(CreateFlag.CREATE),
                fs.getConf().getInt("io.file.buffer.size", 4096),
                fs.getDefaultReplication(path),
                fs.getDefaultBlockSize(path),
                null).close();
      // Set the permission explicitly again to skip the umask
      fs.setPermission(path, fsPermission);
      return true;
    } catch (FileAlreadyExistsException e) {
      return false;
    }
  }

  @Override
  public String getOwner() throws IOException {
    return fs.getFileStatus(path).getOwner();
  }

  @Override
  public String getGroup() throws IOException {
    return fs.getFileStatus(path).getGroup();
  }

  @Override
  public void setGroup(String group) throws IOException {
    fs.setOwner(path, null, group);
  }

  @Override
  public String getPermissions() throws IOException {
    FsPermission permission = fs.getFileStatus(path).getPermission();
    return permission.getUserAction().SYMBOL + permission.getGroupAction().SYMBOL + permission.getOtherAction().SYMBOL;
  }

  @Override
  public void setPermissions(String permission) throws IOException {
    fs.setPermission(path, parsePermissions(permission));
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

  @Override
  public boolean mkdirs() throws IOException {
    try {
      if (fs.exists(path)) {
        return false;
      }
      return fs.mkdirs(path);
    } catch (FileAlreadyExistsException | AccessControlException e) {
      // curiously, if one of the parent dirs exists but is a file, Hadoop throws this:
      // org.apache...AccessControlException: Permission denied: user=..., access=EXECUTE, inode=".../existingfile"
      // however, if the directory itself exists, it will throw FileAlreadyExistsException
      return false;
    }
  }

  @Override
  public boolean mkdirs(String permission) throws IOException {
    return mkdirs(path, parsePermissions(permission));
  }

  /**
   * Helper to create a directory and its parents id necessary, all with the given permissions.
   * We cannot use the fs.mkdirs() because that would apply the umask to the permissions.
   */
  private boolean mkdirs(Path path, FsPermission permission) throws IOException {
    try {
      if (fs.exists(path)) {
        return false;
      }
    } catch (AccessControlException e) {
      // curiously, if one of the parent dirs exists but is a file, Hadoop throws this:
      // org.apache...AccessControlException: Permission denied: user=..., access=EXECUTE, inode=".../existingfile"
      return false;
    }
    Path parent = path.getParent();
    if (null == parent) {
      return false;
    }
    // if parent exists, attempt to create the path as a directory.
    if (fs.exists(parent)) {
      return mkdir(path, permission);
    }
    // attempt to create the parent with the proper permissions
    if (!mkdirs(parent, permission) && !fs.isDirectory(parent)) {
      return false;
    }
    // now the parent exists and we can create this directory
    return mkdir(path, permission);
  }

  /**
   * Helper to create a directory (but not its parents) with the given permissions.
   * We cannot use fs.mkdirs() and then apply the permissions to override the umask.
   */
  private boolean mkdir(Path path, FsPermission permission) throws IOException {
    try {
      if (!fs.mkdirs(path) && !fs.isDirectory(path)) {
        return false;
      }
    } catch (FileAlreadyExistsException e) {
      return false;
    }
    // explicitly set permissions to get around the umask
    fs.setPermission(path, permission);
    return true;
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

  @Override
  public String toString() {
    return path.toString();
  }

  /**
   * Parses the given permission to {@link FsPermission}. Since the {@link HDFSLocationFactory} and this class are
   * deprecated, this method is copied from {@link FileContextLocation} instead of creating an extra library to share.
   *
   * @param permission the permission as passed to the {@link #createNew(String)} or {@link #getOutputStream(String)}
   *                   methods.
   * @return a new {@link FsPermission}.
   */
  private FsPermission parsePermissions(String permission) {
    if (permission.length() == 3) {
      return new FsPermission(permission);
    } else if (permission.length() == 9) {
      // The FsPermission expect a 10 characters string, which it will ignore the first character
      return FsPermission.valueOf("-" + permission);
    } else {
      throw new IllegalArgumentException("Invalid permission " + permission +
                                           ". Permission should either be a three digit or nine character string.");
    }
  }
}
