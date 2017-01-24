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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This interface defines the location and operations of a resource on the filesystem.
 * <p>
 * {@link Location} is agnostic to the type of file system the resource is on.
 * </p>
 */
public interface Location {
  /**
   * Suffix added to every temp file name generated with {@link #getTempFile(String)}.
   */
  String TEMP_FILE_SUFFIX = ".tmp";

  /**
   * Checks if the this location exists.
   *
   * @return true if found; false otherwise.
   * @throws IOException
   */
  boolean exists() throws IOException;

  /**
   * @return Returns the name of the file or directory denoteed by this abstract pathname.
   */
  String getName();

  /**
   * Atomically creates a new, empty file named by this abstract pathname if and only if a file with this name
   * does not yet exist.
   * @return {@code true} if the file is successfully create, {@code false} otherwise.
   * @throws IOException if error encountered during creation of the file
   */
  boolean createNew() throws IOException;

  /**
   * Atomically creates a new, empty file named by this abstract pathname if and only if a file with this name
   * does not yet exist. The newly created file will have permission set based on the given permission settings.
   *
   * @param permission A permission string. It has to be either a three digit or a nine character string.
   *                   For the three digit string, it is similar to the UNIX permission numeric representation.
   *                   The first digit is the permission for owner, second digit is the permission for group and
   *                   the third digit is the permission for all.
   *                   For the nine character string, it uses the format as specified by the
   *                   {@link PosixFilePermissions#fromString(String)} method.
   * @return {@code true} if the file is successfully create, {@code false} otherwise.
   * @throws IOException if error encountered during creation of the file
   */
  boolean createNew(String permission) throws IOException;

  /**
   * Returns the permissions of this {@link Location}. The permission string is a nine character string as the format
   * specified by the {@link PosixFilePermissions#fromString(String)} method.
   *
   * @throws IOException if failed to get the permissions of the location
   * @throws UnsupportedOperationException if Posix file permissions are not supported by the local file system
   */
  String getPermissions() throws IOException;

  /**
   * Returns the owner of the location.
   *
   * @throws IOException if failed to get the owner of the location
   */
  String getOwner() throws IOException;

  /**
   * Returns the group of the location.
   *
   * @throws IOException if failed to get the group of the location
   * @throws UnsupportedOperationException if Posix style groups are not supported by the local file system
   */
  String getGroup() throws IOException;

  /**
   * Sets the group of the location.
   *
   * @throws IOException if failed to set the group of the location
   * @throws UnsupportedOperationException if Posix style groups are not supported by the local file system
   */
  void setGroup(String group) throws IOException;

  /**
   * Sets the permissions on this location.
   *
   * @param permission A permission string. See {@link #createNew(String)} for the format.
   *
   * @throws IOException if failed to set the permission
   * @throws UnsupportedOperationException if Posix file permissions are not supported by the local file system
   */
  void setPermissions(String permission) throws IOException;

  /**
   * @return An {@link java.io.InputStream} for this location.
   * @throws IOException
   */
  InputStream getInputStream() throws IOException;

  /**
   * @return An {@link java.io.OutputStream} for this location.
   * @throws IOException
   */
  OutputStream getOutputStream() throws IOException;

  /**
   * Creates an {@link OutputStream} for this location with the given permission. The actual permission supported
   * depends on implementation.
   *
   * @param permission A permission string. It has to be either a three digit or a nine character string.
   *                   For the three digit string, it is similar to the UNIX permission numeric representation.
   *                   The first digit is the permission for owner, second digit is the permission for group and
   *                   the third digit is the permission for all.
   *                   For the nine character string, it uses the format as specified by the
   *                   {@link PosixFilePermissions#fromString(String)} method.
   * @return An {@link OutputStream} for writing to this location.
   * @throws IOException If failed to create the {@link OutputStream}.
   */
  OutputStream getOutputStream(String permission) throws IOException;

  /**
   * Appends the child to the current {@link Location}.
   * <p>
   * Returns a new instance of Location.
   * </p>
   *
   * @param child to be appended to this location.
   * @return A new instance of {@link Location}
   * @throws IOException
   */
  Location append(String child) throws IOException;

  /**
   * Returns unique location for temporary file to be placed near this location.
   * Allows all temp files to follow same pattern for easier management of them.
   * @param suffix part of the file name to include in the temp file name
   * @return location of the temp file
   * @throws IOException
   */
  Location getTempFile(String suffix) throws IOException;

  /**
   * @return A {@link java.net.URI} for this location.
   */
  URI toURI();

  /**
   * Deletes the file or directory denoted by this abstract pathname. If this
   * pathname denotes a directory, then the directory must be empty in order
   * to be deleted.
   *
   * @return true if and only if the file or directory is successfully deleted; false otherwise.
   */
  boolean delete() throws IOException;

  /**
   * Deletes the file or directory denoted by this abstract pathname. If this
   * pathname denotes a directory and {@code recursive} is {@code true}, then content of the
   * directory will be deleted recursively, otherwise the directory must be empty in order to be deleted.
   * Note that when calling this method with {@code recursive = true} for a directory, any
   * failure during deletion will have some entries inside the directory being deleted while some are not.
   *
   * @param recursive Indicate if recursively delete a directory. Ignored if the pathname represents a file.
   * @return true if and only if the file or directory is successfully deleted; false otherwise.
   */
  boolean delete(boolean recursive) throws IOException;

  /**
   * Moves the file or directory denoted by this abstract pathname.
   *
   * @param destination destination location
   * @return new location if and only if the file or directory is successfully moved; null otherwise.
   */
  @Nullable
  Location renameTo(Location destination) throws IOException;

  /**
   * Creates the directory named by this abstract pathname, including any necessary
   * but nonexistent parent directories.
   *
   * @return true if and only if the directory was created; false otherwise
   */
  boolean mkdirs() throws IOException;

  /**
   * Creates the directory named by this abstract pathname, including any necessary
   * but nonexistent parent directories. The directories will be created with the
   * exact given permissions, regardless of a possible umask setting.
   *
   * @param permission A permission string. It has to be either a three digit or a nine character string.
   *                   For the three digit string, it is similar to the UNIX permission numeric representation.
   *                   The first digit is the permission for owner, second digit is the permission for group and
   *                   the third digit is the permission for all.
   *                   For the nine character string, it uses the format as specified by the
   *                   {@link PosixFilePermissions#fromString(String)} method.
   *
   * @return true if and only if the directory was created; false otherwise
   */
  boolean mkdirs(String permission) throws IOException;

  /**
   * @return Length of file.
   */
  long length() throws IOException;

  /**
   * @return Last modified time of file.
   */
  long lastModified() throws IOException;

  /**
   * Checks if this location represents a directory.
   *
   * @return {@code true} if it is a directory, {@code false} otherwise.
   */
  boolean isDirectory() throws IOException;

  /**
   * List the locations under this location.
   *
   * @return Immutable List of locations under this location.
   *         An empty list is returned if this location is not a directory.
   */
  List<Location> list() throws IOException;

  /**
   * Returns the location factory used to create this instance.
   *
   * @return The {@link LocationFactory} instance for creating this instance.
   */
  LocationFactory getLocationFactory();
}
