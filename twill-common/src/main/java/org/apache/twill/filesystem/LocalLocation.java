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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * A concrete implementation of {@link Location} for the Local filesystem.
 */
final class LocalLocation implements Location {
  private final File file;
  private final LocalLocationFactory locationFactory;

  /**
   * Constructs a LocalLocation.
   *
   * @param locationFactory The {@link LocationFactory} instance used to create this instance.
   * @param file to the file.
   */
  LocalLocation(LocalLocationFactory locationFactory, File file) {
    this.file = file;
    this.locationFactory = locationFactory;
  }

  /**
   * Checks if the this location exists on local file system.
   *
   * @return true if found; false otherwise.
   * @throws java.io.IOException
   */
  @Override
  public boolean exists() throws IOException {
    return file.exists();
  }

  /**
   * @return An {@link java.io.InputStream} for this location on local filesystem.
   * @throws IOException
   */
  @Override
  public InputStream getInputStream() throws IOException {
    return new FileInputStream(file);
  }

  /**
   * @return An {@link java.io.OutputStream} for this location on local filesystem.
   * @throws IOException
   */
  @Override
  public OutputStream getOutputStream() throws IOException {
    ensureDirectory(file.getParentFile());
    return new FileOutputStream(file);
  }

  @Override
  public OutputStream getOutputStream(String permission) throws IOException {
    Set<PosixFilePermission> permissions = parsePermissions(permission);
    ensureDirectory(file.getParentFile(), permissions);
    Path path = file.toPath();
    WritableByteChannel channel = Files.newByteChannel(path,
                                                       EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE),
                                                       PosixFilePermissions.asFileAttribute(permissions));
    // Set the permission explicitly again to skip the umask
    Files.setPosixFilePermissions(path, permissions);
    return Channels.newOutputStream(channel);
  }

  /**
   * @return Returns the name of the file or directory denoted by this abstract pathname.
   */
  @Override
  public String getName() {
    return file.getName();
  }

  @Override
  public boolean createNew() throws IOException {
    return file.createNewFile();
  }

  @Override
  public boolean createNew(String permission) throws IOException {
    Set<PosixFilePermission> permissions = parsePermissions(permission);
    try {
      Path path = Files.createFile(file.toPath(), PosixFilePermissions.asFileAttribute(permissions));
      // Set the permission explicitly again to skip the umask
      Files.setPosixFilePermissions(path, permissions);
      return true;
    } catch (FileAlreadyExistsException e) {
      return false;
    }
  }

  @Override
  public String getOwner() throws IOException {
    return Files.getOwner(file.toPath()).getName();
  }

  @Override
  public String getGroup() throws IOException {
    return Files.readAttributes(file.toPath(), PosixFileAttributes.class).group().getName();
  }

  @Override
  public void setGroup(String group) throws IOException {
    UserPrincipalLookupService lookupService = FileSystems.getDefault().getUserPrincipalLookupService();
    GroupPrincipal groupPrincipal = lookupService.lookupPrincipalByGroupName(group);
    Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class).setGroup(groupPrincipal);
  }

  @Override
  public String getPermissions() throws IOException {
    return PosixFilePermissions.toString(Files.getPosixFilePermissions(file.toPath()));
  }

  @Override
  public void setPermissions(String permission) throws IOException {
    Files.setPosixFilePermissions(file.toPath(), parsePermissions(permission));
  }

  /**
   * Appends the child to the current {@link Location} on local filesystem.
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
    return new LocalLocation(locationFactory, new File(file, child));
  }

  @Override
  public Location getTempFile(String suffix) throws IOException {
    String newName = file.getAbsolutePath() + "." + UUID.randomUUID() + (suffix == null ? TEMP_FILE_SUFFIX : suffix);
    return new LocalLocation(locationFactory, new File(newName));
  }

  /**
   * @return A {@link URI} for this location on local filesystem.
   */
  @Override
  public URI toURI() {
    return file.toURI();
  }

  /**
   * Deletes the file or directory denoted by this abstract pathname. If this
   * pathname denotes a directory, then the directory must be empty in order
   * to be deleted.
   *
   * @return true if and only if the file or directory is successfully delete; false otherwise.
   */
  @Override
  public boolean delete() throws IOException {
    return file.delete();
  }

  @Override
  public boolean delete(boolean recursive) throws IOException {
    if (!recursive) {
      return delete();
    }

    Deque<File> stack = new LinkedList<File>();
    stack.add(file);
    while (!stack.isEmpty()) {
      File f = stack.peekLast();
      File[] files = f.listFiles();

      if (files != null && files.length != 0) {
        Collections.addAll(stack, files);
      } else {
        if (!f.delete()) {
          return false;
        }
        stack.pollLast();
      }
    }
    return true;
  }

  @Override
  public Location renameTo(Location destination) throws IOException {
    // destination will always be of the same type as this location
    Path target = Files.move(file.toPath(), ((LocalLocation) destination).file.toPath(),
                             StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

    if (target != null) {
      return new LocalLocation(locationFactory, target.toFile());
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
    return file.mkdirs();
  }

  @Override
  public boolean mkdirs(String permission) throws IOException {
    Set<PosixFilePermission> posixPermissions = parsePermissions(permission);
    return mkdirs(file, posixPermissions);
  }

  private boolean mkdirs(File file, Set<PosixFilePermission> permissions) throws IOException {
    if (file.exists()) {
      return false;
    }
    if (file.mkdir()) {
      Files.setPosixFilePermissions(file.toPath(), permissions);
      return true;
    }
    File parent = file.getParentFile();
    if (parent == null) {
      return false;
    }
    if (!mkdirs(parent, permissions) && !parent.exists()) {
      return false;
    }
    if (file.mkdir()) {
      Files.setPosixFilePermissions(file.toPath(), permissions);
      return true;
    }
    return false;
  }

  /**
   * @return Length of file.
   */
  @Override
  public long length() throws IOException {
    return file.length();
  }

  @Override
  public long lastModified() {
    return file.lastModified();
  }

  @Override
  public boolean isDirectory() throws IOException {
    return file.isDirectory();
  }

  @Override
  public List<Location> list() throws IOException {
    File[] files = file.listFiles();
    List<Location> result = new ArrayList<Location>();
    if (files != null) {
      for (File file : files) {
        result.add(new LocalLocation(locationFactory, file));
      }
    } else if (!file.exists()) {
      throw new FileNotFoundException("File " + file + " does not exist.");
    }
    return Collections.unmodifiableList(result);
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

    LocalLocation that = (LocalLocation) o;
    return file.equals(that.file);
  }

  @Override
  public int hashCode() {
    return file.hashCode();
  }

  @Override
  public String toString() {
    return file.toString();
  }

  /**
   * Ensures the given {@link File} is a directory. If it doesn't exist, it will be created.
   */
  private void ensureDirectory(File dir) throws IOException {
    // Check, create, check to resolve race conditions if there are concurrent creation of the directory.
    if (!dir.isDirectory() && !dir.mkdirs() && !dir.isDirectory()) {
      throw new IOException("Failed to create directory " + dir);
    }
  }

  /**
   * Ensures the given {@link File} is a directory. If it doesn't exist, it will be created.
   */
  private void ensureDirectory(File dir, Set<PosixFilePermission> permission) throws IOException {
    // Check, create, check to resolve race conditions if there are concurrent creation of the directory.
    if (!dir.isDirectory() && !mkdirs(dir, permission) && !dir.isDirectory()) {
      throw new IOException("Failed to create directory " + dir);
    }
  }

  /**
   * Parses the given permission to a set of {@link PosixFilePermission}.
   *
   * @param permission the permission as passed to the {@link #createNew(String)} or {@link #getOutputStream(String)}
   *                   methods.
   * @return a new set of {@link PosixFilePermission}.
   */
  private Set<PosixFilePermission> parsePermissions(String permission) {
    Set<PosixFilePermission> permissions;
    if (permission.length() == 3) {
      permissions = parseNumericPermissions(permission);
    } else if (permission.length() == 9) {
      permissions = PosixFilePermissions.fromString(permission);
    } else {
      throw new IllegalArgumentException("Invalid permission " + permission +
                                           ". Permission should either be a three digit or nine character string.");
    }

    return permissions;
  }

  /**
   * Parses a three digit UNIX numeric permission representation to a set of {@link PosixFilePermission}.
   */
  private Set<PosixFilePermission> parseNumericPermissions(String permission) {
    String posixPermission = "";
    for (int i = 0; i < 3; i++) {
      int digit = permission.charAt(i) - '0';
      if (digit < 0 || digit > 7) {
        throw new IllegalArgumentException("Invalid permission " + permission +
                                             ". Only digits between 0-7 are allowed.");
      }
      posixPermission += ((digit & 4) != 0) ? "r" : "-";
      posixPermission += ((digit & 2) != 0) ? "w" : "-";
      posixPermission += ((digit & 1) != 0) ? "x" : "-";
    }
    return PosixFilePermissions.fromString(posixPermission);
  }
}
