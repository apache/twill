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

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.utils.Dependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 * This class builds jar files based on class dependencies.
 */
public final class ApplicationBundler {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationBundler.class);

  private final ClassAcceptor classAcceptor;
  private final Set<String> bootstrapClassPaths;
  private final CRC32 crc32;

  private File tempDir;
  private String classesDir;
  private String libDir;
  private String resourcesDir;

  /**
   * Constructs an ApplicationBundler.
   *
   * @param excludePackages Class packages to exclude
   */
  public ApplicationBundler(Iterable<String> excludePackages) {
    this(excludePackages, ImmutableList.<String>of());
  }

  /**
   * Constructs an ApplicationBundler.
   *
   * @param excludePackages Class packages to exclude
   * @param includePackages Class packages that should be included. Anything in this list will override the
   *                        one provided in excludePackages.
   */
  public ApplicationBundler(final Iterable<String> excludePackages, final Iterable<String> includePackages) {
    this(new ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        for (String includePackage : includePackages) {
          if (className.startsWith(includePackage)) {
            return true;
          }
        }
        for (String excludePackage : excludePackages) {
          if (className.startsWith(excludePackage)) {
            return false;
          }
        }
        return true;
      }
    });
  }

  /**
   * Constructs an ApplicationBundler.
   *
   * @param classAcceptor ClassAcceptor for class packages to include
   */
  public ApplicationBundler(ClassAcceptor classAcceptor) {
    this.classAcceptor = classAcceptor;
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (String classpath : Splitter.on(File.pathSeparatorChar).split(System.getProperty("sun.boot.class.path"))) {
      File file = new File(classpath);
      builder.add(file.getAbsolutePath());
      try {
        builder.add(file.getCanonicalPath());
      } catch (IOException e) {
        // Ignore the exception and proceed.
      }
    }
    this.bootstrapClassPaths = builder.build();
    this.crc32 = new CRC32();
    this.tempDir = new File(System.getProperty("java.io.tmpdir"));
    this.classesDir = "classes/";
    this.libDir = "lib/";
    this.resourcesDir = "resources/";
  }

  /**
   * Sets the temporary directory used by this class when generating new jars.
   * By default it is using the {@code java.io.tmpdir} property.
   */
  public ApplicationBundler setTempDir(File tempDir) {
    if (tempDir == null) {
      throw new IllegalArgumentException("Temporary directory cannot be null");
    }
    this.tempDir = tempDir;
    return this;
  }

  /**
   * Sets the name of the directory inside the bundle jar that all ".class" files stored in.
   * Passing in an empty string will store files at the root level inside the jar file.
   * By default it is "classes".
   */
  public ApplicationBundler setClassesDir(String classesDir) {
    if (classesDir == null) {
      throw new IllegalArgumentException("Directory cannot be null");
    }
    this.classesDir = classesDir.endsWith("/") ? classesDir : classesDir + "/";
    return this;
  }

  /**
   * Sets the name of the directory inside the bundle jar that all ".jar" files stored in.
   * Passing in an empty string will store files at the root level inside the jar file.
   * By default it is "lib".
   */
  public ApplicationBundler setLibDir(String libDir) {
    if (classesDir == null) {
      throw new IllegalArgumentException("Directory cannot be null");
    }
    this.libDir = libDir.endsWith("/") ? libDir : libDir + "/";
    return this;
  }

  /**
   * Sets the name of the directory inside the bundle jar that all resource files stored in.
   * Passing in an empty string will store files at the root level inside the jar file.
   * By default it is "resources".
   */
  public ApplicationBundler setResourcesDir(String resourcesDir) {
    if (classesDir == null) {
      throw new IllegalArgumentException("Directory cannot be null");
    }
    this.resourcesDir = resourcesDir.endsWith("/") ? resourcesDir : resourcesDir + "/";
    return this;
  }

  public void createBundle(Location target, Iterable<Class<?>> classes) throws IOException {
    createBundle(target, classes, ImmutableList.<URI>of());
  }

  /**
   * Same as calling {@link #createBundle(Location, Iterable)}.
   */
  public void createBundle(Location target, Class<?> clz, Class<?>...classes) throws IOException {
    createBundle(target, ImmutableSet.<Class<?>>builder().add(clz).add(classes).build());
  }

  /**
   * Creates a jar file which includes all the given classes and all the classes that they depended on.
   * The jar will also include all classes and resources under the packages as given as include packages
   * in the constructor.
   *
   * @param target Where to save the target jar file.
   * @param resources Extra resources to put into the jar file. If resource is a jar file, it'll be put under
   *                  lib/ entry, otherwise under the resources/ entry.
   * @param classes Set of classes to start the dependency traversal.
   * @throws IOException if failed to create the bundle
   */
  public void createBundle(Location target, Iterable<Class<?>> classes, Iterable<URI> resources) throws IOException {
    LOG.debug("Start creating bundle at {}", target);
    // Write the jar to local tmp file first
    File tmpJar = File.createTempFile(target.getName(), ".tmp", tempDir);
    LOG.debug("First create bundle locally at {}", tmpJar);
    try {
      Set<String> entries = Sets.newHashSet();
      try (JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(tmpJar))) {
        // Find class dependencies
        findDependencies(classes, entries, jarOut);

        // Add extra resources
        for (URI resource : resources) {
          copyResource(resource, entries, jarOut);
        }
      }
      LOG.debug("Copying temporary bundle to destination {} ({} bytes)", target, tmpJar.length());
      // Copy the tmp jar into destination.
      try (OutputStream os = new BufferedOutputStream(target.getOutputStream())) {
        Files.copy(tmpJar, os);
      } catch (IOException e) {
        throw new IOException("Failed to copy bundle from " + tmpJar + " to " + target, e);
      }
      LOG.debug("Finished creating bundle at {}", target);
    } finally {
      if (!tmpJar.delete()) {
        LOG.warn("Failed to cleanup local temporary file {}", tmpJar);
      } else {
        LOG.debug("Cleaned up local temporary file {}", tmpJar);
      }
    }
  }

  private void findDependencies(Iterable<Class<?>> classes, final Set<String> entries,
                                final JarOutputStream jarOut) throws IOException {

    Iterable<String> classNames = Iterables.transform(classes, new Function<Class<?>, String>() {
      @Override
      public String apply(Class<?> input) {
        return input.getName();
      }
    });

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = getClass().getClassLoader();
    }

    // Record the set of classpath URL that are already added to the jar
    final Set<URL> seenClassPaths = Sets.newHashSet();
    Dependencies.findClassDependencies(classLoader, new ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        if (bootstrapClassPaths.contains(classPathUrl.getFile())) {
          return false;
        }
        if (!classAcceptor.accept(className, classUrl, classPathUrl)) {
          return false;
        }
        if (seenClassPaths.add(classPathUrl)) {
          putEntry(className, classUrl, classPathUrl, entries, jarOut);
        }
        return true;
      }
    }, classNames);
  }

  private void putEntry(String className, URL classUrl, URL classPathUrl, Set<String> entries, JarOutputStream jarOut) {
    String classPath = classPathUrl.getFile();
    if (classPath.endsWith(".jar")) {
      String entryName = classPath.substring(classPath.lastIndexOf('/') + 1);
      /* need unique name or else we lose classes (TWILL-181) we know the classPath is unique because it is
       * coming from a set, preserve as much as possible of it by prepending elements of the path until it is
       * unique. */
      if (entries.contains(libDir + entryName)) {
        String[] parts = classPath.split("/");
        for (int i = parts.length - 2; i >= 0; i--) {
          entryName = parts[i] + "-" + entryName;
          if (!entries.contains(libDir + entryName)) {
            break;
          }
        }
      }
      saveDirEntry(libDir, entries, jarOut);
      saveEntry(libDir + entryName, classPathUrl, entries, jarOut, false);
    } else {
      // Class file, put it under the classes directory
      saveDirEntry(classesDir, entries, jarOut);
      if ("file".equals(classPathUrl.getProtocol())) {
        // Copy every files under the classPath
        try {
          copyDir(new File(classPathUrl.toURI()), classesDir, entries, jarOut);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      } else {
        String entry = classesDir + className.replace('.', '/') + ".class";
        saveDirEntry(entry.substring(0, entry.lastIndexOf('/') + 1), entries, jarOut);
        saveEntry(entry, classUrl, entries, jarOut, true);
      }
    }
  }

  /**
   * Saves a directory entry to the jar output.
   */
  private void saveDirEntry(String path, Set<String> entries, JarOutputStream jarOut) {
    if (entries.contains(path)) {
      return;
    }

    try {
      String entry = "";
      for (String dir : Splitter.on('/').omitEmptyStrings().split(path)) {
        entry += dir + '/';
        if (entries.add(entry)) {
          JarEntry jarEntry = new JarEntry(entry);
          jarEntry.setMethod(JarOutputStream.STORED);
          jarEntry.setSize(0L);
          jarEntry.setCrc(0L);
          jarOut.putNextEntry(jarEntry);
          jarOut.closeEntry();
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Saves a class entry to the jar output.
   */
  private void saveEntry(String entry, URL url, Set<String> entries, JarOutputStream jarOut, boolean compress) {
    if (!entries.add(entry)) {
      return;
    }
    LOG.trace("adding bundle entry " + entry);
    try {
      JarEntry jarEntry = new JarEntry(entry);

      try (InputStream is = url.openStream()) {
        if (compress) {
          jarOut.putNextEntry(jarEntry);
          ByteStreams.copy(is, jarOut);
        } else {
          crc32.reset();
          TransferByteOutputStream os = new TransferByteOutputStream();
          CheckedOutputStream checkedOut = new CheckedOutputStream(os, crc32);
          ByteStreams.copy(is, checkedOut);
          checkedOut.close();

          long size = os.size();
          jarEntry.setMethod(JarEntry.STORED);
          jarEntry.setSize(size);
          jarEntry.setCrc(checkedOut.getChecksum().getValue());
          jarOut.putNextEntry(jarEntry);
          os.transfer(jarOut);
        }
      }
      jarOut.closeEntry();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }


  /**
   * Copies all entries under the file path.
   */
  private void copyDir(File baseDir, String entryPrefix,
                       Set<String> entries, JarOutputStream jarOut) throws IOException {
    LOG.trace("adding whole dir {} to bundle at '{}'", baseDir, entryPrefix);
    URI baseUri = baseDir.toURI();
    Queue<File> queue = Lists.newLinkedList();
    queue.add(baseDir);
    while (!queue.isEmpty()) {
      File file = queue.remove();

      String entry = entryPrefix + baseUri.relativize(file.toURI()).getPath();
      if (entries.add(entry)) {
        jarOut.putNextEntry(new JarEntry(entry));
        if (file.isFile()) {
          try {
            Files.copy(file, jarOut);
          } catch (IOException e) {
            throw new IOException("failure copying from " + file.getAbsoluteFile() + " to JAR file entry " + entry, e);
          }
        }
        jarOut.closeEntry();
      }

      if (file.isDirectory()) {
        File[] files = file.listFiles();
        if (files != null) {
          Collections.addAll(queue, files);
        }
      }
    }
  }

  private void copyResource(URI resource, Set<String> entries, JarOutputStream jarOut) throws IOException {
    if ("file".equals(resource.getScheme())) {
      File file = new File(resource);
      if (file.isDirectory()) {
        saveDirEntry(resourcesDir, entries, jarOut);
        copyDir(file, resourcesDir, entries, jarOut);
        return;
      }
    }

    URL url = resource.toURL();
    String path = url.getFile();
    String prefix = path.endsWith(".jar") ? libDir : resourcesDir;
    path = prefix + path.substring(path.lastIndexOf('/') + 1);

    if (entries.add(path)) {
      saveDirEntry(prefix, entries, jarOut);
      jarOut.putNextEntry(new JarEntry(path));
      try (InputStream is = url.openStream()) {
        ByteStreams.copy(is, jarOut);
      }
    }
  }

  private static final class TransferByteOutputStream extends ByteArrayOutputStream {

    void transfer(OutputStream os) throws IOException {
      os.write(buf, 0, count);
    }
  }
}
