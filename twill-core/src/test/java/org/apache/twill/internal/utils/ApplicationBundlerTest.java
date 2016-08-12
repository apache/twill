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
package org.apache.twill.internal.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.ApplicationBundler;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 *
 */
public class ApplicationBundlerTest {

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @Test
  public void testFindDependencies() throws IOException, ClassNotFoundException {
    Location location = new LocalLocationFactory(tmpDir.newFolder()).create("test.jar");

    // Create a jar file with by tracing dependency
    ApplicationBundler bundler = new ApplicationBundler(ImmutableList.<String>of());
    bundler.createBundle(location, ApplicationBundler.class);

    File targetDir = tmpDir.newFolder();
    unjar(new File(location.toURI()), targetDir);

    // Load the class back, it should be loaded by the custom classloader
    ClassLoader classLoader = createClassLoader(targetDir);
    Class<?> clz = classLoader.loadClass(ApplicationBundler.class.getName());
    Assert.assertSame(classLoader, clz.getClassLoader());

    // For system classes, they shouldn't be packaged, hence loaded by different classloader.
    clz = classLoader.loadClass(Object.class.getName());
    Assert.assertNotSame(classLoader, clz.getClassLoader());
  }

  @Test
  public void testSameJar() throws IOException, ClassNotFoundException {
    File j1 = new File("src/test/resources/jar1/samename.jar");
    File j2 = new File("src/test/resources/jar2/samename.jar");
    
    ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      List<URL> urls = new ArrayList<>();
      urls.add(j1.toURI().toURL());
      urls.add(j2.toURI().toURL());
      Thread.currentThread().setContextClassLoader(createClassLoader(urls));

      // create bundle
      Location location = new LocalLocationFactory(tmpDir.newFolder()).create("test.jar");
      ApplicationBundler bundler = new ApplicationBundler(ImmutableList.<String> of());
      bundler.createBundle(location, Class1.class, Class2.class);

      File targetDir = tmpDir.newFolder();
      unjar(new File(location.toURI()), targetDir);

      // should be able to load both classes
      ClassLoader classLoader = createClassLoader(targetDir);
      Class<?> c1 = classLoader.loadClass(Class1.class.getName());
      Class<?> c2 = classLoader.loadClass(Class2.class.getName());
      Assert.assertSame(classLoader, c1.getClassLoader());
      Assert.assertSame(classLoader, c2.getClassLoader());

    } finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
  }

  private void unjar(File jarFile, File targetDir) throws IOException {
    try (JarInputStream jarInput = new JarInputStream(new FileInputStream(jarFile))) {
      JarEntry jarEntry = jarInput.getNextJarEntry();
      while (jarEntry != null) {
        File target = new File(targetDir, jarEntry.getName());
        if (jarEntry.isDirectory()) {
          target.mkdirs();
        } else {
          target.getParentFile().mkdirs();
          ByteStreams.copy(jarInput, Files.newOutputStreamSupplier(target));
        }

        jarEntry = jarInput.getNextJarEntry();
      }
    }
  }

  private ClassLoader createClassLoader(File dir) throws MalformedURLException {
    List<URL> urls = Lists.newArrayList();
    urls.add(new File(dir, "classes").toURI().toURL());
    File[] libFiles = new File(dir, "lib").listFiles();
    if (libFiles != null) {
      for (File file : libFiles) {
        urls.add(file.toURI().toURL());
      }
    }
    return createClassLoader(urls);
  }

  /**
   * @param urls
   * @return
   */
  private ClassLoader createClassLoader(final List<URL> urls) {
    return new URLClassLoader(urls.toArray(new URL[0])) {
      /** {@inheritDoc} */
      @Override
      public synchronized URL getResource(String name) {
        /* hard coding this because super.getResource prefers parent loader */
        try {
          if (name.contains("Class1")) {
            return new URL("jar", null, urls.get(0).toExternalForm() + "!/" + name);
          } else if (name.contains("Class2")) {
            return new URL("jar", null, urls.get(1).toExternalForm() + "!/" + name);
          } else {
            return super.getResource(name);
          }
        } catch (MalformedURLException e) {
          return null;
        }
      }

      @Override
      protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // Load class from the given URLs first before delegating to parent.
        try {
          return super.findClass(name);
        } catch (ClassNotFoundException e) {
          ClassLoader parent = getParent();
          return parent == null ? ClassLoader.getSystemClassLoader().loadClass(name) : parent.loadClass(name);
        }
      }
    };
  }
}
