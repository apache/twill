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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;

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
    File dir1 = tmpDir.newFolder();
    File dir2 = tmpDir.newFolder();
    File j1 = new File(dir1, "samename.jar");
    File j2 = new File(dir2, "samename.jar");

    createJar(Class1.class, j1);
    createJar(Class2.class, j2);
    
    ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    Location location;

    try {
      URL[] urls = new URL[] { j1.toURI().toURL(), j2.toURI().toURL() };
      Thread.currentThread().setContextClassLoader(new URLClassLoader(urls, null));

      // create bundle
      location = new LocalLocationFactory(tmpDir.newFolder()).create("test.jar");
      ApplicationBundler bundler = new ApplicationBundler(ImmutableList.<String> of());
      bundler.createBundle(location, Class1.class, Class2.class);

    } finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }

    File targetDir = tmpDir.newFolder();
    unjar(new File(location.toURI()), targetDir);

    // should be able to load both classes
    ClassLoader classLoader = createClassLoader(targetDir);
    Class<?> c1 = classLoader.loadClass(Class1.class.getName());
    Class<?> c2 = classLoader.loadClass(Class2.class.getName());

    // make sure we are loading them from the correct class loader (not defaulting back to some classloader
    // from the test
    Assert.assertSame(classLoader, c1.getClassLoader());
    Assert.assertSame(classLoader, c2.getClassLoader());
  }

  private void createJar(Class<?> clazz, File jarFile) throws IOException {
    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile))) {
      String pathname = clazz.getName().replace(".", "/") + ".class";
      try (InputStream is = clazz.getClassLoader().getResourceAsStream(pathname)) {
        JarEntry entry = new JarEntry(pathname);
        jos.putNextEntry(entry);
        ByteStreams.copy(is, jos);
        jos.closeEntry();
      }
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
    return new URLClassLoader(urls.toArray(new URL[0])) {
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
