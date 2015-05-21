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
package org.apache.twill.ext;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Runs a bundled jar specified by jarPath.
 *
 * 1. Loads the bundled jar and its dependencies (in /lib) into a class loader
 * assuming the following format of the bundled jar:
 * /*.class (class files)
 * /lib/*.jar (dependencies required by the user code)
 *
 * 2. Instantiates an instance of the class {#mainClassName} and calls main({#args}) on it.
*/
public class BundledJarRunner {

  private static final Logger LOG = LoggerFactory.getLogger(BundledJarRunner.class);

  private final File jarFile;
  private final Arguments arguments;
  private Method mainMethod;

  public BundledJarRunner(File jarFile, Arguments arguments) {
    Preconditions.checkNotNull(jarFile);
    Preconditions.checkState(jarFile.exists());
    Preconditions.checkState(jarFile.canRead());
    Preconditions.checkNotNull(arguments.getMainClassName());
    Preconditions.checkNotNull(arguments.getLibFolder());

    this.jarFile = jarFile;
    this.arguments = arguments;
  }

  public void load() throws ClassNotFoundException, NoSuchMethodException,
    InstantiationException, IllegalAccessException, IOException {
    this.load(ClassLoader.getSystemClassLoader());
  }

  public void load(ClassLoader parentClassLoader) throws IOException, ClassNotFoundException,
    IllegalAccessException, InstantiationException, NoSuchMethodException {

    String mainClassName = arguments.getMainClassName();
    String libFolder = arguments.getLibFolder();

    Preconditions.checkNotNull(mainClassName);
    Preconditions.checkNotNull(libFolder);

    File inputJarFile = this.jarFile;
    File outputJarDir = Files.createTempDir();

    LOG.debug("Unpacking jar to " + outputJarDir.getAbsolutePath());
    JarFile jarFile = new JarFile(inputJarFile);
    unJar(jarFile, outputJarDir);

    LOG.debug("Loading jars into ClassLoader");
    List<URL> classPathUrls = new LinkedList<URL>();
    classPathUrls.add(inputJarFile.toURI().toURL());
    classPathUrls.addAll(getJarURLs(new File(outputJarDir, libFolder)));
    URL[] classPathUrlArray = classPathUrls.toArray(new URL[classPathUrls.size()]);

    for (URL url : classPathUrlArray) {
      LOG.debug("Loading jar: " + url.getPath());
    }

    ClassLoader classLoader = new URLClassLoader(classPathUrlArray, parentClassLoader);
    Thread.currentThread().setContextClassLoader(classLoader);

    LOG.debug("Instantiating instance of " + mainClassName);
    Class<?> cls = classLoader.loadClass(mainClassName);
    mainMethod = cls.getMethod("main", String[].class);
  }

  public void run() throws Throwable {
    Preconditions.checkNotNull(mainMethod, "Must call load() first");
    String mainClassName = arguments.getMainClassName();
    String[] args = arguments.getMainArgs();

    try {
      LOG.info("Invoking " + mainClassName + ".main(" + Arrays.toString(args) + ")");
      mainMethod.invoke(null, new Object[] { args });
    } catch (Throwable t) {
      LOG.error("Error while trying to run " + mainClassName + " within " + jarFile.getAbsolutePath(), t);
      throw t;
    }
  }

  private void unJar(JarFile jarFile, File targetDirectory) throws IOException {
    Enumeration<JarEntry> entries = jarFile.entries();
    while (entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      File output = new File(targetDirectory, entry.getName());

      if (entry.isDirectory()) {
        output.mkdirs();
      } else {
        output.getParentFile().mkdirs();

        try (
          OutputStream os = new FileOutputStream(output);
          InputStream is = jarFile.getInputStream(entry)
        ) {
          ByteStreams.copy(is, os);
        }
      }
    }
  }

  private List<URL> getJarURLs(File dir) throws MalformedURLException {
    File[] files = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".jar");
      }
    });
    List<URL> urls = new LinkedList<URL>();

    if (files != null) {
      for (File file : files) {
        urls.add(file.toURI().toURL());
      }
    } else {
      LOG.warn("No jar files found in " + dir.getAbsolutePath());
    }

    return urls;
  }

  /**
   * Contains runtime arguments for {@link org.apache.twill.ext.BundledJarRunner}.
   */
  public static class Arguments {

    /**
     * Filename of the bundled jar, as specified in the TwillSpecification local files.
     */
    private final String jarFileName;

    /**
     * Class name of the class having the main() that is to be called.
     */
    private final String mainClassName;

    /**
     * Arguments to pass the main() of the class specified by mainClassName.
     */
    private final String[] mainArgs;

    /**
     * Folder within the bundled jar containing the jar dependencies.
     */
    private final String libFolder;

    public Arguments(String jarFileName, String libFolder, String mainClassName, String[] mainArgs) {
      this.jarFileName = jarFileName;
      this.libFolder = libFolder;
      this.mainClassName = mainClassName;
      this.mainArgs = mainArgs;
    }

    public static Arguments fromArray(String[] args) {
      Preconditions.checkArgument(args.length >= 3, "Requires at least 3 arguments:"
        + " <jarFileName> <libFolder> <mainClassName>");

      Builder builder = new Builder();
      builder.setJarFileName(args[0]);
      builder.setLibFolder(args[1]);
      builder.setMainClassName(args[2]);
      builder.setMainArgs(Arrays.copyOfRange(args, 3, args.length));

      return builder.createArguments();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Arguments arguments = (Arguments) o;
      return Objects.equal(jarFileName, arguments.jarFileName)
        && Objects.equal(libFolder, arguments.libFolder)
        && Arrays.deepEquals(mainArgs, arguments.mainArgs)
        && Objects.equal(mainClassName, arguments.mainClassName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(jarFileName, mainClassName, mainArgs, libFolder);
    }

    public String[] toArray() {
      String[] result = new String[3 + mainArgs.length];
      result[0] = jarFileName;
      result[1] = libFolder;
      result[2] = mainClassName;
      for (int i = 0; i < mainArgs.length; i++) {
        result[3 + i] = mainArgs[i];
      }

      return result;
    }

    public String getJarFileName() {
      return jarFileName;
    }

    public String getLibFolder() {
      return libFolder;
    }

    public String getMainClassName() {
      return mainClassName;
    }

    public String[] getMainArgs() {
      return mainArgs;
    }

    /**
     * Builder for {@link org.apache.twill.ext.BundledJarRunner.Arguments}.
     */
    public static class Builder {
      private String jarFileName;
      private String libFolder;
      private String mainClassName;
      private String[] mainArgs;

      public Builder() {}

      public Builder setJarFileName(String jarFileName) {
        this.jarFileName = jarFileName;
        return this;
      }

      public Builder setLibFolder(String libFolder) {
        this.libFolder = libFolder;
        return this;
      }

      public Builder setMainClassName(String mainClassName) {
        this.mainClassName = mainClassName;
        return this;
      }

      public Builder setMainArgs(String[] mainArgs) {
        this.mainArgs = mainArgs;
        return this;
      }

      public Builder from(Arguments arguments) {
        this.jarFileName = arguments.getJarFileName();
        this.libFolder = arguments.getLibFolder();
        this.mainClassName = arguments.getMainClassName();
        this.mainArgs = arguments.getMainArgs();
        return this;
      }

      public Arguments createArguments() {
        return new Arguments(jarFileName, libFolder, mainClassName, mainArgs);
      }
    }
  }

}
