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

import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

/**
 * Tests for {@link BundledJarRunner}.
 */
public class BundledJarRunnerTest {

  @ClassRule
  public static TemporaryFolder tempDir = new TemporaryFolder();

  private static String jarOutput;

  static class Simple {
    public static void main(String[] args) {
      jarOutput = args[0];
    }
  }

  static class NoZeroArgsConstructor {
    public static void main(String[] args) {
      NoZeroArgsConstructor obj = new NoZeroArgsConstructor(args[0]);
    }
    public NoZeroArgsConstructor(String arg) {
      jarOutput = arg;
    }
  }

  @Test
  public void runSimpleJar() throws Throwable {
    String output = "Simple";
    String jarFileName = "simple.jar";
    String className = "org.apache.twill.ext.BundledJarRunnerTest$Simple";
    runJarFile(jarFileName, className, new String[]{output});
    Assert.assertEquals(jarOutput, output);
  }

  @Test
  public void runNoZeroArgsConstructorJar() throws Throwable {
    String output = "NoZeroArgsConstructor";
    String jarFileName = "nozeroargs.jar";
    String className = "org.apache.twill.ext.BundledJarRunnerTest$NoZeroArgsConstructor";
    runJarFile(jarFileName, className, new String[]{output});
    Assert.assertEquals(jarOutput, output);
  }

  private void runJarFile(String jarFileName, String className, String[] mainArgs) throws Throwable {
    File jarfile = tempDir.newFile(jarFileName);
    createJarFileFromClass(className, jarfile);
    BundledJarRunner.Arguments args = new BundledJarRunner.Arguments(
        jarFileName, "lib", className, mainArgs);
    BundledJarRunner jarRunner = new BundledJarRunner(jarfile, args);
    jarRunner.load();
    jarRunner.run();
  }

  private void createJarFileFromClass(String className, File jarfile) throws IOException {
    String classAsPath = className.replace(".", "/") + ".class";
    String packagePath = classAsPath.substring(0, classAsPath.lastIndexOf("/") + 1);
    try (
      JarOutputStream jarout = new JarOutputStream(new FileOutputStream(jarfile.getAbsolutePath()))
    ) {
      jarout.putNextEntry(new ZipEntry(packagePath));
      jarout.putNextEntry(new ZipEntry(classAsPath));
      jarout.write(getClassBytes(classAsPath));
      jarout.closeEntry();
    }
  }

  private byte[] getClassBytes(String classAsPath) throws IOException {
    InputStream input = getClass().getClassLoader().getResourceAsStream(classAsPath);
    return ByteStreams.toByteArray(input);
  }
}

