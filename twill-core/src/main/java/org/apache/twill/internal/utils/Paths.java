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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * A utility class for manipulating paths.
 */
public final class Paths {

  /**
   * Returns a new path by appending an extension.
   *
   * @param extractFrom the path to extract the extension from
   * @param appendTo the path to append the extension to
   */
  public static String addExtension(String extractFrom, String appendTo) {
    String suffix = getExtension(extractFrom);
    if (!suffix.isEmpty()) {
      return appendTo + '.' + suffix;
    }
    return appendTo;
  }

  /**
   * Returns the file extension of the given file path. The file extension is defined by the suffix after the last
   * dot character {@code .}. If there is no dot, an empty string will be returned.
   */
  public static String getExtension(String path) {
    if (path.endsWith(".tar.gz")) {
      return "tar.gz";
    }

    int idx = path.lastIndexOf('.');
    return (idx >= 0) ? path.substring(idx + 1) : "";
  }

  /**
   * Deletes the given path. If the path represents a directory, the content of it will be emptied recursively,
   * followed by the removal of the directory itself.
   */
  public static void deleteRecursively(Path path) throws IOException {
    Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException ex) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  private Paths() {
  }
}
