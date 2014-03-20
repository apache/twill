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
package org.apache.twill.api;

import java.net.URI;
import javax.annotation.Nullable;

/**
 * This interface represents a local file that will be available for the container running a {@link TwillRunnable}.
 */
public interface LocalFile {

  String getName();

  URI getURI();

  /**
   * Returns the the last modified time of the file or {@code -1} if unknown.
   */
  long getLastModified();

  /**
   * Returns the size of the file or {@code -1} if unknown.
   */
  long getSize();

  /**
   * Indicates whether this file is an archive. If true, the file is expanded after being copied to the container host.
   */
  boolean isArchive();

  @Nullable
  String getPattern();
}
