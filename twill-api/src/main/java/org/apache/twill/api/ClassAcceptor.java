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

import java.net.URL;

/**
 * Class that can be used to determine if class can be accepted.
 */
public class ClassAcceptor {
  /**
   * Invoked to determine if class can be accepted. default behavior returns true.
   *
   * @param className Name of the class.
   * @param classUrl URL for the class resource.
   * @param classPathUrl URL for the class path resource that contains the class resource.
   *                     If the URL protocol is {@code file}, it would be the path to root package.
   *                     If the URL protocol is {@code jar}, it would be the jar file.
   * @return true to accept the given class, false otherwise.
   */
  public boolean accept(String className, URL classUrl, URL classPathUrl) {
    return true;
  }
}
