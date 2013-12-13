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
package org.apache.twill.yarn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Server for testing that will die if you give it a 0.
 */
public final class BuggyServer extends SocketServer {

  private static final Logger LOG = LoggerFactory.getLogger(BuggyServer.class);

  @Override
  public void handleRequest(BufferedReader reader, PrintWriter writer) throws IOException {
    String line = reader.readLine();
    LOG.info("Received: " + line + " going to divide by it");
    Integer toDivide = Integer.valueOf(line);
    writer.println(Integer.toString(100 / toDivide));
  }
}
