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
package org.apache.twill.internal.yarn;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Wrapper class for AMRMClient for Hadoop version 2.2 or greater.
 */
public final class Hadoop22YarnAMClient extends Hadoop21YarnAMClient {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop22YarnAMClient.class);

  public Hadoop22YarnAMClient(Configuration conf) {
    super(conf);
  }

  @Override
  protected void updateBlacklist(List<String> blacklistAdditions, List<String> blacklistRemovals) {
    LOG.debug("Blacklist Additions: {} , Blacklist Removals: {}", blacklistAdditions, blacklistRemovals);
    amrmClient.updateBlacklist(blacklistAdditions, blacklistRemovals);
  }
}
