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

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public final class VersionDetectYarnAMClientFactory implements YarnAMClientFactory {

  private final Configuration conf;

  public VersionDetectYarnAMClientFactory(Configuration conf) {
    this.conf = conf;
  }

  @Override
  @SuppressWarnings("unchecked")
  public YarnAMClient create() {
    try {
      Class<YarnAMClient> clz;
      String clzName;
      switch (YarnUtils.getHadoopVersion()) {
        case HADOOP_20:
          // Uses hadoop-2.0 class
          clzName = getClass().getPackage().getName() + ".Hadoop20YarnAMClient";
          clz = (Class<YarnAMClient>) Class.forName(clzName);
          break;
        case HADOOP_21:
          // Uses hadoop-2.1 class
          clzName = getClass().getPackage().getName() + ".Hadoop21YarnAMClient";
          clz = (Class<YarnAMClient>) Class.forName(clzName);
          break;
        case HADOOP_22:
        case HADOOP_23:
          // Uses hadoop-2.2 class
          clzName = getClass().getPackage().getName() + ".Hadoop22YarnAMClient";
          clz = (Class<YarnAMClient>) Class.forName(clzName);
          break;
        default:
          // Uses hadoop-2.6 or above class
          clzName = getClass().getPackage().getName() + ".Hadoop26YarnAMClient";
          clz = (Class<YarnAMClient>) Class.forName(clzName);
          break;
      }

      return clz.getConstructor(Configuration.class).newInstance(conf);

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
