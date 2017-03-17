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
 * Helper factory class to return the right instance of {@link YarnAppClient} based on Apache Hadoop version.
 */
public final class VersionDetectYarnAppClientFactory implements YarnAppClientFactory {

  @Override
  @SuppressWarnings("unchecked")
  public YarnAppClient create(Configuration configuration) {
    try {
      String clzName;
      switch (YarnUtils.getHadoopVersion()) {
        case HADOOP_20:
          clzName = getClass().getPackage().getName() + ".Hadoop20YarnAppClient";
          break;
        case HADOOP_21:
        case HADOOP_22:
          // 2.1 and 2.2 uses the same YarnAppClient
          clzName = getClass().getPackage().getName() + ".Hadoop21YarnAppClient";
          break;
        default:
          // 2.3 and above uses the 2.3 YarnAppClient to support RM HA
          clzName = getClass().getPackage().getName() + ".Hadoop23YarnAppClient";
      }
      Class<YarnAppClient> clz = (Class<YarnAppClient>) Class.forName(clzName);
      return clz.getConstructor(Configuration.class).newInstance(configuration);

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
