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
      Class<YarnAppClient> clz;

      if (YarnUtils.getHadoopVersion().equals(YarnUtils.HadoopVersions.HADOOP_20)) {
        // Uses hadoop-2.0 class.
        String clzName = getClass().getPackage().getName() + ".Hadoop20YarnAppClient";
        clz = (Class<YarnAppClient>) Class.forName(clzName);
      } else {
        // Uses hadoop-2.1 class
        String clzName = getClass().getPackage().getName() + ".Hadoop21YarnAppClient";
        clz = (Class<YarnAppClient>) Class.forName(clzName);
      }

      return clz.getConstructor(Configuration.class).newInstance(configuration);

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
