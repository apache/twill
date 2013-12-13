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
package org.apache.twill.internal.kafka;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Properties;

/**
 *
 */
public final class EmbeddedKafkaServer extends AbstractIdleService {

  private static final String KAFAK_CONFIG_CLASS = "kafka.server.KafkaConfig";
  private static final String KAFKA_SERVER_CLASS = "kafka.server.KafkaServerStartable";

  private final Object server;

  public EmbeddedKafkaServer(File kafkaDir, Properties properties) {
    this(createClassLoader(kafkaDir), properties);
  }

  public EmbeddedKafkaServer(ClassLoader classLoader, Properties properties) {
    try {
      Class<?> configClass = classLoader.loadClass(KAFAK_CONFIG_CLASS);
      Object config = configClass.getConstructor(Properties.class).newInstance(properties);

      Class<?> serverClass = classLoader.loadClass(KAFKA_SERVER_CLASS);
      server = serverClass.getConstructor(configClass).newInstance(config);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void startUp() throws Exception {
    server.getClass().getMethod("startup").invoke(server);
  }

  @Override
  protected void shutDown() throws Exception {
    server.getClass().getMethod("shutdown").invoke(server);
    server.getClass().getMethod("awaitShutdown").invoke(server);
  }

  private static ClassLoader createClassLoader(File kafkaDir) {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader thisClassLoader = EmbeddedKafkaServer.class.getClassLoader();
    ClassLoader parent = contextClassLoader != null
                            ? contextClassLoader
                            : thisClassLoader != null
                                ? thisClassLoader : ClassLoader.getSystemClassLoader();

    return new URLClassLoader(findJars(kafkaDir, Lists.<URL>newArrayList()).toArray(new URL[0]), parent);
  }

  private static List<URL> findJars(File dir, List<URL> urls) {
    try {
      for (File file : dir.listFiles()) {
        if (file.isDirectory()) {
          findJars(file, urls);
        } else if (file.getName().endsWith(".jar")) {
          urls.add(file.toURI().toURL());
        }
      }
      return urls;
    } catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
  }
}
