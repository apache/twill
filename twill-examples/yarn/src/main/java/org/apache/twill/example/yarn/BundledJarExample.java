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
package org.apache.twill.example.yarn;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.ext.BundledJarRunnable;
import org.apache.twill.ext.BundledJarRunner;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * Demonstrates using BundledJarApplication to run a bundled jar
 * as defined by {@link org.apache.twill.ext.BundledJarRunner}.
 */
public class BundledJarExample {
  public static final Logger LOG = LoggerFactory.getLogger(BundledJarExample.class);

  /**
   * BundledJarApplication that specifies a single instance of main.sample.Scratch
   * to be run from a bundled jar.
   */
  private static class ExampleBundledJarApp implements TwillApplication {
    private final String jarName;
    private final URI jarURI;

    public ExampleBundledJarApp(String jarName, URI jarURI) {
      this.jarName = jarName;
      this.jarURI = jarURI;
    }

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("ExampleBundedJarApp")
        .withRunnable()
        .add("BundledJarRunnable", new BundledJarRunnable())
        .withLocalFiles()
        .add(jarName, jarURI, false)
        .apply()
        .anyOrder()
        .build();
    }
  }

  public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("Arguments format: <host:port of zookeeper server>"
                           + " <bundle jar path> <main class name> <extra args>");
      System.exit(1);
    }

    String zkStr = args[0];
    BundledJarRunner.Arguments arguments = new BundledJarRunner.Arguments(
            args[1], "/lib", args[2], Arrays.copyOfRange(args, 3, args.length));

    File jarFile = new File(arguments.getJarFileName());
    Preconditions.checkState(jarFile.exists());
    Preconditions.checkState(jarFile.canRead());

    final TwillRunnerService twillRunner = new YarnTwillRunnerService(new YarnConfiguration(), zkStr);
    twillRunner.start();


    final TwillController controller = twillRunner.prepare(
      new ExampleBundledJarApp(jarFile.getName(), jarFile.toURI()))
      .withArguments("BundledJarRunnable", arguments.toArray())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          Futures.getUnchecked(controller.terminate());
        } finally {
          twillRunner.stop();
        }
      }
    });

    try {
      controller.awaitTerminated();
    } catch (ExecutionException e) {
      LOG.error("Error", e);
    }
  }
}
