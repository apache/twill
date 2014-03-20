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
package org.apache.twill.ext;

import com.google.common.base.Preconditions;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Uses {@link BundledJarRunnable} to run a bundled jar.
 */
public class BundledJarRunnable implements TwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(BundledJarRunnable.class);

  /**
   * Runs the bundled jar.
   */
  private BundledJarRunner jarRunner;

  /**
   * Arguments for running the bundled jar.
   */
  private BundledJarRunner.Arguments arguments;

  @Override
  public void stop() {
    // stop
  }

  @Override
  public void run() {
    Preconditions.checkNotNull(jarRunner);
    Preconditions.checkNotNull(arguments.getMainClassName());
    Preconditions.checkNotNull(arguments.getMainArgs());

    try {
      jarRunner.run();
    } catch (Throwable t) {
      System.exit(1);
    }
  }

  protected void doInitialize(TwillContext context) {
    // NO-OP: left for subclass to implement
  }

  protected BundledJarRunner.Arguments getArguments() {
    return arguments;
  }

  protected void setMainArgs(String[] mainArgs) {
    this.arguments = new BundledJarRunner.Arguments.Builder()
      .from(arguments)
      .setMainArgs(mainArgs)
      .createArguments();
  }

  protected void setMainArgs(String mainArgs) {
    this.setMainArgs(mainArgs.split(" "));
  }

  private String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(getName())
      .noConfigs()
      .build();
  }

  private BundledJarRunner loadJarRunner(File jarFile, BundledJarRunner.Arguments arguments) {
    BundledJarRunner jarRunner = new BundledJarRunner(jarFile, arguments);

    try {
      jarRunner.load();
      return jarRunner;
    } catch (Exception e) {
      LOG.error("Error loading classes into jarRunner", e);
    }

    return null;
  }

  @Override
  public final void initialize(TwillContext context) {
    this.doInitialize(context);

    arguments = BundledJarRunner.Arguments.fromArray(context.getArguments());

    File jarFile = new File(arguments.getJarFileName());
    Preconditions.checkArgument(jarFile != null, "Jar file {} cannot be null", jarFile.getAbsolutePath());
    Preconditions.checkArgument(jarFile.exists(), "Jar file {} must exist", jarFile.getAbsolutePath());
    Preconditions.checkArgument(jarFile.canRead(), "Jar file {} must be readable", jarFile.getAbsolutePath());

    jarRunner = loadJarRunner(jarFile, arguments);
  }

  @Override
  public void handleCommand(org.apache.twill.api.Command command) throws Exception {
    // No-op by default. Left for children class to override.
  }

  @Override
  public void destroy() {
    // No-op by default. Left for children class to override.
  }

}
