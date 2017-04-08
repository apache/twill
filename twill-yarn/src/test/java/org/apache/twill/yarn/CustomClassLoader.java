/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.yarn;

import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * ClassLoader that generates a new class for the {@link CustomClassLoaderTestRun}.
 */
public final class CustomClassLoader extends URLClassLoader {

  public CustomClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    if (!CustomClassLoaderRunnable.GENERATED_CLASS_NAME.equals(name)) {
      return super.findClass(name);
    }

    // Generate a class that look like this:
    //
    // public class Generated {
    //
    //   public void announce(ServiceAnnouncer announcer, String serviceName, int port) {
    //     announcer.announce(serviceName, port);
    //   }
    // }
    Type generatedClassType = Type.getObjectType(CustomClassLoaderRunnable.GENERATED_CLASS_NAME.replace('.', '/'));
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    cw.visit(Opcodes.V1_7, Opcodes.ACC_PUBLIC + Opcodes.ACC_FINAL,
             generatedClassType.getInternalName(), null, Type.getInternalName(Object.class), null);

    // Generate the default constructor, which just call super();
    Method constructor = new Method("<init>", Type.VOID_TYPE, new Type[0]);
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, constructor, null, null, cw);
    mg.loadThis();
    mg.invokeConstructor(Type.getType(Object.class), constructor);
    mg.returnValue();
    mg.endMethod();

    // Generate the announce method
    Method announce = new Method("announce", Type.VOID_TYPE, new Type[] {
      Type.getType(ServiceAnnouncer.class), Type.getType(String.class), Type.INT_TYPE
    });
    mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, announce, null, null, cw);
    mg.loadArg(0);
    mg.loadArg(1);
    mg.loadArg(2);
    mg.invokeInterface(Type.getType(ServiceAnnouncer.class),
                       new Method("announce", Type.getType(Cancellable.class), new Type[] {
                         Type.getType(String.class), Type.INT_TYPE
                       }));
    mg.pop();
    mg.returnValue();
    mg.endMethod();
    cw.visitEnd();

    byte[] byteCode = cw.toByteArray();
    return defineClass(CustomClassLoaderRunnable.GENERATED_CLASS_NAME, byteCode, 0, byteCode.length);
  }
}
