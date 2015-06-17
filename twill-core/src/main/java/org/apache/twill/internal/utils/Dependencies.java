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
package org.apache.twill.internal.utils;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.twill.api.ClassAcceptor;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.TypePath;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Queue;
import java.util.Set;

/**
 * Utility class to help find out class dependencies.
 */
public final class Dependencies {

  public static void findClassDependencies(ClassLoader classLoader,
                                           ClassAcceptor acceptor,
                                           String...classesToResolve) throws IOException {
    findClassDependencies(classLoader, acceptor, ImmutableList.copyOf(classesToResolve));
  }

  /**
   * Finds the class dependencies of the given class.
   * @param classLoader ClassLoader for finding class bytecode.
   * @param acceptor Predicate to accept a found class and its bytecode.
   * @param classesToResolve Classes for looking for dependencies.
   * @throws IOException Thrown where there is error when loading in class bytecode.
   */
  public static void findClassDependencies(ClassLoader classLoader,
                                           ClassAcceptor acceptor,
                                           Iterable<String> classesToResolve) throws IOException {

    final Set<String> seenClasses = Sets.newHashSet(classesToResolve);
    final Queue<String> classes = Lists.newLinkedList(classesToResolve);

    // Breadth-first-search classes dependencies.
    while (!classes.isEmpty()) {
      String className = classes.remove();
      URL classUrl = getClassURL(className, classLoader);
      if (classUrl == null) {
        continue;
      }

      // Call the accept to see if it accept the current class.
      if (!acceptor.accept(className, classUrl, getClassPathURL(className, classUrl))) {
        continue;
      }

      try (InputStream is = classUrl.openStream()) {
        // Visit the bytecode to lookup classes that the visiting class is depended on.
        new ClassReader(ByteStreams.toByteArray(is)).accept(new DependencyClassVisitor(new DependencyAcceptor() {
          @Override
          public void accept(String className) {
            // See if the class is accepted
            if (seenClasses.add(className)) {
              classes.add(className);
            }
          }
        }), ClassReader.SKIP_DEBUG + ClassReader.SKIP_FRAMES);
      }
    }
  }

  /**
   * Returns the URL for loading the class bytecode of the given class, or null if it is not found or if it is
   * a system class.
   */
  private static URL getClassURL(String className, ClassLoader classLoader) {
    String resourceName = className.replace('.', '/') + ".class";
    return classLoader.getResource(resourceName);
  }

  private static URL getClassPathURL(String className, URL classUrl) {
    try {
      if ("file".equals(classUrl.getProtocol())) {
        String path = classUrl.getFile();
        // Compute the directory container the class.
        int endIdx = path.length() - className.length() - ".class".length();
        if (endIdx > 1) {
          // If it is not the root directory, return the end index to remove the trailing '/'.
          endIdx--;
        }
        return new URL("file", "", -1, path.substring(0, endIdx));
      }
      if ("jar".equals(classUrl.getProtocol())) {
        String path = classUrl.getFile();
        return URI.create(path.substring(0, path.indexOf("!/"))).toURL();
      }
    } catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
    throw new IllegalStateException("Unsupported class URL: " + classUrl);
  }

  /**
   * A private interface for accepting a dependent class that is found during bytecode inspection.
   */
  private interface DependencyAcceptor {
    void accept(String className);
  }

  /**
   * ASM ClassVisitor for extracting classes dependencies.
   */
  private static final class DependencyClassVisitor extends ClassVisitor {

    private final SignatureVisitor signatureVisitor;
    private final AnnotationVisitor annotationVisitor;
    private final DependencyAcceptor acceptor;

    public DependencyClassVisitor(DependencyAcceptor acceptor) {
      super(Opcodes.ASM5);
      this.acceptor = acceptor;
      this.signatureVisitor = createSignatureVisitor();
      this.annotationVisitor = createAnnotationVisitor();
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      addClass(name);

      if (signature != null) {
        new SignatureReader(signature).accept(signatureVisitor);
      } else {
        addClass(superName);
        addClasses(interfaces);
      }
    }

    @Override
    public void visitOuterClass(String owner, String name, String desc) {
      addClass(owner);
    }

    @Override
    public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
      if (!visible) {
        return null;
      }
      addType(Type.getType(desc));
      return annotationVisitor;
    }

    @Override
    public void visitInnerClass(String name, String outerName, String innerName, int access) {
      addClass(name);
    }

    @Override
    public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
      if (signature != null) {
        new SignatureReader(signature).acceptType(signatureVisitor);
      } else {
        addType(Type.getType(desc));
      }

      return new FieldVisitor(Opcodes.ASM5) {
        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
          if (!visible) {
            return null;
          }
          addType(Type.getType(desc));
          return annotationVisitor;
        }
      };
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
      if (signature != null) {
        new SignatureReader(signature).accept(signatureVisitor);
      } else {
        addMethod(desc);
      }
      addClasses(exceptions);

      return new MethodVisitor(Opcodes.ASM5) {

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
          if (!visible) {
            return null;
          }
          addType(Type.getType(desc));
          return annotationVisitor;
        }

        @Override
        public AnnotationVisitor visitParameterAnnotation(int parameter, String desc, boolean visible) {
          if (!visible) {
            return null;
          }
          addType(Type.getType(desc));
          return annotationVisitor;
        }

        @Override
        public void visitTypeInsn(int opcode, String type) {
          addType(Type.getObjectType(type));
        }

        @Override
        public void visitFieldInsn(int opcode, String owner, String name, String desc) {
          addType(Type.getObjectType(owner));
          addType(Type.getType(desc));
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
          addType(Type.getObjectType(owner));
          addMethod(desc);
        }

        @Override
        public void visitLdcInsn(Object cst) {
          if (cst instanceof Type) {
            addType((Type) cst);
          }
        }

        @Override
        public AnnotationVisitor visitLocalVariableAnnotation(int typeRef, TypePath typePath, Label[] start,
                                                              Label[] end, int[] index, String desc, boolean visible) {
          if (!visible) {
            return null;
          }
          addType(Type.getType(desc));
          return annotationVisitor;
        }

        @Override
        public void visitMultiANewArrayInsn(String desc, int dims) {
          addType(Type.getType(desc));
        }

        @Override
        public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
          if (signature != null) {
            new SignatureReader(signature).acceptType(signatureVisitor);
          } else {
            addType(Type.getType(desc));
          }
        }
      };
    }

    private void addClass(String internalName) {
      if (internalName == null || internalName.startsWith("java/")) {
        return;
      }
      acceptor.accept(Type.getObjectType(internalName).getClassName());
    }

    private void addClasses(String[] classes) {
      if (classes != null) {
        for (String clz : classes) {
          addClass(clz);
        }
      }
    }

    private void addType(Type type) {
      if (type.getSort() == Type.ARRAY) {
        type = type.getElementType();
      }
      if (type.getSort() == Type.OBJECT) {
        addClass(type.getInternalName());
      }
    }

    private void addMethod(String desc) {
      addType(Type.getReturnType(desc));
      for (Type type : Type.getArgumentTypes(desc)) {
        addType(type);
      }
    }

    /**
     * Creates a {@link SignatureVisitor} for gathering dependency information from class signature.
     */
    private SignatureVisitor createSignatureVisitor() {
      return new SignatureVisitor(Opcodes.ASM5) {
        private String currentClass;

        @Override
        public void visitClassType(String name) {
          currentClass = name;
          addClass(name);
        }

        @Override
        public void visitInnerClassType(String name) {
          addClass(currentClass + "$" + name);
        }
      };
    }

    /**
     * Creates an {@link AnnotationVisitor} for gathering dependency information from annotations.
     */
    private AnnotationVisitor createAnnotationVisitor() {
      return new AnnotationVisitor(Opcodes.ASM5) {
        @Override
        public void visit(String name, Object value) {
          if (value instanceof Type) {
            addType((Type) value);
          }
        }

        @Override
        public AnnotationVisitor visitAnnotation(String name, String desc) {
          addType(Type.getType(desc));
          return this;
        }

        @Override
        public AnnotationVisitor visitArray(String name) {
          return this;
        }

        @Override
        public void visitEnum(String name, String desc, String value) {
          addType(Type.getType(desc));
        }
      };
    }
  }

  private Dependencies() {
  }
}
