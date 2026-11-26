/*
 * Copyright © 2026 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.bigtable.common;

import com.google.bigtable.repackaged.io.grpc.StatusRuntimeException;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.cdap.plugin.gcp.bigtable.source.BigtableSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Verifies that unused HBase Jetty connector classes are stripped from hbase-shaded-server without breaking
 * Cloud Bigtable client or MapReduce classloading.
 *
 * <p>The packaged plugin bundle is verified by the package-phase antrun execution in pom.xml, and the
 * redundant hbase-shaded-client dependency is banned by maven-enforcer-plugin.
 */
public class BigtableShadedJarPackagingTest {
  private static final int CLASS_FILE_MAGIC = 0xCAFEBABE;
  private static final Pattern DESCRIPTOR_CLASS_PATTERN = Pattern.compile("L([a-zA-Z0-9_$/]+)[;<]");
  private static final Set<String> EXPECTED_REMOVED_CLASSES =
    Collections.singleton("org/apache/hadoop/hbase/jetty/SslSelectChannelConnectorSecure");
  private static final List<String> AUDITED_HBASE_PACKAGES = ImmutableList.of(
    "org/apache/hadoop/hbase/client/",
    "org/apache/hadoop/hbase/mapreduce/",
    "org/apache/hadoop/hbase/io/",
    "org/apache/hadoop/hbase/util/",
    "org/apache/hadoop/hbase/filter/",
    "org/apache/hadoop/hbase/shaded/",
    "org/apache/hadoop/hbase/security/"
  );
  private static final List<String> CLASSES_TO_LOAD_AND_INITIALIZE = ImmutableList.of(
    "io.cdap.plugin.gcp.bigtable.source.BigtableSource",
    "io.cdap.plugin.gcp.bigtable.sink.BigtableSink",
    "io.cdap.plugin.gcp.bigtable.source.BigtableInputFormat",
    "io.cdap.plugin.gcp.bigtable.sink.BigtableOutputFormat",
    "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
    "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
    "org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil",
    "org.apache.hadoop.hbase.HBaseConfiguration",
    "org.apache.hadoop.hbase.client.Put",
    "org.apache.hadoop.hbase.client.Result",
    "org.apache.hadoop.hbase.client.Scan"
  );

  @Test
  public void testStrippedHBaseJarHasNoJettyAndNoDanglingRefs() throws Exception {
    File strippedJar = findStrippedHBaseJar();
    File upstreamJar = new File(Put.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    Assert.assertTrue("Upstream hbase-shaded-server JAR should exist: " + upstreamJar, upstreamJar.isFile());
    Assert.assertTrue("Expected Put to resolve from hbase-shaded-server, got: " + upstreamJar.getName(),
                      upstreamJar.getName().startsWith("hbase-shaded-server-"));

    try (ZipFile zipFile = new ZipFile(strippedJar)) {
      List<String> forbiddenEntries = findForbiddenJettyEntries(zipFile);
      Assert.assertTrue("Unexpected Jetty/Mortbay entries in stripped JAR: " + forbiddenEntries,
                        forbiddenEntries.isEmpty());

      Set<String> removedClasses = new HashSet<>(collectClassNames(upstreamJar));
      removedClasses.removeAll(collectClassNames(strippedJar));
      Assert.assertEquals(EXPECTED_REMOVED_CLASSES, removedClasses);

      List<ZipEntry> auditedEntries = findAuditedHBaseClassEntries(zipFile);
      Assert.assertFalse("Expected HBase classes to audit in " + strippedJar, auditedEntries.isEmpty());
      List<String> danglingRefs = findDanglingReferences(zipFile, auditedEntries, removedClasses);
      Assert.assertTrue("Dangling references to removed classes: " + danglingRefs, danglingRefs.isEmpty());
    }
  }

  @Test
  public void testBigtableAndHBaseClassesLoadFromStrippedJar() throws Exception {
    File strippedJar = findStrippedHBaseJar();
    URL strippedUrl = strippedJar.toURI().toURL();
    Set<URL> isolatedUrls = new LinkedHashSet<>();
    isolatedUrls.add(strippedUrl);
    isolatedUrls.add(BigtableConfiguration.class.getProtectionDomain().getCodeSource().getLocation());
    isolatedUrls.add(StatusRuntimeException.class.getProtectionDomain().getCodeSource().getLocation());
    isolatedUrls.add(BigtableSource.class.getProtectionDomain().getCodeSource().getLocation());

    try (IsolatedHBaseClassLoader loader = new IsolatedHBaseClassLoader(
      isolatedUrls.toArray(new URL[0]), BigtableShadedJarPackagingTest.class.getClassLoader())) {
      assertClassAbsent(loader, "org.apache.hadoop.hbase.shaded.org.mortbay.util.LazyList");
      assertClassAbsent(loader, "org.apache.hadoop.hbase.jetty.SslSelectChannelConnectorSecure");

      URL hbaseDefaults = loader.getResource("hbase-default.xml");
      Assert.assertNotNull("hbase-default.xml should be in the stripped JAR", hbaseDefaults);
      Assert.assertEquals("jar:" + strippedUrl + "!/hbase-default.xml", hbaseDefaults.toString());

      for (String className : CLASSES_TO_LOAD_AND_INITIALIZE) {
        Class.forName(className, true, loader);
      }

      Class<?> btInputFormatClass = Class.forName("io.cdap.plugin.gcp.bigtable.source.BigtableInputFormat",
                                                  true, loader);
      Assert.assertNotNull(btInputFormatClass.getDeclaredConstructor().newInstance());

      Class<?> btOutputFormatClass = Class.forName("io.cdap.plugin.gcp.bigtable.sink.BigtableOutputFormat",
                                                   true, loader);
      Assert.assertNotNull(btOutputFormatClass.getDeclaredConstructor().newInstance());

      Class<?> hbaseConfClass = Class.forName("org.apache.hadoop.hbase.HBaseConfiguration", true, loader);
      Configuration conf = (Configuration) hbaseConfClass.getMethod("create").invoke(null);
      Assert.assertNotNull(conf.get("hbase.defaults.for.version"));

      Class<?> btConfClass = Class.forName("com.google.cloud.bigtable.hbase.BigtableConfiguration", true, loader);
      btConfClass.getMethod("configure", Configuration.class, String.class, String.class)
        .invoke(null, conf, "test-project", "test-instance");

      byte[] rowKey = "row-1".getBytes(StandardCharsets.UTF_8);
      byte[] family = "cf".getBytes(StandardCharsets.UTF_8);
      byte[] qualifier = "col".getBytes(StandardCharsets.UTF_8);
      byte[] value = "val".getBytes(StandardCharsets.UTF_8);

      Class<?> putClass = Class.forName("org.apache.hadoop.hbase.client.Put", true, loader);
      Assert.assertNotSame(Put.class, putClass);
      Assert.assertEquals(strippedUrl, putClass.getProtectionDomain().getCodeSource().getLocation());
      Object put = putClass.getConstructor(byte[].class).newInstance((Object) rowKey);
      putClass.getMethod("addColumn", byte[].class, byte[].class, byte[].class)
        .invoke(put, family, qualifier, value);
      boolean hasCell = (Boolean) putClass.getMethod("has", byte[].class, byte[].class, byte[].class)
        .invoke(put, family, qualifier, value);
      Assert.assertTrue(hasCell);

      Class<?> resultClass = Class.forName("org.apache.hadoop.hbase.client.Result", true, loader);
      Assert.assertNotNull(resultClass.getField("EMPTY_RESULT").get(null));

      Class<?> scanClass = Class.forName("org.apache.hadoop.hbase.client.Scan", true, loader);
      Object scan = scanClass.getDeclaredConstructor().newInstance();
      scanClass.getMethod("addColumn", byte[].class, byte[].class).invoke(scan, family, qualifier);

      Class<?> tmrUtilClass = Class.forName("org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil", true, loader);
      Method convertScanToString = tmrUtilClass.getMethod("convertScanToString", scanClass);
      String encodedScan = (String) convertScanToString.invoke(null, scan);
      Assert.assertNotNull(encodedScan);
      Assert.assertFalse(encodedScan.isEmpty());
    }
  }

  private static void assertClassAbsent(ClassLoader loader, String className) {
    Assert.assertThrows("Expected class to be absent from JAR: " + className, ClassNotFoundException.class,
                        () -> Class.forName(className, false, loader));
  }

  private static List<String> findForbiddenJettyEntries(ZipFile zipFile) {
    List<String> forbiddenEntries = new ArrayList<>();
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    while (entries.hasMoreElements()) {
      String name = entries.nextElement().getName();
      if (isForbiddenJettyEntry(name)) {
        forbiddenEntries.add(name);
      }
    }
    return forbiddenEntries;
  }

  private static List<ZipEntry> findAuditedHBaseClassEntries(ZipFile zipFile) {
    List<ZipEntry> auditedEntries = new ArrayList<>();
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      if (entry.getName().endsWith(".class") && startsWithAny(entry.getName(), AUDITED_HBASE_PACKAGES)) {
        auditedEntries.add(entry);
      }
    }
    return auditedEntries;
  }

  private static List<String> findDanglingReferences(ZipFile zipFile, List<ZipEntry> classEntries,
                                                     Set<String> removedClasses) throws IOException {
    List<String> danglingRefs = new ArrayList<>();
    for (ZipEntry entry : classEntries) {
      byte[] classBytes;
      try (InputStream in = zipFile.getInputStream(entry)) {
        classBytes = ByteStreams.toByteArray(in);
      }
      for (String ref : extractConstantPoolClassReferences(classBytes)) {
        if (removedClasses.contains(ref)) {
          danglingRefs.add(entry.getName() + " -> " + ref);
        }
      }
    }
    return danglingRefs;
  }

  private static boolean isForbiddenJettyEntry(String entryName) {
    String lower = entryName.toLowerCase(Locale.ROOT);
    if (!lower.contains("jetty") && !lower.contains("mortbay")) {
      return false;
    }
    return !(entryName.endsWith(".class") && isAllowedNettyOrGrpcTlsWrapper(entryName));
  }

  private static boolean isAllowedNettyOrGrpcTlsWrapper(String entryName) {
    return entryName.contains("io/netty/handler/ssl/JettyAlpnSslEngine")
      || entryName.contains("io/netty/handler/ssl/JettyNpnSslEngine")
      || entryName.contains("io/grpc/netty/JettyTlsUtil");
  }

  private static File findStrippedHBaseJar() {
    File targetDir = findTargetDirectory();
    File strippedLibDir = new File(targetDir, "stripped-hbase/lib");
    File[] matches = strippedLibDir.listFiles(
      (dir, name) -> name.startsWith("hbase-shaded-server-") && name.endsWith(".jar"));
    Assert.assertNotNull("Missing stripped-hbase/lib directory under " + targetDir, matches);
    Assert.assertEquals("Expected exactly one stripped hbase-shaded-server JAR", 1, matches.length);
    return matches[0];
  }

  private static File findTargetDirectory() {
    String tmpDir = System.getProperty("java.io.tmpdir");
    if (tmpDir != null) {
      File candidate = new File(tmpDir);
      if ("target".equals(candidate.getName()) && candidate.isDirectory()) {
        return candidate;
      }
    }
    File cwdTarget = new File("target");
    Assert.assertTrue("Unable to locate Maven target directory", cwdTarget.isDirectory());
    return cwdTarget;
  }

  private static Set<String> collectClassNames(File jarFile) throws IOException {
    Set<String> classes = new HashSet<>();
    try (ZipFile zipFile = new ZipFile(jarFile)) {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        String name = entries.nextElement().getName();
        if (name.endsWith(".class")) {
          classes.add(name.substring(0, name.length() - ".class".length()));
        }
      }
    }
    return classes;
  }

  private static boolean startsWithAny(String value, List<String> prefixes) {
    for (String prefix : prefixes) {
      if (value.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the internal names of all classes referenced from the constant pool of a class file, either as
   * class constants or inside field, method, and generic signatures.
   */
  private static Set<String> extractConstantPoolClassReferences(byte[] classBytes) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(classBytes));
    if (in.readInt() != CLASS_FILE_MAGIC) {
      throw new IOException("Not a Java class file");
    }
    in.readUnsignedShort(); // minor_version
    in.readUnsignedShort(); // major_version
    int constantPoolCount = in.readUnsignedShort();

    Map<Integer, String> utf8Entries = new HashMap<>();
    List<Integer> classNameIndices = new ArrayList<>();
    int index = 1;
    while (index < constantPoolCount) {
      index += readConstantPoolEntry(in, index, utf8Entries, classNameIndices);
    }

    Set<String> referenced = new HashSet<>();
    for (int nameIndex : classNameIndices) {
      addClassConstantReference(utf8Entries.get(nameIndex), referenced);
    }
    for (String utf8 : utf8Entries.values()) {
      addSignatureReferences(utf8, referenced);
    }
    return referenced;
  }

  /**
   * Reads one constant pool entry, recording Utf8 values and Class name indices.
   *
   * @return the number of constant pool slots the entry occupies
   */
  private static int readConstantPoolEntry(DataInputStream in, int index, Map<Integer, String> utf8Entries,
                                           List<Integer> classNameIndices) throws IOException {
    int tag = in.readUnsignedByte();
    switch (tag) {
      case 1: // CONSTANT_Utf8
        utf8Entries.put(index, in.readUTF());
        return 1;
      case 7: // CONSTANT_Class
        classNameIndices.add(in.readUnsignedShort());
        return 1;
      case 8: // CONSTANT_String
      case 16: // CONSTANT_MethodType
      case 19: // CONSTANT_Module
      case 20: // CONSTANT_Package
        in.skipBytes(2);
        return 1;
      case 15: // CONSTANT_MethodHandle
        in.skipBytes(3);
        return 1;
      case 3: // CONSTANT_Integer
      case 4: // CONSTANT_Float
      case 9: // CONSTANT_Fieldref
      case 10: // CONSTANT_Methodref
      case 11: // CONSTANT_InterfaceMethodref
      case 12: // CONSTANT_NameAndType
      case 17: // CONSTANT_Dynamic
      case 18: // CONSTANT_InvokeDynamic
        in.skipBytes(4);
        return 1;
      case 5: // CONSTANT_Long
      case 6: // CONSTANT_Double
        in.skipBytes(8);
        return 2;
      default:
        throw new IOException("Unsupported constant pool tag: " + tag);
    }
  }

  private static void addClassConstantReference(String className, Set<String> referenced) {
    if (className == null) {
      return;
    }
    if (!className.startsWith("[")) {
      referenced.add(className);
      return;
    }
    // Array class constants use descriptor syntax, for example "[Lcom/example/Foo;".
    int objectStart = className.indexOf('L');
    if (objectStart >= 0 && className.endsWith(";")) {
      referenced.add(className.substring(objectStart + 1, className.length() - 1));
    }
  }

  private static void addSignatureReferences(String utf8, Set<String> referenced) {
    boolean looksLikeSignature = utf8.indexOf('(') >= 0
      || utf8.startsWith("L") || utf8.startsWith("[") || utf8.startsWith("<");
    if (utf8.indexOf('/') < 0 || !looksLikeSignature) {
      return;
    }
    Matcher matcher = DESCRIPTOR_CLASS_PATTERN.matcher(utf8);
    while (matcher.find()) {
      referenced.add(matcher.group(1));
    }
  }

  /**
   * Loads HBase and Bigtable classes and HBase resources only from the given URLs, so the test exercises the
   * stripped JAR instead of the unstripped hbase-shaded-server JAR on the test classpath.
   */
  private static final class IsolatedHBaseClassLoader extends URLClassLoader {
    IsolatedHBaseClassLoader(URL[] urls, ClassLoader parent) {
      super(urls, parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          if (isIsolatedPackage(name)) {
            loaded = findClass(name);
          } else {
            loaded = super.loadClass(name, false);
          }
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }

    @Override
    public URL getResource(String name) {
      return isIsolatedResource(name) ? findResource(name) : super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
      return isIsolatedResource(name) ? findResources(name) : super.getResources(name);
    }

    private static boolean isIsolatedPackage(String name) {
      return name.startsWith("org.apache.hadoop.hbase.")
        || name.startsWith("org.apache.hbase.")
        || name.startsWith("com.google.cloud.bigtable.")
        || name.startsWith("com.google.bigtable.")
        || name.startsWith("io.cdap.plugin.gcp.bigtable.");
    }

    private static boolean isIsolatedResource(String name) {
      return name.startsWith("org/apache/hadoop/hbase/")
        || name.equals("hbase-default.xml")
        || name.equals("hbase-site.xml")
        || name.startsWith("META-INF/services/org.apache.hadoop.hbase.");
    }
  }
}
