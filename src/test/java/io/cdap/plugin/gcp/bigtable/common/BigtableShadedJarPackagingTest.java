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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Verifies that unused relocated Jetty classes and metadata are stripped from the packaged
 * HBase MapReduce JAR without breaking Cloud Bigtable client or MapReduce classloading.
 *
 * <p>The final plugin bundle is checked during the Maven package phase in pom.xml, because unit
 * tests run before the bundle is built.
 */
public class BigtableShadedJarPackagingTest {
  private static final String CLASS_FILE_SUFFIX = ".class";
  private static final int CLASS_FILE_MAGIC = 0xCAFEBABE;
  private static final Pattern DESCRIPTOR_CLASS_PATTERN = Pattern.compile("L([a-zA-Z0-9_$/]+)[;<]");
  private static final String HBASE_HTTP_PACKAGE = "org/apache/hadoop/hbase/http/";
  private static final List<String> AUDITED_HBASE_PACKAGES = ImmutableList.of(
    "org/apache/hadoop/hbase/client/",
    "org/apache/hadoop/hbase/mapreduce/",
    "org/apache/hadoop/hbase/io/",
    "org/apache/hadoop/hbase/util/",
    "org/apache/hadoop/hbase/filter/",
    "org/apache/hadoop/hbase/shaded/",
    "org/apache/hadoop/hbase/security/"
  );
  private static final List<String> EXCLUDED_HBASE_PREFIXES = ImmutableList.of(
    "META-INF/maven/org.apache.hbase/hbase-http/",
    "org/apache/hadoop/hbase/master/http/",
    "org/apache/hadoop/hbase/regionserver/http/",
    "org/apache/hadoop/hbase/generated/",
    "org/apache/hadoop/hbase/tool/Canary"
  );
  private static final List<String> ALLOWED_HBASE_HTTP_ENTRIES = ImmutableList.of(
    HBASE_HTTP_PACKAGE,
    HBASE_HTTP_PACKAGE + "HttpServer.class"
  );

  @Test
  public void testStrippedHBaseJarHasNoJettyAndNoDanglingRefs() throws Exception {
    File strippedJar = findStrippedHBaseJar();
    File upstreamJar = new File(Put.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    Assert.assertTrue("Upstream hbase-shaded-mapreduce JAR should exist: " + upstreamJar, upstreamJar.isFile());

    try (JarInputStream strippedStream = new JarInputStream(new FileInputStream(strippedJar));
         JarInputStream upstreamStream = new JarInputStream(new FileInputStream(upstreamJar))) {
      Manifest strippedManifest = strippedStream.getManifest();
      Assert.assertNotNull("Stripped JAR must keep META-INF/MANIFEST.MF first for JarInputStream",
                           strippedManifest);
      Assert.assertEquals("Stripped JAR manifest should match upstream manifest",
                          upstreamStream.getManifest(), strippedManifest);
    }

    try (ZipFile zipFile = new ZipFile(strippedJar)) {
      List<String> entryNames = new ArrayList<>();
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        entryNames.add(entries.nextElement().getName());
      }

      List<String> forbiddenEntries = new ArrayList<>();
      for (String name : entryNames) {
        if (isForbiddenJettyEntry(name) || startsWithAny(name, EXCLUDED_HBASE_PREFIXES)
          || (name.startsWith(HBASE_HTTP_PACKAGE) && !ALLOWED_HBASE_HTTP_ENTRIES.contains(name))) {
          forbiddenEntries.add(name);
        }
      }
      Assert.assertTrue("Unexpected stripped entries in JAR: " + forbiddenEntries, forbiddenEntries.isEmpty());

      Set<String> strippedClasses = toClassNames(entryNames);
      Assert.assertTrue("Expected >20000 classes in stripped JAR, actual: " + strippedClasses.size(),
                        strippedClasses.size() > 20000);

      Set<String> removedClasses = collectClassNames(upstreamJar);
      removedClasses.removeAll(strippedClasses);
      Assert.assertTrue("Expected >1000 Jetty/HTTP classes removed, actual: " + removedClasses.size(),
                        removedClasses.size() > 1000);

      int auditedClassCount = 0;
      List<String> danglingRefs = new ArrayList<>();
      for (String name : entryNames) {
        if (!name.endsWith(CLASS_FILE_SUFFIX) || !startsWithAny(name, AUDITED_HBASE_PACKAGES)) {
          continue;
        }
        auditedClassCount++;
        for (String ref : extractConstantPoolClassReferences(readEntry(zipFile, name))) {
          if (removedClasses.contains(ref)) {
            danglingRefs.add(name + " -> " + ref);
          }
        }
      }
      Assert.assertTrue("Expected audited HBase classes, actual: " + auditedClassCount, auditedClassCount > 0);
      Assert.assertTrue("Dangling references to removed classes: " + danglingRefs, danglingRefs.isEmpty());
    }
  }

  @Test
  public void testBigtableAndHBaseClassesLoadFromStrippedJar() throws Exception {
    File strippedJar = findStrippedHBaseJar();
    File upstreamJar = new File(Put.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    URL strippedUrl = strippedJar.toURI().toURL();
    Set<URL> isolatedUrls = new LinkedHashSet<>();
    isolatedUrls.add(strippedUrl);
    isolatedUrls.add(BigtableConfiguration.class.getProtectionDomain().getCodeSource().getLocation());
    isolatedUrls.add(StatusRuntimeException.class.getProtectionDomain().getCodeSource().getLocation());
    isolatedUrls.add(BigtableSource.class.getProtectionDomain().getCodeSource().getLocation());

    try (IsolatedHBaseClassLoader loader = new IsolatedHBaseClassLoader(
      isolatedUrls.toArray(new URL[0]), BigtableShadedJarPackagingTest.class.getClassLoader())) {
      assertClassAbsent(loader, "org.apache.hbase.thirdparty.org.eclipse.jetty.server.Server");
      assertClassAbsent(loader, "org.apache.hadoop.hbase.http.InfoServer");

      URL hbaseDefaults = loader.getResource("hbase-default.xml");
      Assert.assertNotNull(hbaseDefaults);
      Assert.assertTrue("hbase-default.xml should load from the stripped JAR: " + hbaseDefaults,
                        hbaseDefaults.toString().startsWith("jar:" + strippedUrl + "!/"));

      List<String> classesToLoadAndInitialize = ImmutableList.of(
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
      for (String className : classesToLoadAndInitialize) {
        Class.forName(className, true, loader);
      }

      Class<?> bigtableInputFormatClass =
        Class.forName("io.cdap.plugin.gcp.bigtable.source.BigtableInputFormat", true, loader);
      Assert.assertNotNull(bigtableInputFormatClass.getDeclaredConstructor().newInstance());

      Class<?> bigtableOutputFormatClass =
        Class.forName("io.cdap.plugin.gcp.bigtable.sink.BigtableOutputFormat", true, loader);
      Assert.assertNotNull(bigtableOutputFormatClass.getDeclaredConstructor().newInstance());

      Class<?> hbaseConfClass = Class.forName("org.apache.hadoop.hbase.HBaseConfiguration", true, loader);
      Configuration conf = (Configuration) hbaseConfClass.getMethod("create").invoke(null);
      Assert.assertNotNull(conf);

      Class<?> bigtableConfClass =
        Class.forName("com.google.cloud.bigtable.hbase.BigtableConfiguration", true, loader);
      bigtableConfClass.getMethod("configure", Configuration.class, String.class, String.class)
        .invoke(null, conf, "test-project", "test-instance");
      Assert.assertEquals("test-project", conf.get("google.bigtable.project.id"));
      Assert.assertEquals("test-instance", conf.get("google.bigtable.instance.id"));

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

      Class<?> tableMapReduceUtilClass =
        Class.forName("org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil", true, loader);
      Method convertScanToString = tableMapReduceUtilClass.getMethod("convertScanToString", scanClass);
      String encodedScan = (String) convertScanToString.invoke(null, scan);
      Assert.assertNotNull(encodedScan);
      Assert.assertFalse(encodedScan.isEmpty());

      tableMapReduceUtilClass.getMethod("addHBaseDependencyJars", Configuration.class).invoke(null, conf);
      String tmpJars = conf.get("tmpjars");
      Assert.assertNotNull(tmpJars);
      Assert.assertTrue("tmpjars should include the stripped JAR: " + tmpJars,
                        tmpJars.contains(strippedJar.getAbsolutePath()));
      Assert.assertFalse("tmpjars should not include the upstream JAR: " + tmpJars,
                         tmpJars.contains(upstreamJar.getAbsolutePath()));
    }
  }

  private static void assertClassAbsent(ClassLoader loader, String className) {
    Assert.assertThrows("Expected class to be stripped from JAR: " + className, ClassNotFoundException.class,
                        () -> Class.forName(className, false, loader));
  }

  private static boolean isForbiddenJettyEntry(String entryName) {
    String lower = entryName.toLowerCase(Locale.ROOT);
    if (!lower.contains("jetty") && !lower.contains("mortbay")) {
      return false;
    }
    return !(entryName.endsWith(CLASS_FILE_SUFFIX) && isAllowedNettyOrGrpcTlsWrapper(entryName));
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
      (dir, name) -> name.startsWith("hbase-shaded-mapreduce-") && name.endsWith(".jar"));
    Assert.assertNotNull("Missing stripped-hbase/lib directory under " + targetDir, matches);
    Assert.assertEquals("Expected exactly one stripped hbase-shaded-mapreduce JAR", 1, matches.length);
    return matches[0];
  }

  private static File findTargetDirectory() {
    // Surefire sets java.io.tmpdir to ${project.build.directory} in pom.xml.
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
    List<String> entryNames = new ArrayList<>();
    try (ZipFile zipFile = new ZipFile(jarFile)) {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        entryNames.add(entries.nextElement().getName());
      }
    }
    return toClassNames(entryNames);
  }

  private static Set<String> toClassNames(List<String> entryNames) {
    Set<String> classes = new HashSet<>();
    for (String name : entryNames) {
      if (name.endsWith(CLASS_FILE_SUFFIX)) {
        classes.add(name.substring(0, name.length() - CLASS_FILE_SUFFIX.length()));
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

  private static byte[] readEntry(ZipFile zipFile, String name) throws IOException {
    try (InputStream in = zipFile.getInputStream(zipFile.getEntry(name))) {
      return ByteStreams.toByteArray(in);
    }
  }

  /**
   * Returns the internal names of all classes referenced from the constant pool, either directly
   * through CONSTANT_Class entries or through field, method and generic signature descriptors.
   */
  private static Set<String> extractConstantPoolClassReferences(byte[] classBytes) throws IOException {
    Map<Integer, String> utf8Entries = new HashMap<>();
    List<Integer> classNameIndices = new ArrayList<>();
    readConstantPool(classBytes, utf8Entries, classNameIndices);

    Set<String> referenced = new HashSet<>();
    for (Integer nameIndex : classNameIndices) {
      String className = unwrapArrayClassName(utf8Entries.get(nameIndex));
      if (className != null) {
        referenced.add(className);
      }
    }
    for (String utf8 : utf8Entries.values()) {
      if (looksLikeDescriptor(utf8)) {
        Matcher matcher = DESCRIPTOR_CLASS_PATTERN.matcher(utf8);
        while (matcher.find()) {
          referenced.add(matcher.group(1));
        }
      }
    }
    return referenced;
  }

  /**
   * Parses the class file constant pool, collecting CONSTANT_Utf8 values by index and the name
   * indices of CONSTANT_Class entries.
   */
  private static void readConstantPool(byte[] classBytes, Map<Integer, String> utf8Entries,
                                       List<Integer> classNameIndices) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(classBytes));
    int magic = in.readInt();
    if (magic != CLASS_FILE_MAGIC) {
      throw new IOException(String.format("Invalid class file magic: 0x%08X", magic));
    }
    in.readUnsignedShort(); // minor
    in.readUnsignedShort(); // major
    int constantPoolCount = in.readUnsignedShort();

    for (int index = 1; index < constantPoolCount; index++) {
      int tag = in.readUnsignedByte();
      switch (tag) {
        case 1: // CONSTANT_Utf8
          utf8Entries.put(index, in.readUTF());
          break;
        case 7: // CONSTANT_Class
          classNameIndices.add(in.readUnsignedShort());
          break;
        case 8: // CONSTANT_String
        case 16: // CONSTANT_MethodType
        case 19: // CONSTANT_Module
        case 20: // CONSTANT_Package
          in.skipBytes(2);
          break;
        case 15: // CONSTANT_MethodHandle
          in.skipBytes(3);
          break;
        case 3: // CONSTANT_Integer
        case 4: // CONSTANT_Float
        case 9: // CONSTANT_Fieldref
        case 10: // CONSTANT_Methodref
        case 11: // CONSTANT_InterfaceMethodref
        case 12: // CONSTANT_NameAndType
        case 17: // CONSTANT_Dynamic
        case 18: // CONSTANT_InvokeDynamic
          in.skipBytes(4);
          break;
        case 5: // CONSTANT_Long
        case 6: // CONSTANT_Double
          in.skipBytes(8);
          index++; // 8-byte constants take two constant pool slots.
          break;
        default:
          throw new IOException("Unsupported constant pool tag: " + tag);
      }
    }
  }

  /**
   * Returns the element class name of an array class such as {@code [[Lfoo/Bar;}, the name itself
   * for a non-array class, or {@code null} for primitive arrays and missing names.
   */
  private static String unwrapArrayClassName(String rawName) {
    if (rawName == null || !rawName.startsWith("[")) {
      return rawName;
    }
    int objectStart = rawName.indexOf('L');
    if (objectStart < 0 || !rawName.endsWith(";")) {
      return null;
    }
    return rawName.substring(objectStart + 1, rawName.length() - 1);
  }

  private static boolean looksLikeDescriptor(String utf8) {
    return utf8.indexOf('/') >= 0
      && (utf8.indexOf('(') >= 0 || utf8.startsWith("L") || utf8.startsWith("[") || utf8.startsWith("<"));
  }

  /**
   * Loads HBase, Bigtable and plugin classes and resources only from the given URLs so that the
   * upstream hbase-shaded-mapreduce JAR on the test classpath cannot satisfy them.
   */
  private static final class IsolatedHBaseClassLoader extends URLClassLoader {
    private static final List<String> ISOLATED_RESOURCE_PREFIXES = ImmutableList.of(
      "org/apache/hadoop/hbase/",
      "org/apache/hbase/",
      "hbase-default.xml",
      "META-INF/services/org.apache.hadoop.hbase.",
      "META-INF/services/org.apache.hbase."
    );

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
      return startsWithAny(name, ISOLATED_RESOURCE_PREFIXES);
    }
  }
}
