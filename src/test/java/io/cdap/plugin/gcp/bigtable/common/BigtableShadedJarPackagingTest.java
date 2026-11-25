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
import io.cdap.plugin.gcp.bigtable.source.BigtableSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * Verifies that redundant hbase-shaded-client (with shaded Jetty 6) and unused HBase Jetty
 * connector classes are excluded without breaking Cloud Bigtable client or MapReduce classloading.
 */
public class BigtableShadedJarPackagingTest {
  private static final Pattern DESCRIPTOR_CLASS_PATTERN = Pattern.compile("L([a-zA-Z0-9_$/]+);");
  private static final List<String> AUDITED_HBASE_PACKAGES = Collections.unmodifiableList(Arrays.asList(
    "org/apache/hadoop/hbase/client/",
    "org/apache/hadoop/hbase/mapreduce/",
    "org/apache/hadoop/hbase/io/",
    "org/apache/hadoop/hbase/util/",
    "org/apache/hadoop/hbase/filter/",
    "org/apache/hadoop/hbase/shaded/",
    "org/apache/hadoop/hbase/security/"
  ));

  @Test
  public void testStrippedHBaseJarHasNoJettyAndNoDanglingRefs() throws Exception {
    assertNoHBaseShadedClientOnClasspath();
    File strippedJar = findStrippedHBaseJar();
    File upstreamJar = new File(Put.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    Assert.assertTrue("Upstream hbase-shaded-server JAR should exist: " + upstreamJar, upstreamJar.isFile());
    Assert.assertTrue("Expected Put to resolve from hbase-shaded-server, got: " + upstreamJar.getName(),
                      upstreamJar.getName().startsWith("hbase-shaded-server-"));

    Set<String> upstreamClasses = collectClassNames(upstreamJar);
    Set<String> strippedClasses = new HashSet<>();
    List<String> forbiddenEntries = new ArrayList<>();

    try (ZipFile zipFile = new ZipFile(strippedJar)) {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        String name = entry.getName();
        if (name.endsWith(".class")) {
          strippedClasses.add(name.substring(0, name.length() - 6));
        }
        if (isForbiddenJettyEntry(name)) {
          forbiddenEntries.add(name);
        }
      }

      Assert.assertTrue("Unexpected Jetty/Mortbay entries in stripped JAR: " + forbiddenEntries,
                        forbiddenEntries.isEmpty());

      Set<String> removedClasses = new HashSet<>(upstreamClasses);
      removedClasses.removeAll(strippedClasses);
      Assert.assertTrue("Expected SslSelectChannelConnectorSecure to be removed",
                        removedClasses.contains("org/apache/hadoop/hbase/jetty/SslSelectChannelConnectorSecure"));

      List<String> danglingRefs = new ArrayList<>();
      Enumeration<? extends ZipEntry> auditEntries = zipFile.entries();
      while (auditEntries.hasMoreElements()) {
        ZipEntry entry = auditEntries.nextElement();
        String name = entry.getName();
        if (!name.endsWith(".class") || !startsWithAny(name, AUDITED_HBASE_PACKAGES)) {
          continue;
        }
        byte[] classBytes = readAllBytes(zipFile.getInputStream(entry));
        Set<String> referencedClasses = extractConstantPoolClassReferences(classBytes);
        for (String ref : referencedClasses) {
          if (removedClasses.contains(ref)) {
            danglingRefs.add(name + " -> " + ref);
          }
        }
      }
      Assert.assertTrue("Dangling references to removed classes: " + danglingRefs, danglingRefs.isEmpty());
    }

    File packagedBundle = findOptionalPackagedPluginBundle();
    if (packagedBundle != null) {
      auditPackagedPluginBundle(packagedBundle);
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

      List<String> classesToLoadAndInitialize = Arrays.asList(
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

      Class<?> btInputFormatClass = Class.forName("io.cdap.plugin.gcp.bigtable.source.BigtableInputFormat",
                                                  true, loader);
      Assert.assertNotNull(btInputFormatClass.getDeclaredConstructor().newInstance());

      Class<?> btOutputFormatClass = Class.forName("io.cdap.plugin.gcp.bigtable.sink.BigtableOutputFormat",
                                                   true, loader);
      Assert.assertNotNull(btOutputFormatClass.getDeclaredConstructor().newInstance());

      Class<?> hbaseConfClass = Class.forName("org.apache.hadoop.hbase.HBaseConfiguration", true, loader);
      Configuration conf = (Configuration) hbaseConfClass.getMethod("create").invoke(null);
      Assert.assertNotNull(conf);

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
    try {
      Class.forName(className, false, loader);
      Assert.fail("Expected class to be absent from JAR: " + className);
    } catch (ClassNotFoundException expected) {
      // expected
    }
  }

  private static void assertNoHBaseShadedClientOnClasspath() throws Exception {
    String classPath = System.getProperty("java.class.path", "");
    for (String entry : classPath.split(Pattern.quote(File.pathSeparator))) {
      if (entry.isEmpty()) {
        continue;
      }
      File file = new File(entry);
      Assert.assertFalse("hbase-shaded-client should not be on classpath: " + file.getName(),
                         file.getName().startsWith("hbase-shaded-client-"));
      if (file.isFile() && file.getName().endsWith(".jar")) {
        try (JarFile jarFile = new JarFile(file)) {
          Manifest manifest = jarFile.getManifest();
          if (manifest == null) {
            continue;
          }
          String mfClassPath = manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH);
          if (mfClassPath == null || mfClassPath.trim().isEmpty()) {
            continue;
          }
          URI baseUri = file.toURI();
          for (String token : mfClassPath.trim().split("\\s+")) {
            File resolvedFile = new File(baseUri.resolve(token));
            Assert.assertFalse("hbase-shaded-client should not be on classpath: " + resolvedFile.getName(),
                               resolvedFile.getName().startsWith("hbase-shaded-client-"));
          }
        }
      }
    }
  }

  private static void auditPackagedPluginBundle(File bundleJar) throws IOException {
    boolean foundStrippedHBaseJar = false;
    List<String> violations = new ArrayList<>();
    try (ZipFile outerZip = new ZipFile(bundleJar)) {
      Enumeration<? extends ZipEntry> entries = outerZip.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        String name = entry.getName();
        if (name.startsWith("lib/hbase-shaded-client-")) {
          violations.add("Unexpected redundant hbase-shaded-client JAR: " + name);
        }
        if (isForbiddenJettyEntry(name)) {
          violations.add(bundleJar.getName() + "!/" + name);
        }
        if (name.startsWith("lib/hbase-shaded-server-") && name.endsWith(".jar")) {
          foundStrippedHBaseJar = true;
        }
        if (name.endsWith(".jar")) {
          try (ZipInputStream nestedZip = new ZipInputStream(outerZip.getInputStream(entry))) {
            ZipEntry nestedEntry;
            while ((nestedEntry = nestedZip.getNextEntry()) != null) {
              String nestedName = nestedEntry.getName();
              if (isForbiddenJettyEntry(nestedName)) {
                violations.add(bundleJar.getName() + "!/" + name + "!/" + nestedName);
              }
            }
          }
        }
      }
    }
    Assert.assertTrue("Expected lib/hbase-shaded-server-*.jar inside " + bundleJar, foundStrippedHBaseJar);
    Assert.assertTrue("Forbidden Jetty/Mortbay entries in packaged plugin bundle: " + violations,
                      violations.isEmpty());
  }

  private static boolean isForbiddenJettyEntry(String entryName) {
    String lower = entryName.toLowerCase(Locale.ROOT);
    if (!lower.contains("jetty") && !lower.contains("mortbay")) {
      return false;
    }
    if (entryName.endsWith(".class") && isAllowedNettyOrGrpcTlsWrapper(entryName)) {
      return false;
    }
    if (entryName.startsWith("META-INF/maven/org.apache.avro/avro-ipc-jetty")
        || entryName.equals("lib/avro-ipc-jetty-1.11.4.jar")
        || entryName.startsWith("org/apache/avro/ipc/jetty/")) {
      return false;
    }
    return true;
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

  private static File findOptionalPackagedPluginBundle() {
    File targetDir = findTargetDirectory();
    File[] matches = targetDir.listFiles(
      (dir, name) -> name.startsWith("google-cloud-")
        && name.endsWith(".jar")
        && !name.endsWith("-sources.jar")
        && !name.endsWith("-javadoc.jar")
        && !name.endsWith("-tests.jar"));
    if (matches == null || matches.length == 0) {
      return null;
    }
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
          classes.add(name.substring(0, name.length() - 6));
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

  private static byte[] readAllBytes(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int read;
    while ((read = in.read(buffer)) != -1) {
      out.write(buffer, 0, read);
    }
    return out.toByteArray();
  }

  private static Set<String> extractConstantPoolClassReferences(byte[] classBytes) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(classBytes));
    int magic = in.readInt();
    if (magic != 0xCAFEBABE) {
      return Collections.emptySet();
    }
    in.readUnsignedShort(); // minor
    in.readUnsignedShort(); // major
    int cpCount = in.readUnsignedShort();

    Map<Integer, String> utf8Entries = new HashMap<>();
    List<Integer> classNameIndices = new ArrayList<>();

    int idx = 1;
    while (idx < cpCount) {
      int tag = in.readUnsignedByte();
      switch (tag) {
        case 1: // CONSTANT_Utf8
          utf8Entries.put(idx, in.readUTF());
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
          idx++;
          break;
        case 15: // CONSTANT_MethodHandle
          in.skipBytes(3);
          break;
        default:
          throw new IOException("Unsupported constant pool tag: " + tag);
      }
      idx++;
    }

    Set<String> referenced = new HashSet<>();
    for (Integer nameIndex : classNameIndices) {
      String raw = utf8Entries.get(nameIndex);
      if (raw == null) {
        continue;
      }
      if (raw.startsWith("[")) {
        int objStart = raw.indexOf('L');
        if (objStart >= 0 && raw.endsWith(";")) {
          referenced.add(raw.substring(objStart + 1, raw.length() - 1));
        }
      } else {
        referenced.add(raw);
      }
    }

    for (String utf8 : utf8Entries.values()) {
      if (utf8.indexOf('/') >= 0 && (utf8.indexOf('(') >= 0 || utf8.startsWith("L") || utf8.startsWith("["))) {
        Matcher matcher = DESCRIPTOR_CLASS_PATTERN.matcher(utf8);
        while (matcher.find()) {
          referenced.add(matcher.group(1));
        }
      }
    }
    return referenced;
  }

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

    private static boolean isIsolatedPackage(String name) {
      return name.startsWith("org.apache.hadoop.hbase.")
        || name.startsWith("org.apache.hbase.")
        || name.startsWith("com.google.cloud.bigtable.")
        || name.startsWith("com.google.bigtable.")
        || name.startsWith("io.cdap.plugin.gcp.bigtable.");
    }
  }
}
