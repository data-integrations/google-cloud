/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.gcp.crypto;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.Map;

/**
 * A hadoop {@link FileSystem} that support files decryption (encryption is currently not supported).
 */
public class EncryptedFileSystem extends FilterFileSystem {

  private static final String CONF_PREFIX = "io.cdap.crypto.fs.";
  private static final String FS_SCHEME = CONF_PREFIX + "scheme";
  private static final String FS_IMPL = CONF_PREFIX + "impl";
  private static final String DECRYPTOR_IMPL = CONF_PREFIX + "decryptor.impl";

  private static final Logger LOG = LoggerFactory.getLogger(EncryptedFileSystem.class);

  private Decryptor decryptor;

  public static Map<String, String> configure(String scheme, Class<? extends Decryptor> decryptorClass,
                                              Map<String, String> properties) {
    String fsClass = properties.get("fs." + scheme + ".impl");
    if (fsClass == null) {
      throw new IllegalArgumentException("Missing implementation for FileSystem scheme " + scheme);
    }

    properties.put(FS_SCHEME, scheme);
    properties.put(FS_IMPL, fsClass);
    properties.put("fs." + scheme + ".impl", EncryptedFileSystem.class.getName());
    properties.put(DECRYPTOR_IMPL, decryptorClass.getName());

    LOG.debug("Configured FileSystem scheme {} to use {}", scheme, EncryptedFileSystem.class.getName());

    return properties;
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    String scheme = conf.get(FS_SCHEME);
    Class<? extends FileSystem> fsClass = conf.getClass(FS_IMPL, null, FileSystem.class);
    Class<? extends Decryptor> decryptorClass = conf.getClass(DECRYPTOR_IMPL, null, Decryptor.class);

    if (scheme == null) {
      throw new IllegalArgumentException("Missing configuration '" + FS_SCHEME + "'");
    }
    if (fsClass == null) {
      throw new IllegalArgumentException("Missing configuration '" + FS_IMPL + "'");
    }
    if (decryptorClass == null) {
      throw new IllegalArgumentException("Missing configuration '" + DECRYPTOR_IMPL + "'");
    }

    LOG.debug("Initializing for scheme {} with class {}", scheme, fsClass.getName());

    Configuration copyConf = new Configuration(conf);
    copyConf.setClass("fs." + scheme + ".impl", fsClass, FileSystem.class);

    this.fs = FileSystem.get(name, copyConf);
    this.statistics = FileSystem.getStatistics(fs.getScheme(), fs.getClass());
    try {
      this.decryptor = decryptorClass.newInstance();
      if (this.decryptor instanceof Configurable) {
        ((Configurable) this.decryptor).setConf(copyConf);
      }
      LOG.debug("Decryptor class used for decryption is {}", decryptorClass.getName());
    } catch (Exception e) {
      throw new IOException("Failed to instantiate Decryptor class " + decryptorClass, e);
    }
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    return new FSDataInputStream(new SeekableByteChannelFSInputStream(decryptor.open(fs, path, bufferSize)));
  }

  /**
   * A {@link FSInputStream} implementation backed by a {@link SeekableByteChannel}.
   */
  private static final class SeekableByteChannelFSInputStream extends FSInputStream {

    private final SeekableByteChannel seekableChannel;
    private final InputStream is;

    SeekableByteChannelFSInputStream(SeekableByteChannel seekableChannel) {
      this.seekableChannel = seekableChannel;
      this.is = Channels.newInputStream(seekableChannel);
    }

    @Override
    public int read() throws IOException {
      return is.read();
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
      return is.read(bytes, off, len);
    }

    @Override
    public synchronized void seek(long position) throws IOException {
      seekableChannel.position(position);
    }

    @Override
    public synchronized long getPos() throws IOException {
      return seekableChannel.position();
    }

    @Override
    public boolean seekToNewSource(long l) {
      return false;
    }
  }
}
