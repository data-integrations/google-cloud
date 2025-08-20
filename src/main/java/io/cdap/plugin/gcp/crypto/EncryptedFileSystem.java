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
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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
   * Opens a file asynchronously and returns a {@link FutureDataInputStreamBuilder}
   * to build a {@link FSDataInputStream} for the specified {@link Path}.
   *
   * <p>This implementation returns a builder that constructs an input stream by using a decryptor
   * to open the file through a {@link SeekableByteChannelFSInputStream}. The file is read
   * with a buffer size of 4096 bytes.</p>
   *
   * @param path the {@link Path} of the file to open
   * @return a {@link FutureDataInputStreamBuilder} that asynchronously builds a {@link FSDataInputStream}
   * @throws UnsupportedOperationException if the operation is not supported
   */
  @Override
  public FutureDataInputStreamBuilder openFile(Path path) throws UnsupportedOperationException {
    return new FutureDataInputStreamBuilder() {
      @Override
      public CompletableFuture<FSDataInputStream> build()
        throws IllegalArgumentException, UnsupportedOperationException {
        return CompletableFuture.supplyAsync(() -> {
          try {
            return new FSDataInputStream(new SeekableByteChannelFSInputStream(
                decryptor.open(fs, path, CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT)));
          } catch (Exception e) {
            throw new CompletionException(e);
          }
        });
      }

      @Override
      public FutureDataInputStreamBuilder opt(@NotNull String s, @NotNull String s1) {
        return this;
      }

      @Override
      public FutureDataInputStreamBuilder opt(@NotNull String s, @NotNull String... strings) {
        return this;
      }

      @Override
      public FutureDataInputStreamBuilder must(@NotNull String s, @NotNull String s1) {
        return this;
      }

      @Override
      public FutureDataInputStreamBuilder must(@NotNull String s, @NotNull String... strings) {
        return this;
      }
    };
  }

  /**
   * Opens a file asynchronously using the provided {@link Path}, and returns
   * a {@link CompletableFuture} that supplies a {@link FSDataInputStream}.
   *
   * <p>This method uses a decryptor to open the file and wraps it in a {@link SeekableByteChannelFSInputStream}.
   * It uses the buffer size specified in the {@code parameters}; if the buffer size is not greater than zero,
   * a default of 4096 bytes is used.</p>
   *
   * @param path the {@link Path} to the file to open
   * @param parameters the {@link OpenFileParameters} containing optional configuration, such as buffer size
   * @return a {@link CompletableFuture} that will complete with the {@link FSDataInputStream}
   * @throws CompletionException if an exception occurs during file opening
   */
  @Override
  protected CompletableFuture<FSDataInputStream> openFileWithOptions(Path path, OpenFileParameters parameters) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        int bufferSize = parameters.getBufferSize() > 0 ? parameters.getBufferSize()
            : CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
        return new FSDataInputStream(
          new SeekableByteChannelFSInputStream(decryptor.open(fs, path, bufferSize)));
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    });
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
