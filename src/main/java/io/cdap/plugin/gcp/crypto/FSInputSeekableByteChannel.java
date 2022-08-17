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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

/**
 * A read-only {@link SeekableByteChannel} implementation backed by {@link FSDataInputStream}.
 */
public final class FSInputSeekableByteChannel implements SeekableByteChannel {

  private final long size;
  private final FSDataInputStream is;
  private final ReadableByteChannel readChannel;

  public FSInputSeekableByteChannel(FileSystem fs, Path path, int bufferSize) throws IOException {
    this.size = fs.getFileStatus(path).getLen();
    this.is = fs.open(path, bufferSize);
    this.readChannel = Channels.newChannel(is);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return readChannel.read(dst);
  }

  @Override
  public int write(ByteBuffer src) {
    throw new UnsupportedOperationException("write is not support");
  }

  @Override
  public long position() throws IOException {
    return is.getPos();
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    is.seek(newPosition);
    return this;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public SeekableByteChannel truncate(long size) {
    throw new UnsupportedOperationException("truncate is not support");
  }

  @Override
  public boolean isOpen() {
    return readChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    readChannel.close();
  }
}
