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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package com.scylladb.alternator.keyrouting;

/**
 * MurmurHash3 x64 128-bit implementation returning the first 64 bits.
 *
 * <p>This implementation is used to hash partition keys for deterministic node routing. It produces
 * the same hash values as the Go implementation for compatibility with other Alternator clients.
 *
 * <p>Based on the original MurmurHash3 algorithm by Austin Appleby and the gocql implementation
 * (Apache 2.0 / BSD-3-Clause licensed).
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public final class MurmurHash3 {

  private static final long C1 = 0x87c37b91114253d5L;
  private static final long C2 = 0x4cf5ad432745937fL;
  private static final long FMIX1 = 0xff51afd7ed558ccdL;
  private static final long FMIX2 = 0xc4ceb9fe1a85ec53L;

  private MurmurHash3() {}

  /**
   * Computes MurmurHash3 x64 128-bit hash and returns the first 64 bits (h1).
   *
   * @param data the data to hash
   * @return the hash value (first 64 bits of 128-bit hash)
   */
  public static long hash(byte[] data) {
    return hash(data, 0, data.length);
  }

  /**
   * Computes MurmurHash3 x64 128-bit hash and returns the first 64 bits (h1).
   *
   * @param data the data to hash
   * @param offset the starting offset in data
   * @param length the number of bytes to hash
   * @return the hash value (first 64 bits of 128-bit hash)
   */
  public static long hash(byte[] data, int offset, int length) {
    long h1 = 0;
    long h2 = 0;

    // body - process 16-byte blocks
    int nBlocks = length / 16;
    for (int i = 0; i < nBlocks; i++) {
      long k1 = getBlock(data, offset + i * 16);
      long k2 = getBlock(data, offset + i * 16 + 8);

      k1 *= C1;
      k1 = rotl64(k1, 31);
      k1 *= C2;
      h1 ^= k1;

      h1 = rotl64(h1, 27);
      h1 += h2;
      // 0x52dce729 is a mixing constant from the original MurmurHash3 specification by Austin
      // Appleby
      h1 = h1 * 5 + 0x52dce729;

      k2 *= C2;
      k2 = rotl64(k2, 33);
      k2 *= C1;
      h2 ^= k2;

      h2 = rotl64(h2, 31);
      h2 += h1;
      // 0x38495ab5 is a mixing constant from the original MurmurHash3 specification by Austin
      // Appleby
      h2 = h2 * 5 + 0x38495ab5;
    }

    // tail - handle remaining bytes
    int tailOffset = offset + nBlocks * 16;
    long k1 = 0;
    long k2 = 0;

    switch (length & 15) {
      case 15:
        k2 ^= ((long) data[tailOffset + 14] & 0xff) << 48;
      // fall through
      case 14:
        k2 ^= ((long) data[tailOffset + 13] & 0xff) << 40;
      // fall through
      case 13:
        k2 ^= ((long) data[tailOffset + 12] & 0xff) << 32;
      // fall through
      case 12:
        k2 ^= ((long) data[tailOffset + 11] & 0xff) << 24;
      // fall through
      case 11:
        k2 ^= ((long) data[tailOffset + 10] & 0xff) << 16;
      // fall through
      case 10:
        k2 ^= ((long) data[tailOffset + 9] & 0xff) << 8;
      // fall through
      case 9:
        k2 ^= ((long) data[tailOffset + 8] & 0xff);

        k2 *= C2;
        k2 = rotl64(k2, 33);
        k2 *= C1;
        h2 ^= k2;
      // fall through
      case 8:
        k1 ^= ((long) data[tailOffset + 7] & 0xff) << 56;
      // fall through
      case 7:
        k1 ^= ((long) data[tailOffset + 6] & 0xff) << 48;
      // fall through
      case 6:
        k1 ^= ((long) data[tailOffset + 5] & 0xff) << 40;
      // fall through
      case 5:
        k1 ^= ((long) data[tailOffset + 4] & 0xff) << 32;
      // fall through
      case 4:
        k1 ^= ((long) data[tailOffset + 3] & 0xff) << 24;
      // fall through
      case 3:
        k1 ^= ((long) data[tailOffset + 2] & 0xff) << 16;
      // fall through
      case 2:
        k1 ^= ((long) data[tailOffset + 1] & 0xff) << 8;
      // fall through
      case 1:
        k1 ^= ((long) data[tailOffset] & 0xff);

        k1 *= C1;
        k1 = rotl64(k1, 31);
        k1 *= C2;
        h1 ^= k1;
    }

    // finalization
    h1 ^= length;
    h2 ^= length;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    // h2 += h1 is omitted since h2 is discarded

    return h1;
  }

  /**
   * Reads 8 bytes from the data array in little-endian order and returns as a long. Uses unsigned
   * byte masking (& 0xff) to match the Go implementation where bytes are unsigned.
   */
  private static long getBlock(byte[] data, int offset) {
    return ((long) data[offset] & 0xff)
        | (((long) data[offset + 1] & 0xff) << 8)
        | (((long) data[offset + 2] & 0xff) << 16)
        | (((long) data[offset + 3] & 0xff) << 24)
        | (((long) data[offset + 4] & 0xff) << 32)
        | (((long) data[offset + 5] & 0xff) << 40)
        | (((long) data[offset + 6] & 0xff) << 48)
        | (((long) data[offset + 7] & 0xff) << 56);
  }

  private static long rotl64(long x, int r) {
    return (x << r) | (x >>> (64 - r));
  }

  private static long fmix64(long k) {
    k ^= k >>> 33;
    k *= FMIX1;
    k ^= k >>> 33;
    k *= FMIX2;
    k ^= k >>> 33;
    return k;
  }
}
