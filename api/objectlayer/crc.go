// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.
// This file incorporates code from zlib and includes changes made by Storj Labs, Inc.

/*
 * The CRC combination is derived from multmodp, x2nmodp and crc32_combine in zlib's crc32.c.
 *
 * crc32.c -- compute the CRC-32 of a data stream
 * Copyright (C) 1995-2022 Mark Adler
 *
 * zlib.h -- interface of the 'zlib' general purpose compression library
 * Copyright (C) 1995-2024 Jean-loup Gailly and Mark Adler
 *
 * This software is provided 'as-is', without any express or implied
 * warranty.  In no event will the authors be held liable for any damages
 * arising from the use of this software.
 *
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions:
 *
 * 1. The origin of this software must not be misrepresented; you must not
 *    claim that you wrote the original software. If you use this software
 *    in a product, an acknowledgment in the product documentation would be
 *    appreciated but is not required.
 * 2. Altered source versions must be plainly marked as such, and must not be
 *    misrepresented as being the original software.
 * 3. This notice may not be removed or altered from any source distribution.
 *
 * Jean-loup Gailly        Mark Adler
 * jloup@gzip.org          madler@alumni.caltech.edu
 */

package objectlayer

import (
	"encoding/binary"
	"hash/crc32"
)

var (
	crc32IEEECombiner       = newCRCCombiner[uint32](crc32.IEEE)
	crc32CastagnoliCombiner = newCRCCombiner[uint32](crc32.Castagnoli)
	crc64NVMECombiner       = newCRCCombiner[uint64](0x9a6c9329ac4bc9b5)
)

// crcCombiner combines the CRCs of consecutive data with a reflected polynomial, the way zlib's
// crc32_combine does.
type crcCombiner[T uint32 | uint64] struct {
	poly  T
	width int
	// x2n holds x^(2^n) modulo the polynomial for every n that combine reaches: 3, for bytes to
	// bits, plus each bit of a length. Unlike zlib, it doesn't reuse x2n[n%width], which assumes
	// that x^(2^width) is x modulo the polynomial. That holds for CRC32 and CRC64NVME, but not for
	// CRC32C.
	x2n [3 + 63]T
}

func newCRCCombiner[T uint32 | uint64](poly T) *crcCombiner[T] {
	c := &crcCombiner[T]{poly: poly, width: binary.Size(poly) * 8}
	p := T(1) << (c.width - 2) // x^1
	c.x2n[0] = p
	for n := 1; n < len(c.x2n); n++ {
		p = c.multModP(p, p)
		c.x2n[n] = p
	}
	return c
}

// multModP returns a times b modulo the polynomial. a must not be zero.
func (c *crcCombiner[T]) multModP(a, b T) T {
	m := T(1) << (c.width - 1)
	var p T
	for {
		if a&m != 0 {
			p ^= b
			if a&(m-1) == 0 {
				return p
			}
		}
		m >>= 1
		if b&1 != 0 {
			b = b>>1 ^ c.poly
		} else {
			b >>= 1
		}
	}
}

// x2nModP returns x^(n*2^k) modulo the polynomial.
func (c *crcCombiner[T]) x2nModP(n int64, k int) T {
	p := T(1) << (c.width - 1) // x^0
	for n != 0 {
		if n&1 != 0 {
			p = c.multModP(c.x2n[k], p)
		}
		n >>= 1
		k++
	}
	return p
}

// combine returns the CRC of data whose first part has crc1 and whose second, of len2 bytes, has
// crc2.
func (c *crcCombiner[T]) combine(crc1, crc2 T, len2 int64) T {
	return c.multModP(c.x2nModP(len2, 3), crc1) ^ crc2
}
