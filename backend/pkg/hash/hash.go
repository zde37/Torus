package hash

import (
	"crypto/sha256"
	"fmt"
	"math/big"
)

const (
	// M is the size of the identifier space in bits (2^160)
	M = 160
)

var (
	// ringSize is 2^M, the size of the Chord ring
	ringSize = new(big.Int).Exp(big.NewInt(2), big.NewInt(M), nil)

	// zero and one for convenience
	zero = big.NewInt(0)
	one  = big.NewInt(1)
)

// HashKey hashes arbitrary data to a 160-bit identifier using SHA-256.
// The hash is truncated to the first 20 bytes (160 bits).
func HashKey(data []byte) *big.Int {
	hash := sha256.Sum256(data)
	// Take first 20 bytes (160 bits) of the 32-byte SHA-256 hash
	id := new(big.Int).SetBytes(hash[:20])
	return id
}

// HashString hashes a string to a 160-bit identifier.
func HashString(s string) *big.Int {
	return HashKey([]byte(s))
}

// HashAddress hashes a network address (host:port) to a 160-bit identifier.
// This is used to compute node IDs from their network addresses.
func HashAddress(host string, port int) *big.Int {
	addr := fmt.Sprintf("%s:%d", host, port)
	return HashString(addr)
}

// InRange checks if id is in the range (start, end] on the Chord ring.
// The range wraps around if end <= start.
//
// Examples:
//   - InRange(5, 3, 7) = true    // 5 is in (3, 7]
//   - InRange(3, 3, 7) = false   // 3 is not in (3, 7] (exclusive start)
//   - InRange(7, 3, 7) = true    // 7 is in (3, 7] (inclusive end)
//   - InRange(1, 8, 3) = true    // 1 is in (8, 3] with wraparound
//   - InRange(9, 8, 3) = true    // 9 is in (8, 3] with wraparound
func InRange(id, start, end *big.Int) bool {
	if id == nil || start == nil || end == nil {
		return false
	}

	// Normalize to ring
	id = mod(id)
	start = mod(start)
	end = mod(end)

	if start.Cmp(end) < 0 {
		// Normal case: start < end, no wraparound
		// Check if id in (start, end]
		return id.Cmp(start) > 0 && id.Cmp(end) <= 0
	} else if start.Cmp(end) > 0 {
		// Wraparound case: start > end
		// id in (start, 2^M) or id in [0, end]
		return id.Cmp(start) > 0 || id.Cmp(end) <= 0
	} else {
		// start == end, the range is the entire ring except start
		return id.Cmp(start) != 0
	}
}

// Between checks if id is in the range (start, end) on the Chord ring (exclusive on both ends).
func Between(id, start, end *big.Int) bool {
	if id == nil || start == nil || end == nil {
		return false
	}

	// Normalize to ring
	id = mod(id)
	start = mod(start)
	end = mod(end)

	if start.Cmp(end) < 0 {
		// Normal case: start < end
		return id.Cmp(start) > 0 && id.Cmp(end) < 0
	} else if start.Cmp(end) > 0 {
		// Wraparound case
		return id.Cmp(start) > 0 || id.Cmp(end) < 0
	} else {
		// start == end, range is entire ring except start
		return id.Cmp(start) != 0
	}
}

// BetweenRightIncl checks if id is in the range (start, end] (inclusive on end).
// This is an alias for InRange for clarity.
func BetweenRightIncl(id, start, end *big.Int) bool {
	return InRange(id, start, end)
}

// BetweenLeftIncl checks if id is in the range [start, end) (inclusive on start).
func BetweenLeftIncl(id, start, end *big.Int) bool {
	if id == nil || start == nil || end == nil {
		return false
	}

	// Normalize to ring
	id = mod(id)
	start = mod(start)
	end = mod(end)

	if start.Cmp(end) < 0 {
		// Normal case
		return id.Cmp(start) >= 0 && id.Cmp(end) < 0
	} else if start.Cmp(end) > 0 {
		// Wraparound case
		return id.Cmp(start) >= 0 || id.Cmp(end) < 0
	} else {
		// start == end
		return id.Cmp(start) != 0
	}
}

// Distance computes the clockwise distance from start to end on the Chord ring.
// Returns (end - start) mod 2^M.
func Distance(start, end *big.Int) *big.Int {
	if start == nil || end == nil {
		return new(big.Int)
	}

	start = mod(start)
	end = mod(end)

	// Calculate (end - start) mod 2^M
	dist := new(big.Int).Sub(end, start)
	return mod(dist)
}

// PowerOfTwo returns 2^exponent.
func PowerOfTwo(exponent int) *big.Int {
	if exponent < 0 {
		return new(big.Int)
	}
	return new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(exponent)), nil)
}

// AddPowerOfTwo computes (n + 2^exponent) mod 2^M.
// This is used to calculate finger table start values: finger[i].start = (n + 2^(i-1)) mod 2^M
func AddPowerOfTwo(n *big.Int, exponent int) *big.Int {
	if n == nil {
		return new(big.Int)
	}

	n = mod(n)
	offset := PowerOfTwo(exponent)
	result := new(big.Int).Add(n, offset)
	return mod(result)
}

// mod returns x mod 2^M, ensuring the result is in [0, 2^M).
// Handles negative values correctly by adding 2^M until positive.
func mod(x *big.Int) *big.Int {
	result := new(big.Int).Mod(x, ringSize)
	// Ensure result is non-negative
	if result.Sign() < 0 {
		result.Add(result, ringSize)
	}
	return result
}

// RingSize returns 2^M, the size of the Chord ring.
func RingSize() *big.Int {
	return new(big.Int).Set(ringSize)
}

// MaxID returns the maximum valid ID on the ring (2^M - 1).
func MaxID() *big.Int {
	return new(big.Int).Sub(ringSize, one)
}

// IsValidID checks if an ID is within the valid range [0, 2^M).
func IsValidID(id *big.Int) bool {
	if id == nil {
		return false
	}
	return id.Cmp(zero) >= 0 && id.Cmp(ringSize) < 0
}
