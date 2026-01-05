package hash

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashKey(t *testing.T) {
	tests := []struct {
		name  string
		data  []byte
		check func(*testing.T, *big.Int)
	}{
		{
			name: "deterministic",
			data: []byte("test"),
			check: func(t *testing.T, id *big.Int) {
				id2 := HashKey([]byte("test"))
				assert.Equal(t, id, id2, "same input should produce same hash")
			},
		},
		{
			name: "different inputs produce different hashes",
			data: []byte("test1"),
			check: func(t *testing.T, id *big.Int) {
				id2 := HashKey([]byte("test2"))
				assert.NotEqual(t, id, id2)
			},
		},
		{
			name: "empty data",
			data: []byte{},
			check: func(t *testing.T, id *big.Int) {
				assert.NotNil(t, id)
			},
		},
		{
			name: "valid range",
			data: []byte("test"),
			check: func(t *testing.T, id *big.Int) {
				assert.True(t, IsValidID(id))
				assert.True(t, id.Cmp(zero) >= 0)
				assert.True(t, id.Cmp(ringSize) < 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := HashKey(tt.data)
			require.NotNil(t, id)
			tt.check(t, id)
		})
	}
}

func TestHashString(t *testing.T) {
	tests := []struct {
		name string
		str  string
	}{
		{name: "simple string", str: "hello"},
		{name: "empty string", str: ""},
		{name: "special chars", str: "test@#$%"},
		{name: "unicode", str: "こんにちは"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := HashString(tt.str)
			require.NotNil(t, id)
			assert.True(t, IsValidID(id))

			// Should be same as HashKey
			id2 := HashKey([]byte(tt.str))
			assert.Equal(t, id2, id)
		})
	}
}

func TestHashAddress(t *testing.T) {
	tests := []struct {
		name string
		host string
		port int
	}{
		{name: "localhost", host: "127.0.0.1", port: 8080},
		{name: "different port", host: "127.0.0.1", port: 9000},
		{name: "different host", host: "192.168.1.1", port: 8080},
		{name: "hostname", host: "example.com", port: 443},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := HashAddress(tt.host, tt.port)
			require.NotNil(t, id)
			assert.True(t, IsValidID(id))

			// Different addresses should produce different hashes
			id2 := HashAddress(tt.host, tt.port+1)
			assert.NotEqual(t, id, id2)
		})
	}
}

func TestInRange(t *testing.T) {
	tests := []struct {
		name     string
		id       *big.Int
		start    *big.Int
		end      *big.Int
		expected bool
	}{
		{
			name:     "id in normal range",
			id:       big.NewInt(5),
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: true,
		},
		{
			name:     "id equals start (exclusive)",
			id:       big.NewInt(3),
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: false,
		},
		{
			name:     "id equals end (inclusive)",
			id:       big.NewInt(7),
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: true,
		},
		{
			name:     "id outside range",
			id:       big.NewInt(10),
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: false,
		},
		{
			name:     "wraparound range - id after start",
			id:       big.NewInt(9),
			start:    big.NewInt(8),
			end:      big.NewInt(3),
			expected: true,
		},
		{
			name:     "wraparound range - id before end",
			id:       big.NewInt(1),
			start:    big.NewInt(8),
			end:      big.NewInt(3),
			expected: true,
		},
		{
			name:     "wraparound range - id at end",
			id:       big.NewInt(3),
			start:    big.NewInt(8),
			end:      big.NewInt(3),
			expected: true,
		},
		{
			name:     "wraparound range - id not in range",
			id:       big.NewInt(5),
			start:    big.NewInt(8),
			end:      big.NewInt(3),
			expected: false,
		},
		{
			name:     "start equals end (full ring except start)",
			id:       big.NewInt(5),
			start:    big.NewInt(3),
			end:      big.NewInt(3),
			expected: true,
		},
		{
			name:     "start equals end - id equals start/end",
			id:       big.NewInt(3),
			start:    big.NewInt(3),
			end:      big.NewInt(3),
			expected: false,
		},
		{
			name:     "nil id",
			id:       nil,
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: false,
		},
		{
			name:     "nil start",
			id:       big.NewInt(5),
			start:    nil,
			end:      big.NewInt(7),
			expected: false,
		},
		{
			name:     "nil end",
			id:       big.NewInt(5),
			start:    big.NewInt(3),
			end:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InRange(tt.id, tt.start, tt.end)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBetween(t *testing.T) {
	tests := []struct {
		name     string
		id       *big.Int
		start    *big.Int
		end      *big.Int
		expected bool
	}{
		{
			name:     "id in normal range",
			id:       big.NewInt(5),
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: true,
		},
		{
			name:     "id equals start (exclusive)",
			id:       big.NewInt(3),
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: false,
		},
		{
			name:     "id equals end (exclusive)",
			id:       big.NewInt(7),
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: false,
		},
		{
			name:     "wraparound range",
			id:       big.NewInt(1),
			start:    big.NewInt(8),
			end:      big.NewInt(3),
			expected: true,
		},
		{
			name:     "start equals end",
			id:       big.NewInt(5),
			start:    big.NewInt(3),
			end:      big.NewInt(3),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Between(tt.id, tt.start, tt.end)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBetweenRightIncl(t *testing.T) {
	// Should behave same as InRange
	id := big.NewInt(5)
	start := big.NewInt(3)
	end := big.NewInt(7)

	result1 := BetweenRightIncl(id, start, end)
	result2 := InRange(id, start, end)
	assert.Equal(t, result2, result1)
}

func TestBetweenLeftIncl(t *testing.T) {
	tests := []struct {
		name     string
		id       *big.Int
		start    *big.Int
		end      *big.Int
		expected bool
	}{
		{
			name:     "id in range",
			id:       big.NewInt(5),
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: true,
		},
		{
			name:     "id equals start (inclusive)",
			id:       big.NewInt(3),
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: true,
		},
		{
			name:     "id equals end (exclusive)",
			id:       big.NewInt(7),
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: false,
		},
		{
			name:     "wraparound range",
			id:       big.NewInt(8),
			start:    big.NewInt(8),
			end:      big.NewInt(3),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BetweenLeftIncl(tt.id, tt.start, tt.end)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDistance(t *testing.T) {
	tests := []struct {
		name     string
		start    *big.Int
		end      *big.Int
		expected *big.Int
	}{
		{
			name:     "normal distance",
			start:    big.NewInt(3),
			end:      big.NewInt(7),
			expected: big.NewInt(4),
		},
		{
			name:     "zero distance",
			start:    big.NewInt(5),
			end:      big.NewInt(5),
			expected: big.NewInt(0),
		},
		{
			name:  "wraparound distance",
			start: big.NewInt(8),
			end:   big.NewInt(3),
			expected: func() *big.Int {
				// (3 - 8) mod 2^160 = 2^160 - 5
				result := new(big.Int).Sub(ringSize, big.NewInt(5))
				return result
			}(),
		},
		{
			name:     "nil start",
			start:    nil,
			end:      big.NewInt(5),
			expected: big.NewInt(0),
		},
		{
			name:     "nil end",
			start:    big.NewInt(5),
			end:      nil,
			expected: big.NewInt(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Distance(tt.start, tt.end)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPowerOfTwo(t *testing.T) {
	tests := []struct {
		name     string
		exponent int
		expected *big.Int
	}{
		{
			name:     "2^0",
			exponent: 0,
			expected: big.NewInt(1),
		},
		{
			name:     "2^1",
			exponent: 1,
			expected: big.NewInt(2),
		},
		{
			name:     "2^3",
			exponent: 3,
			expected: big.NewInt(8),
		},
		{
			name:     "2^10",
			exponent: 10,
			expected: big.NewInt(1024),
		},
		{
			name:     "2^159",
			exponent: 159,
			expected: new(big.Int).Exp(big.NewInt(2), big.NewInt(159), nil),
		},
		{
			name:     "negative exponent",
			exponent: -1,
			expected: big.NewInt(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PowerOfTwo(tt.exponent)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAddPowerOfTwo(t *testing.T) {
	tests := []struct {
		name     string
		n        *big.Int
		exponent int
		check    func(*testing.T, *big.Int)
	}{
		{
			name:     "simple addition",
			n:        big.NewInt(5),
			exponent: 2,
			check: func(t *testing.T, result *big.Int) {
				// 5 + 2^2 = 5 + 4 = 9
				assert.Equal(t, big.NewInt(9), result)
			},
		},
		{
			name:     "zero exponent",
			n:        big.NewInt(10),
			exponent: 0,
			check: func(t *testing.T, result *big.Int) {
				// 10 + 2^0 = 10 + 1 = 11
				assert.Equal(t, big.NewInt(11), result)
			},
		},
		{
			name:     "wraparound",
			n:        new(big.Int).Sub(ringSize, big.NewInt(5)),
			exponent: 4,
			check: func(t *testing.T, result *big.Int) {
				// (2^160 - 5) + 16 = 2^160 + 11, which wraps to 11
				assert.Equal(t, big.NewInt(11), result)
			},
		},
		{
			name:     "nil n",
			n:        nil,
			exponent: 2,
			check: func(t *testing.T, result *big.Int) {
				assert.NotNil(t, result)
				assert.Equal(t, big.NewInt(0), result)
			},
		},
		{
			name:     "large exponent",
			n:        big.NewInt(100),
			exponent: 159,
			check: func(t *testing.T, result *big.Int) {
				expected := new(big.Int).Add(big.NewInt(100), PowerOfTwo(159))
				expected = mod(expected)
				assert.Equal(t, expected, result)
				assert.True(t, IsValidID(result))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AddPowerOfTwo(tt.n, tt.exponent)
			require.NotNil(t, result)
			tt.check(t, result)
		})
	}
}

func TestMod(t *testing.T) {
	tests := []struct {
		name     string
		x        *big.Int
		expected *big.Int
	}{
		{
			name:     "positive number in range",
			x:        big.NewInt(42),
			expected: big.NewInt(42),
		},
		{
			name:     "zero",
			x:        big.NewInt(0),
			expected: big.NewInt(0),
		},
		{
			name: "negative number",
			x:    big.NewInt(-5),
			expected: func() *big.Int {
				// -5 mod 2^160 = 2^160 - 5
				return new(big.Int).Sub(ringSize, big.NewInt(5))
			}(),
		},
		{
			name: "number larger than ring size",
			x: func() *big.Int {
				return new(big.Int).Add(ringSize, big.NewInt(10))
			}(),
			expected: big.NewInt(10),
		},
		{
			name:     "ring size itself",
			x:        new(big.Int).Set(ringSize),
			expected: big.NewInt(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mod(tt.x)
			assert.Equal(t, 0, tt.expected.Cmp(result), "expected %s, got %s", tt.expected.String(), result.String())
			assert.True(t, IsValidID(result))
		})
	}
}

func TestRingSize(t *testing.T) {
	rs := RingSize()
	require.NotNil(t, rs)

	// Should equal 2^160
	expected := new(big.Int).Exp(big.NewInt(2), big.NewInt(M), nil)
	assert.Equal(t, expected, rs)

	// Should return a copy, not the original
	assert.NotSame(t, ringSize, rs)

	// Modifying return value shouldn't affect internal ringSize
	rs.Add(rs, big.NewInt(1))
	assert.Equal(t, expected, RingSize())
}

func TestMaxID(t *testing.T) {
	maxID := MaxID()
	require.NotNil(t, maxID)

	// Should equal 2^160 - 1
	expected := new(big.Int).Sub(ringSize, big.NewInt(1))
	assert.Equal(t, expected, maxID)

	// MaxID should be valid
	assert.True(t, IsValidID(maxID))

	// MaxID + 1 should not be valid
	invalid := new(big.Int).Add(maxID, big.NewInt(1))
	assert.False(t, IsValidID(invalid))
}

func TestIsValidID(t *testing.T) {
	tests := []struct {
		name     string
		id       *big.Int
		expected bool
	}{
		{
			name:     "zero",
			id:       big.NewInt(0),
			expected: true,
		},
		{
			name:     "positive number",
			id:       big.NewInt(42),
			expected: true,
		},
		{
			name:     "max ID",
			id:       MaxID(),
			expected: true,
		},
		{
			name:     "negative number",
			id:       big.NewInt(-1),
			expected: false,
		},
		{
			name:     "ring size (invalid)",
			id:       new(big.Int).Set(ringSize),
			expected: false,
		},
		{
			name:     "larger than ring size",
			id:       new(big.Int).Add(ringSize, big.NewInt(1)),
			expected: false,
		},
		{
			name:     "nil",
			id:       nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsValidID(tt.id)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Benchmark tests
func BenchmarkHashKey(b *testing.B) {
	data := []byte("test data for benchmarking")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = HashKey(data)
	}
}

func BenchmarkHashAddress(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = HashAddress("127.0.0.1", 8080)
	}
}

func BenchmarkInRange(b *testing.B) {
	id := big.NewInt(5)
	start := big.NewInt(3)
	end := big.NewInt(7)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = InRange(id, start, end)
	}
}

func BenchmarkDistance(b *testing.B) {
	start := big.NewInt(3)
	end := big.NewInt(7)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Distance(start, end)
	}
}

func BenchmarkAddPowerOfTwo(b *testing.B) {
	n := big.NewInt(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = AddPowerOfTwo(n, 10)
	}
}
