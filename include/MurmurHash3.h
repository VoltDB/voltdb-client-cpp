//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#ifndef _MURMURHASH3_H_
#define _MURMURHASH3_H_
#include <stdint.h>

namespace voltdb {
int32_t MurmurHash3_x64_128 ( const void * key, int len, uint32_t seed );

inline int32_t MurmurHash3_x64_128 ( int64_t value, uint32_t seed) {
    return MurmurHash3_x64_128( &value, 8, seed);
}

inline int32_t MurmurHash3_x64_128 ( int64_t value) {
    return MurmurHash3_x64_128(value, 0);
}
}
#endif // _MURMURHASH3_H_
