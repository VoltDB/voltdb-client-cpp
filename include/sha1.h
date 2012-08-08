/* public api for steve reid's public domain SHA-1 implementation */
/* this file is in the public domain */

#ifndef __VDB_SHA1_H
#define __VDB_SHA1_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint32_t state[5];
    uint32_t count[2];
    uint8_t  buffer[64];
} VDB_SHA1_CTX;

#define VDB_SHA1_DIGEST_SIZE 20

void VDB_SHA1_Init(VDB_SHA1_CTX* context);
void VDB_SHA1_Update(VDB_SHA1_CTX* context, const uint8_t* data, const size_t len);
void VDB_SHA1_Final(VDB_SHA1_CTX* context, uint8_t digest[VDB_SHA1_DIGEST_SIZE]);

#ifdef __cplusplus
}
#endif

#endif /* __SHA1_H */
