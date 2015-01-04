/* This file is part of VoltDB.
* Copyright (C) 2008-2015 VoltDB Inc.
*
* Permission is hereby granted, free of charge, to any person obtaining
* a copy of this software and associated documentation files (the
* "Software"), to deal in the Software without restriction, including
* without limitation the rights to use, copy, modify, merge, publish,
* distribute, sublicense, and/or sell copies of the Software, and to
* permit persons to whom the Software is furnished to do so, subject to
* the following conditions:
*
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
* MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
* IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
* OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
* ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
* OTHER DEALINGS IN THE SOFTWARE.
*/

#ifndef VOLTDB_PLATFORM_INTERFACE_H
#define VOLTDB_PLATFORM_INTERFACE_H

// Platform-specific declarations, etc..

#ifdef _MSC_VER

//=== Windows-specific tweaks.

// Disable annoying warnings that are too hard to fix.
#pragma warning( disable : 4290 )

// Additional required libraries.
#pragma comment(lib, "Ws2_32.lib")

#include <BaseTsd.h>
#include <WinSock2.h>
typedef SSIZE_T ssize_t;

// libevent checks WIN32 instead of _WIN32.
#ifndef WIN32
#define WIN32
#endif

// The shared event-config.h is set up to assume sys/time.h is available,
// even in Windows. Unset the flag to avoid using it in Windows.
// Make this the first include in source files that refer to libevent.
#include <event2/event-config.h>
#undef _EVENT_HAVE_SYS_TIME_H
#undef _EVENT_HAVE_NETDB_H
#undef _EVENT_HAVE_SYS_UIO_H

#include <Windows.h>

//TODO: Fix call-once on Windows?
#define CALL_ONCE_DECLARE(_name_)
#define CALL_ONCE_INVOKE(_name_, _func_) _func_()

#else

#include <arpa/inet.h>

#define CALL_ONCE_DECLARE(_name_) pthread_once_t _name_ = PTHREAD_ONCE_INIT
#define CALL_ONCE_INVOKE(_name_, _func_) pthread_once(&_name_, _func_)

#endif

namespace voltdb {
} // namespace voltdb

#endif /* VOLTDB_PLATFORM_INTERFACE_H */
