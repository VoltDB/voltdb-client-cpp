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

#ifndef VOLTDB_CLIENT_H_
#define VOLTDB_CLIENT_H_

#include <map>
#include <deque>
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <boost/shared_ptr.hpp>
#include "CoreClient.h"

namespace voltdb {
    
    //////////////////////////////////////////////////////////////
    //
    // CLIENT CLASS FOR ACCESSING VOLTDB
    //
    //////////////////////////////////////////////////////////////
    
    /*class Client : protected CoreClient {
    public:
        Client(const voltdb_connection_callback callback,
               const std::string username = "",
               const std::string password = "");
        virtual ~Client();
        
        /*
         * Create a connection to the VoltDB process running at the specified host 
         * and port, authenticating using the username and password provided when 
         * this client was constructed.
         *
         * THIS METHOD IS ASYNC: It returns before connection completion.
         * When the connection has been established, or if there is any error,
         * the voltdb_connection_callback provided at client construction time
         * will be called.
         *
         * Reentrant / Safe to call from any thread.
         *
         * May throw voltdb::LibEventException
         */
        int createConnection(std::string hostname, int port = 21212);
        
        /*
         * Asynchronously invoke a stored procedure. The provided callback will be
         * called upon success or failure. This method returns right away.
         *
         * The VoltDB client claims no memory ownership of the payload address.
         *
         * Reentrant / Safe to call from any thread.
         *
         * May throw voltdb::LibEventException
         */
        int invoke(Procedure &proc, voltdb_proc_callback callback, void *payload);
        InvocationResponse invoke(Procedure &proc);
        
    private:
    };*/

}

#endif // VOLTDB_CLIENT_H_
