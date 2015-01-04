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

#ifndef VOLTDB_CORECLIENT_H_
#define VOLTDB_CORECLIENT_H_

#include <map>
#include <deque>
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <boost/shared_ptr.hpp>
#include "Procedure.hpp"
#include "AuthenticationResponse.hpp"
#include "InvocationResponse.hpp"

namespace voltdb {
    
// forward declarations used internally
class CxnContext;
class PreparedInvocation;
class MockVoltDB;
class CoreClient;

//////////////////////////////////////////////////////////////
//
// CALLBACKS FOR ASYNCRONOUS CLIENT USAGE
//
//////////////////////////////////////////////////////////////
    
const int EVENT_LOOP_ERROR = -1;
const int TIMEOUT_ELAPSED = 0;
const int INTERRUPTED_OR_EARLY_EXIT = 1;
    
enum connection_event_type {
    // the connection is ready to accept work
    CONNECTED,
    // the connection has been disconnected remotely or locally
    CONNECTION_LOST,
    // the connection is currently full of work and will reject subsequent work
    BACKPRESSURE_ON,
    // the connection is writable again
    BACKPRESSURE_OFF
};
    
struct connection_event {
    connection_event_type type;
    std::string hostname;
    int port;
    std::string info; // additional event description
};
    
// function pointers for callbacks
    
// callback for connection changes
typedef void (*voltdb_connection_callback)(CoreClient*, struct connection_event);
    
// callback for procedures
// void* is user_data, passed from Client::invoke(..) as payload
typedef void (*voltdb_proc_callback)(CoreClient*, InvocationResponse, void *);

//////////////////////////////////////////////////////////////
//
// CORE CLIENT CLASS PURE ASYNC ACCESS TO VOLTDB
//
//////////////////////////////////////////////////////////////
    
class CoreClient {
    friend class MockVoltDB;
public:
    /*
     * Construct a VoltDB client, with various configuration options.
     * Initially not connected to any VoltDB nodes.
     * When VoltDB isn't using authentication, any user/password is fine,
     *  though using the empty string is convention.
     * using the username and password provided when this client was constructed
     * 
     * May throw voltdb::LibEventException
     */
    explicit CoreClient(const voltdb_connection_callback callback,
                        const std::string username = "",
                        const std::string password = "");
    
    virtual ~CoreClient();

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
    
    /*
     * Run the event loop and process zero or one active events.
     *
     * Returns 0 on success and -1 on error.
     */
    int runOnce();
    
    /*
     * Run the event loop until interrupt() is called or an error occurs.
     *
     * Returns EVENT_LOOP_ERROR or INTERRUPTED_OR_EARLY_EXIT
     *
     * Functionally equivalent to runWithTimeout(..) with a 3yr timeout, so
     * technically it can also return TIMEOUT_ELAPSED.
     */
    int run();
    
    /*
     * Run the event loop until a timer elapses, until interrupt() is called
     * or an error occurs.
     *
     * Returns EVENT_LOOP_ERROR, INTERRUPTED_OR_EARLY_EXIT or TIMEOUT_ELAPSED
     */
    int runWithTimeout(int64_t ms);

    /*
     * If one of the run family of methods is running on another thread, this
     * method will instruct it to exit as soon as it finishes it's current 
     * immediate task. If the thread in the run method is blocked/idle, then
     * it will return immediately.
     */
    void interrupt();
    
    /**
     * Set a user-supplied data pointer that can be retrieved at any time.
     * No explicit thread-safety. No memory ownership is assumed.
     */
    void setData(void *data);
    
    /**
     * Return user-supplied data pointer. Return NULL if unset (or set to NULL).
     * No explicit thread-safety.
     */
    void *getData();
    
    // public only for libevent callbacks. DO NOT CALL
    // this will be moved in a future revision
    void completeAuthenticationRequest(struct CxnContext *context);
    bool processAuthenticationResponse(struct CxnContext *context, AuthenticationResponse &response);
    void invocationRequestCallback();
    void regularReadCallback(struct CxnContext *context);
    void regularEventCallback(struct CxnContext *context, short events);
    void regularWriteCallback(struct CxnContext *context);
    void timerFired();

private:
    
    CxnContext *getNextContext(voltdb_proc_callback callback, void *payload);
    
    // shared data between threads that's threadsafe
    struct event_base *m_base;
    
    // shared state between threads with mutex protection
    pthread_mutex_t m_contexts_mutex;
    std::vector<boost::shared_ptr<CxnContext> > m_contexts;
    pthread_mutex_t m_requests_mutex;
    std::deque<PreparedInvocation> m_requests;
    
    // used by the event-processing thread
    int64_t m_nextRequestId;
    voltdb_connection_callback m_connCallback;
    bool m_timerFired;
    
    bool m_instanceIdIsSet;
    int32_t m_outstandingRequests;
    
    //Identifier of the database instance this client is connected to
    int64_t m_clusterStartTime;
    int32_t m_leaderAddress;

    std::string m_username;
    unsigned char m_passwordHash[20];
    
    // user data pointer
    void *m_data;
};
}
#endif // VOLTDB_CORECLIENT_H_
