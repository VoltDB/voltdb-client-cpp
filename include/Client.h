/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
#include "Procedure.hpp"
#include "AuthenticationResponse.hpp"
#include "InvocationResponse.hpp"

namespace voltdb {
    
// forward declarations used internally
class CxnContext;
class PreparedInvocation;
class MockVoltDB;
class Client;

//////////////////////////////////////////////////////////////
//
// CALLBACKS FOR ASYNCRONOUS CLIENT USAGE
//
//////////////////////////////////////////////////////////////
    
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
    short port;
    std::string info; // additional event description
};
    
// function pointers for callbacks
    
// callback for connection changes
typedef void (*voltdb_connection_callback)(Client*, struct connection_event);
    
// callback for procedures
// void* is user_data, passed from Client::invoke(..) as payload
typedef void (*voltdb_proc_callback)(Client*, InvocationResponse, void *);

//////////////////////////////////////////////////////////////
//
// CLIENT CLASS FOR ACCESSING VOLTDB
//
//////////////////////////////////////////////////////////////
    
class Client {
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
    explicit Client(const voltdb_connection_callback callback,
                    const std::string username = "",
                    const std::string password = "");
    
    ~Client();

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
    void createConnection(std::string hostname, short port = 21212);
    
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
    void invoke(Procedure &proc, voltdb_proc_callback callback, void *payload);
    
    /*
     * Run the event loop and process zero or one active events.
     *
     * May throw voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException
     */
    void runOnce();
    
    /*
     * Run the event loop until a procedure or connection callback is called.
     * This method will return (perhaps immediately) if there are no active
     * or pending connections.
     *
     * May throw voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException
     */
    void run();
    
    /*
     * Run the event loop until a procedure or connection callback is called,
     * or until a timer elapses. This method will only return in one of these
     * two cases, even if there are no connections.
     *
     * May throw voltdb::Exception, voltdb::NoConnectionsException, voltdb::LibEventException
     */
    void runWithTimeout(int ms); 

    /*
     * If one of the run family of methods is running on another thread, this
     * method will instruct it to exit as soon as it finishes it's current 
     * immediate task. If the thread in the run method is blocked/idle, then
     * it will return immediately.
     */
    void interrupt();
    
    // public only for libevent callbacks. DO NOT CALL
    // this will be moved in a future revision
    void completeAuthenticationRequest(struct CxnContext *context);
    bool processAuthenticationResponse(struct CxnContext *context, AuthenticationResponse &response);
    void invocationRequestCallback();
    void regularReadCallback(struct CxnContext *context);
    void regularEventCallback(struct CxnContext *context, short events);
    void regularWriteCallback(struct CxnContext *context);

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
    
    bool m_instanceIdIsSet;
    int32_t m_outstandingRequests;
    
    //Identifier of the database instance this client is connected to
    int64_t m_clusterStartTime;
    int32_t m_leaderAddress;

    std::string m_username;
    unsigned char m_passwordHash[20];
};
}
#endif /* VOLTDB_CLIENT_H_ */
