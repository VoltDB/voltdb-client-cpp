/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

#ifndef CLIENT_H_
#define CLIENT_H_

#include <string>
#include "ProcedureCallback.hpp"
#include "Procedure.hpp"
#include <boost/shared_ptr.hpp>
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <vector>
#include <boost/unordered/unordered_map.hpp>
#include <boost/unordered/unordered_set.hpp>

namespace voltdb {
typedef boost::unordered_map< int64_t, boost::shared_ptr<ProcedureCallback> > CallbackMap;
typedef boost::unordered_map< struct bufferevent*, CallbackMap* > BEVToCallbackMap;

class Client;
class CxnContext {
public:
    CxnContext(Client *client, struct event_base *base,
            boost::unordered_set<struct bufferevent *> *backpressuredBevs,
            BEVToCallbackMap *callbacks)
        : m_client(client), m_base(base), m_lengthOrMessage(true), m_nextLength(4),
          m_backpressuredBevs(backpressuredBevs),
          m_callbacks(callbacks), m_invocationBlockedOnBackpressure(false) {}
    Client *m_client;
    struct event_base *m_base;
    bool m_lengthOrMessage;
    int32_t m_nextLength;
    boost::unordered_set<struct bufferevent *> *m_backpressuredBevs;
    BEVToCallbackMap *m_callbacks;
    bool m_invocationBlockedOnBackpressure;
};

class Client {
public:
    bool createConnection(std::string hostname, std::string username, std::string password);
    boost::shared_ptr<InvocationResponse> invoke(Procedure *proc);
    void invoke(Procedure *proc, boost::shared_ptr<ProcedureCallback> callback);
    bool runOnce();
    bool run();
    static boost::shared_ptr<Client> create();
    ~Client();
private:
    Client();
    struct event_base *m_base;
    int64_t m_nextRequestId;
    uint64_t m_nextConnectionIndex;
    std::vector<std::pair<struct bufferevent *, boost::shared_ptr<CxnContext> > > m_bevs;
    boost::unordered_set<struct bufferevent *> m_backpressuredBevs;
    BEVToCallbackMap m_callbacks;
};
}

#endif /* CLIENT_H_ */
