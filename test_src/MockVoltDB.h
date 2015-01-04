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

#ifndef VOLT_MOCKVOLTDB_H_
#define VOLT_MOCKVOLTDB_H_
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <boost/shared_ptr.hpp>
#include <vector>
#include <map>
#include <set>
#include "Client.h"

namespace voltdb {
class CxnContext;
class MockVoltDB {
    friend class ClientTest;
public:
    MockVoltDB(Client client);
    void readCallback(struct bufferevent *bev);
    void eventCallback(struct bufferevent *bev, short events);
    void writeCallback(struct bufferevent *bev);
    void acceptCallback(struct evconnlistener *listener,
            evutil_socket_t sock, struct sockaddr *addr, int len);
    void mimicLargeReply(int64_t, struct bufferevent *bev);
    ~MockVoltDB();

    void filenameForNextResponse(std::string filename) {
        m_filenameForNextResponse = filename;
    }

    void hangupOnRequestCount(int32_t count) {
        m_hangupOnRequestCounter = count;
    }

    void dontRead() {
        m_dontRead = true;
    }

    Client* client() { return &m_client; }
private:
    struct event_base *m_base;
    struct evconnlistener *m_listener;
    std::set<struct bufferevent*> m_connections;
    std::map<struct bufferevent*, boost::shared_ptr<CxnContext> > m_contexts;
    std::string m_filenameForNextResponse;
    int32_t m_hangupOnRequestCounter;
    bool m_dontRead;
    Client m_client;
};
}

#endif /* VOLT_MOCKVOLTDB_H_ */
