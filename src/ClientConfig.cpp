/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
#include "ClientConfig.h"
#include "ProcedureCallback.hpp"
namespace voltdb {


class DummyStatusListener : public StatusListener {
public:
    StatusListener *m_listener;
    DummyStatusListener(StatusListener *listener) : m_listener(listener) {}
    bool uncaughtException(
            std::exception exception,
            boost::shared_ptr<voltdb::ProcedureCallback> callback,
            InvocationResponse response) {
        if (m_listener != NULL) {
            return m_listener->uncaughtException(exception, callback, response);
        } else {
            std::cerr << exception.what() << std::endl;
            return false;
        }
    }
    bool connectionLost(std::string hostname, int32_t connectionsLeft) {
        if (m_listener != NULL) {
            bool retval = m_listener->connectionLost(hostname, connectionsLeft);
            return retval;
        } else {
            return false;
        }
    }
    bool connectionActive(std::string hostname, int32_t connectionsActive) {
        if (m_listener != NULL) {
            bool retval = m_listener->connectionActive(hostname, connectionsActive);
            return retval;
        } else {
            return false;
        }
    }
    bool backpressure(bool hasBackpressure) {
        if (m_listener != NULL) {
            return m_listener->backpressure(hasBackpressure);
        } else {
            return false;
        }
    }
};

    ClientConfig::ClientConfig(
            std::string username,
            std::string password, ClientAuthHashScheme scheme, bool enableAbandon,
            bool enableQueryTimeout, int timeoutInSeconds) :
            m_username(username), m_password(password), m_listener(reinterpret_cast<StatusListener*>(NULL)),
            m_maxOutstandingRequests(3000), m_hashScheme(scheme), m_enableAbandon(enableAbandon),
            m_enableQueryTimeout(enableQueryTimeout) {
        m_queryTimeout.tv_sec = timeoutInSeconds;
        m_queryTimeout.tv_usec = 0;
        m_scanIntervalForTimedoutQuery.tv_sec = DEFAULT_SCAN_INTERVAL_FOR_EXPIRED_REQUESTS_SEC;
        m_scanIntervalForTimedoutQuery.tv_usec = 0;
    }

    ClientConfig::ClientConfig(
            std::string username,
            std::string password,
            StatusListener *listener, ClientAuthHashScheme scheme, bool enableAbandon,
            bool enableQueryTimeout, int timeoutInSeconds) :
            m_username(username), m_password(password), m_listener(new DummyStatusListener(listener)),
            m_maxOutstandingRequests(3000), m_hashScheme(scheme), m_enableAbandon(enableAbandon),
            m_enableQueryTimeout(enableQueryTimeout) {
        m_queryTimeout.tv_sec = timeoutInSeconds;
        m_queryTimeout.tv_usec = 0;
        m_scanIntervalForTimedoutQuery.tv_sec = DEFAULT_SCAN_INTERVAL_FOR_EXPIRED_REQUESTS_SEC;
        m_scanIntervalForTimedoutQuery.tv_usec = 0;
    }

    ClientConfig::ClientConfig(
            std::string username,
            std::string password,
            boost::shared_ptr<StatusListener> listener, ClientAuthHashScheme scheme, bool enableAbandon,
            bool enableQueryTimeout, int timeoutInSeconds) :
                m_username(username), m_password(password), m_listener(listener),
                m_maxOutstandingRequests(3000), m_hashScheme(scheme), m_enableAbandon(enableAbandon),
                m_enableQueryTimeout(enableQueryTimeout) {
        m_queryTimeout.tv_sec = timeoutInSeconds;
        m_queryTimeout.tv_usec = 0;
        m_scanIntervalForTimedoutQuery.tv_sec = DEFAULT_SCAN_INTERVAL_FOR_EXPIRED_REQUESTS_SEC;
        m_scanIntervalForTimedoutQuery.tv_usec = 0;
    }
}

