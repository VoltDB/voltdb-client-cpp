/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#ifndef VOLTDB_CLIENTASIOIMPL_H_
#define VOLTDB_CLIENTASIOIMPL_H_
#include <map>
#include "ProcedureCallback.hpp"
#include "StatusListener.h"
#include "Procedure.hpp"
#include "ByteBuffer.hpp"
#include <boost/atomic.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/asio.hpp>
#include "ClientConfig.h"
#include "Distributer.h"

namespace voltdb {

class ClientAsioHandler;

class ClientAsioImpl {
public:
    void createConnection(const std::string &hostname, const unsigned short port) throw (voltdb::Exception, voltdb::ConnectException);

    void invoke(Procedure &proc, const boost::shared_ptr<ProcedureCallback>& callback) throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::ElasticModeMismatchException);

    ~ClientAsioImpl();

    ClientAsioImpl(ClientConfig config) throw(voltdb::Exception, voltdb::LibEventException);
    ClientAsioImpl(ClientConfig config, const size_t threadPoolSize) throw(voltdb::Exception, voltdb::LibEventException);

    void setAffinityEnabled(const bool enabled) {
        m_affinityEnabled = enabled;
    }
private:

    boost::shared_ptr<ClientAsioHandler> routeProcedure(Procedure &proc, SharedByteBuffer &sbb);
    void updateHashinator();

    boost::asio::io_service m_service;
    boost::optional<boost::asio::io_service::work> m_work;
    boost::thread_group m_ioThreads;
    boost::atomic<int64_t> m_nextRequestId;
    boost::atomic<size_t> m_nextConnectionIndex;
    std::map<int, boost::shared_ptr<ClientAsioHandler> > m_handlers;
    std::vector<boost::shared_ptr<ClientAsioHandler> > m_handlersRR;

    std::string m_username;
    unsigned char m_passwordHash[20];

    Distributer  m_distributer;

    bool m_affinityEnabled;
};

class ClientAsioHandler {
    friend class ClientAsioImpl;
public:
    ~ClientAsioHandler(){};
private:
    ClientAsioHandler(boost::asio::io_service& m_service, const std::string& username, const unsigned char* m_passwordHash);

    int32_t createConnection(const std::string &hostname, const unsigned short port) throw (voltdb::Exception, voltdb::ConnectException);

    void doWrite(SharedByteBuffer& sbb, boost::shared_ptr<ProcedureCallback>& callback, int64_t clientData);
    void handleWrite(const boost::system::error_code& error, SharedByteBuffer& sbb);
    void handleReadHeader(const boost::system::error_code& error, boost::shared_array<char>& hdr);
    void handleRead(const boost::system::error_code& error, boost::shared_array<char>& msg, const size_t mgsLength);
    void doClose();
    bool isOpen() const {return m_socket.is_open();}

    boost::asio::io_service& m_service;
    boost::asio::strand m_wstrand;
    boost::asio::strand m_rstrand;
    boost::asio::ip::tcp::socket m_socket;

    const std::string m_username;
    const unsigned char* m_passwordHash;

    typedef std::map<int64_t, boost::shared_ptr<ProcedureCallback> > CallbackMap;
    CallbackMap m_callbacks;
    boost::mutex m_callbacksLock;
};
}
#endif /* VOLTDB_CLIENTASIOIMPL_H_ */
