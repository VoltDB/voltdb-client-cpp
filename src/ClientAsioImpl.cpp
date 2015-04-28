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
#include "ClientAsioImpl.h"
#include <boost/lexical_cast.hpp>
#include "sha1.h"
#include <cassert>
#include "AuthenticationResponse.hpp"
#include "AuthenticationRequest.hpp"


#define RECONNECT_INTERVAL 10

namespace voltdb {

int32_t ClientAsioHandler::createConnection(const std::string &hostname, const unsigned short port)
throw (voltdb::Exception, voltdb::ConnectException) {

// 1. Create socket connection
    boost::asio::ip::tcp::resolver resolver(m_service);
    boost::asio::ip::tcp::resolver::query query(hostname, boost::lexical_cast<std::string>(port));
    boost::system::error_code ec;
    try {
        boost::asio::connect(m_socket, resolver.resolve(query), ec);
        if (ec) {
            throw voltdb::ConnectException();
        }
    } catch(...) {
        throw voltdb::ConnectException();
    }

// 2. Sending auth request
    AuthenticationRequest authRequest( m_username, "database", const_cast<unsigned char*>(m_passwordHash));
    ScopedByteBuffer bb(authRequest.getSerializedSize());
    authRequest.serializeTo(&bb);
    const size_t bytesSent = m_socket.write_some(boost::asio::buffer(bb.bytes(), bb.remaining()), ec);
    if (ec || bytesSent != (size_t)bb.remaining()) {
        m_socket.close();
        throw voltdb::ConnectException();
    }

// 3. Read auth request response header, i.e. 4 bytes
    char messageLengthBytes[4];
    size_t readBytes = m_socket.read_some(boost::asio::buffer(messageLengthBytes, sizeof(messageLengthBytes)), ec);

    if (ec || readBytes != sizeof(messageLengthBytes)) {
        m_socket.close();
        throw voltdb::ConnectException();
    }

// 4. Getting response message legth
    ByteBuffer messageLengthBuffer(messageLengthBytes, sizeof(messageLengthBytes));
    int32_t messageLength = messageLengthBuffer.getInt32();

    if (messageLength < 0 || messageLength > 1024*1024) {
        m_socket.close();
        throw voltdb::ConnectException();
    }

// 5. Raed auth response message body
    ScopedByteBuffer buffer(messageLength);
    readBytes = m_socket.read_some(boost::asio::buffer(buffer.bytes(), messageLength), ec);
    if (ec || readBytes != (size_t)messageLength) {
        m_socket.close();
        throw voltdb::ConnectException();
    } 

    AuthenticationResponse r = AuthenticationResponse(buffer);
    if (!r.success()) {
        m_socket.close();
        throw voltdb::ConnectException();
    }

    boost::asio::ip::tcp::no_delay option(true);
    m_socket.set_option(option);

// 6. Schedule for next inokation responce header read
    boost::shared_array<char> hdr = boost::shared_array<char>(new char[1024*4]);
    boost::asio::async_read(m_socket, boost::asio::buffer(hdr.get(), 4),
                        m_rstrand.wrap(boost::bind(&ClientAsioHandler::handleReadHeader, this, boost::asio::placeholders::error, hdr)));

// 7. return current connection host ID, used by hashinator to route requests
    return r.hostId();
    //std::cout << "ClientAsioImpl connected to server..." << std::endl;
}

// Response message body read handler
void ClientAsioHandler::handleRead(const boost::system::error_code& error, boost::shared_array<char>& msg, const size_t mgsLength) {
    if (!error) {
      //  std::cout << "ClientAsioImpl handleRead msg len: " << mgsLength << std::endl;
        InvocationResponse response(msg, mgsLength);

        boost::shared_ptr<ProcedureCallback> cb;
        {
            // grab callback object from the map
            boost::mutex::scoped_lock l(m_callbacksLock);
            CallbackMap::iterator c = m_callbacks.find(response.clientData());
            if (c != m_callbacks.end()) { 
                cb = c->second;
                m_callbacks.erase(c);
            }
        }

        // invoke on callbak
        if (cb.get()) {
            try {
                cb->callback(response);
            } catch (...) {
            }
       }

       // schedule for next response header read
       boost::shared_array<char> hdr = boost::shared_array<char>(new char[1024*4]);
       boost::asio::async_read(m_socket, boost::asio::buffer(hdr.get(), 4),
                        m_rstrand.wrap(boost::bind(&ClientAsioHandler::handleReadHeader, this, boost::asio::placeholders::error, hdr)));
    } else {
        //std::cout << "ClientAsioImpl handleRead error" << std::endl;
        doClose();
    }
}


// Response message header read handler
void ClientAsioHandler::handleReadHeader(const boost::system::error_code& error, boost::shared_array<char>& hdr) {
    if (!error) {
       // std::cout << "ClientAsioImpl handleReadHeader success" << std::endl;
        // Get message size
        ByteBuffer lengthBuffer(hdr.get(), 4);
        const size_t mgsLength = static_cast<size_t>(lengthBuffer.getInt32());
        // schedule for the message body read
        boost::shared_array<char> msg = (mgsLength > 1024*4)? boost::shared_array<char>(new char[mgsLength]): hdr;
        boost::asio::async_read(m_socket, boost::asio::buffer(msg.get(), mgsLength),
                            m_rstrand.wrap(boost::bind(&ClientAsioHandler::handleRead, this, boost::asio::placeholders::error, msg, mgsLength)));
    } else {
        //std::cout << "ClientAsioImpl handleReadHeader error" << std::endl;
        doClose();
    }
}

void ClientAsioHandler::handleWrite(const boost::system::error_code& error, SharedByteBuffer& sbb) {
    // it is mostly a noop, used to hold SharedByteBuffer arround 
    if (error) doClose();
}
void ClientAsioHandler::doWrite(SharedByteBuffer& sbb, boost::shared_ptr<ProcedureCallback>& callback, int64_t clientData) {
    //std::cout << "ClientAsioImpl doWrite" << std::endl;
    {
        boost::mutex::scoped_lock l(m_callbacksLock);
        m_callbacks[clientData] = callback;
    }
    boost::asio::async_write(m_socket, boost::asio::buffer(sbb.bytes(), sbb.remaining()),
                         m_wstrand.wrap(boost::bind(&ClientAsioHandler::handleWrite, this, boost::asio::placeholders::error, sbb)));
}

void ClientAsioHandler::doClose() {
    if (m_socket.is_open()) m_socket.close();
    boost::mutex::scoped_lock l(m_callbacksLock);
    for (CallbackMap::iterator c =  m_callbacks.begin();
                c != m_callbacks.end(); ++c) {
        c->second->callback(InvocationResponse());
    }
    m_callbacks.clear();

   //TODO: reconenct feature using a timer
}

ClientAsioHandler::ClientAsioHandler(boost::asio::io_service& service, const std::string& username, const unsigned char* passwordHash): 
m_service(service), m_wstrand(service), m_rstrand(service), m_socket(service), m_username(username), m_passwordHash(passwordHash){
}


void ClientAsioImpl::createConnection(const std::string &hostname, const unsigned short port)
throw (voltdb::Exception, voltdb::ConnectException) {
    boost::shared_ptr<ClientAsioHandler> handler(new ClientAsioHandler(m_service, m_username, m_passwordHash));
    int32_t hostId = handler->createConnection(hostname, port);
    // if connection is succesful, i.e. did not throw, save the handler in our maps
    m_handlers[hostId] = handler;
    m_handlersRR.push_back(handler);
    updateHashinator();
}

boost::shared_ptr<ClientAsioHandler> ClientAsioImpl::routeProcedure(Procedure &proc, SharedByteBuffer &sbb){
    boost::shared_ptr<ClientAsioHandler> handler;
    if (m_distributer.isUpdating()) return handler;
    ProcedureInfo *procInfo = m_distributer.getProcedure(proc.getName());

    //route transaction to correct event if procedure is found, transaction is single partitioned
    int hostId = -1;
    if (procInfo && !procInfo->m_multiPart){
        const int hashedPartition = m_distributer.getHashedPartitionForParameter(sbb, procInfo->m_partitionParameter);
        if (hashedPartition >= 0) {
            hostId = m_distributer.getHostIdByPartitionId(hashedPartition);
        }
    } else {
        //use MIP partition instead
        hostId = m_distributer.getHostIdByPartitionId(Distributer::MP_INIT_PID);
    }
    return (hostId >= 0 && m_handlers[hostId]->isOpen())? m_handlers[hostId]: handler;
}

void ClientAsioImpl::invoke(Procedure &proc, const boost::shared_ptr<ProcedureCallback>& callback)
throw (voltdb::Exception, voltdb::NoConnectionsException, voltdb::UninitializedParamsException, voltdb::ElasticModeMismatchException) {
   //std::cout << "invoke start" << std::endl;
    if (callback.get() == NULL) throw voltdb::NullPointerException();

    const size_t connectionCount = m_handlersRR.size();
    size_t roundRobinLoopCount = connectionCount;

// Serialize the stored proc in to a buffer
    const int32_t messageSize = proc.getSerializedSize();
    boost::shared_array<char> msg = boost::shared_array<char>(new char[messageSize]);
    SharedByteBuffer sbb(msg, messageSize);
    const int64_t clientData = m_nextRequestId.fetch_add(1, boost::memory_order_relaxed);
    proc.serializeTo(&sbb, clientData);

// Try to route the procedure
    boost::shared_ptr<ClientAsioHandler> handler(routeProcedure(proc, sbb));
    size_t nextConnectionIndex = m_nextConnectionIndex.fetch_add(1, boost::memory_order_relaxed);

// If routing did not work just do a roundrobin
    while (roundRobinLoopCount-- && !handler.get()) {
        if (m_handlersRR[m_nextConnectionIndex%connectionCount]->isOpen()) {
            handler = m_handlersRR[m_nextConnectionIndex%connectionCount];
        }
        ++nextConnectionIndex;
    }
    if (!handler.get()) throw voltdb::NoConnectionsException();

    // dispatch with in a selected handler
    m_service.dispatch(boost::bind(&ClientAsioHandler::doWrite, handler.get(), sbb, callback, clientData));
    //std::cout << "invoke end" << std::endl;
} 

ClientAsioImpl::~ClientAsioImpl() {
    m_work = boost::none;
    m_service.stop();
    m_ioThreads.join_all();
}

ClientAsioImpl::ClientAsioImpl(ClientConfig config) throw(voltdb::Exception, voltdb::LibEventException):
m_work(boost::in_place(boost::ref(m_service))),
m_nextRequestId(INT64_MIN),
m_nextConnectionIndex(0),
m_username(config.m_username),
m_affinityEnabled(true) {
    SHA1_CTX context;
    SHA1_Init(&context);
    SHA1_Update(&context, reinterpret_cast<const unsigned char*>(config.m_password.data()), config.m_password.size());
    SHA1_Final(&context, m_passwordHash);

    m_ioThreads.create_thread(boost::bind(&boost::asio::io_service::run, &m_service));
}

ClientAsioImpl::ClientAsioImpl(ClientConfig config, const size_t threadPoolSize) throw(voltdb::Exception, voltdb::LibEventException):
m_work(boost::in_place(boost::ref(m_service))),
m_nextRequestId(INT64_MIN),
m_nextConnectionIndex(0),
m_username(config.m_username),
m_affinityEnabled(true) {
    SHA_CTX context;
    SHA1_Init(&context);
    SHA1_Update(&context, reinterpret_cast<const unsigned char*>(config.m_password.data()), config.m_password.size());
    SHA1_Final(m_passwordHash,&context);

    for (size_t i = 0 ; i<threadPoolSize; i++) {
        m_ioThreads.create_thread(boost::bind(&boost::asio::io_service::run, &m_service));
    }
}

/*
 *Callback for async topology update for transaction routing algorithm
 */
class TopoUpdateCallbackAsio : public voltdb::ProcedureCallback
{
public:
    TopoUpdateCallbackAsio(Distributer *dist):m_dist(dist){}
    bool callback(InvocationResponse response) throw (voltdb::Exception)
    {
        if (!response.failure()){
            m_dist->updateAffinityTopology(response.results());
        }
        return false;
    }
 private:
    Distributer *m_dist;
};
/*
 * Callback for ("@SystemCatalog", "PROCEDURES")
 */
class ProcUpdateCallbackAsio : public voltdb::ProcedureCallback
{
public:
    ProcUpdateCallbackAsio(Distributer *dist):m_dist(dist){}
    bool callback(InvocationResponse response) throw (voltdb::Exception)
    {
        if (!response.failure()){
            m_dist->updateProcedurePartitioning(response.results());
        }
        return false;
    }

 private:
    Distributer *m_dist;
};



void ClientAsioImpl::updateHashinator(){
    if (!m_affinityEnabled) return;
    m_distributer.startUpdate();
    voltdb::Procedure systemCatalogProc("@SystemCatalog");
    voltdb::ParameterSet* params = systemCatalogProc.params();
    params->addString("PROCEDURES");

    boost::shared_ptr<voltdb::ProcedureCallback> procCallback(new ProcUpdateCallbackAsio(&m_distributer));

    voltdb::Procedure statisticsProc("@Statistics");
    params = statisticsProc.params();
    params->addString("TOPO").addInt32(0);

    boost::shared_ptr<voltdb::ProcedureCallback> topoCallback(new TopoUpdateCallbackAsio(&m_distributer));

    invoke(systemCatalogProc, procCallback);
    invoke(statisticsProc, topoCallback);

}

}
