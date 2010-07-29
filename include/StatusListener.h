/*
 * StatusListener.h
 *
 *  Created on: Jun 18, 2010
 *      Author: aweisberg
 */

#ifndef VOLT_STATUSLISTENER_H_
#define VOLT_STATUSLISTENER_H_
#include <exception>

namespace voltdb {
/*
 * A status listener that an application should provide to a Client instance so
 * that the application can be notified of important events.
 */
class StatusListener {
public:
    /*
     * Handle exceptions thrown by a Client's callback. All exceptions (std::exception) are caught
     * but a client should only throw exceptions that are instances or subclasses of
     * voltdb::ClientCallbackException.
     * @param exception The exception thrown by the callback
     * @param callback Instance of the callback that threw the exception
     * @return true if the event loop should terminate and false otherwise
     */
    virtual bool uncaughtException(
            std::exception exception,
            boost::shared_ptr<voltdb::ProcedureCallback> callback) = 0;

    /*
     * Notify the client application that a connection to the database was lost
     * @param hostname Name of the host that the connection was lost to
     * @param connectionsLeft Number of connections to the database remaining
     * @return true if the event loop should terminate and false otherwise
     */
    virtual bool connectionLost(std::string hostname, int32_t connectionsLeft) = 0;

    /*
     * Notify the client application that backpressure occured
     * @param hasBackpressure True if backpressure is beginning and false if it is ending
     * @return true if the client library should queue the invocation and return from invoke()
     * or false if the library should wait until there is a connection without backpressure and then queue it.
     */
    virtual bool backpressure(bool hasBackpressure) = 0;

    virtual ~StatusListener() {}
};
}

#endif /* STATUSLISTENER_H_ */
