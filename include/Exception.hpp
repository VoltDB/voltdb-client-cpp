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

#ifndef VOLTDB_EXCEPTION_HPP_
#define VOLTDB_EXCEPTION_HPP_
#include <exception>

namespace voltdb {

/*
 * Base class for all exceptions thrown by the VoltDB client API
 */
class Exception : public std::exception {
public:
    virtual ~Exception() throw() {}
    virtual const char* what() const throw() {
        return "An unknown error occured in the VoltDB client API";
    }
};

class NullPointerException : public Exception {
public:
    NullPointerException() :
        Exception() {}
    virtual const char* what() const throw() {
        return "Found a null pointer where an address was expected";
    }
};

/*
 * Thrown when attempting to retrieve a column from a Row using a column
 * index that is < 0, > num columns, or when using a getter with an inappropriate data type
 */
class InvalidColumnException : public voltdb::Exception {
public:
    InvalidColumnException() :
        Exception() {}
    virtual const char* what() const throw() {
        return "Attempted to retrieve a column with an invalid index or name, or an invalid type for the specified column";
    }
};

/*
 * Thrown by ByteBuffer when an attempt is made to get or put data beyond the limit or capacity of the ByteBuffer
 * Users should never see this error since they don't access ByteBuffers directly. They access them
 * through message wrappers like AuthenticationRequest, AuthenticationResponse, InvocationResponse, Table,
 * TableIterator, and Row.
 */
class OverflowUnderflowException : public voltdb::Exception {
public:
    OverflowUnderflowException() : Exception() {}
    virtual const char* what() const throw() {
        return "Overflow underflow exception";
    }
};

/*
 * Thrown by ByteBuffer when an attempt is made to access a value at a specific index or set the position/limit
 * to a specific index that is < 0 or > the limit/capacity. This should never be seen by users.
 */
class IndexOutOfBoundsException : public voltdb::Exception {
public:
    IndexOutOfBoundsException() : Exception() {}
    virtual const char* what() const throw() {
        return "Index out of bounds exception";
    }
};

/*
 * Thrown by ByteBuffer when an attempt is made to expand a buffer that is not expandable. Should
 * never be seen by users.
 */
class NonExpandableBufferException : public voltdb::Exception {
public:
    NonExpandableBufferException() : Exception() {}
    virtual const char* what() const throw() {
        return "Attempted to add/expand a nonexpandable buffer";
    }
};

/*
 * Thrown when an attempt is made to invoke a procedure without initializing all the parameters to the procedure.
 * Users may see this exception.
 */
class UninitializedParamsException : public voltdb::Exception {
public:
    UninitializedParamsException() : Exception() {}
    virtual const char* what() const throw() {
        return "Not all parameters were set";
    }
};

/*
 * Thrown when an attempt is made to add a parameter to a parameter set that is the wrong type for the argument
 * or an extra argument. Users may see this exception.
 */
class ParamMismatchException : public voltdb::Exception {
public:
    ParamMismatchException() : Exception() {}
    virtual const char* what() const throw() {
        return "Attempted to set a parameter using the wrong type";
    }
};

/*
 * Thrown by TableIterator when next() is invoked when there are no more rows to return. Users may see this exceptino.
 */
class NoMoreRowsException : public voltdb::Exception {
public:
    NoMoreRowsException() : Exception() {}
    virtual const char* what() const throw() {
        return "Requests another row when there are no more";
    }
};

/*
 * Thrown by the Decimal string constructor when an attempt is made to construct a Decimal with
 * an invalid string. Users may see this exception.
 */
class StringToDecimalException : public voltdb::Exception {
public:
    StringToDecimalException() : Exception() {}
    virtual const char* what() const throw() {
        return "Parse error constructing decimal from string";
    }
};

/*
 * Thrown when an application specific error occurs during the connection process.
 * e.g. Authentication fails while attempting to connect to a node
 */
class ConnectException : public voltdb::Exception {
public:
    ConnectException() : Exception() {}
    virtual const char* what() const throw() {
        return "An error occured while attempting to create and authenticate a connection to VoltDB";
    }
};

/*
 * Thrown when an attempt is made to invoke a procedure or run the event loop when
 * there are no connections to VoltDB.
 */
class NoConnectionsException : public voltdb::Exception {
public:
    NoConnectionsException() : Exception() {}
    virtual const char* what() const throw() {
        return "Attempted to invoke a procedure while there are no connections";
    }
};

/*
 * Thrown when an attempt is made to return a client that does not belong to this thread
 * back to connection pool.
 */
class MisplacedClientException : public voltdb::Exception {
public:
    MisplacedClientException() : Exception() {}
    virtual const char* what() const throw() {
        return "Attempted to return a client that does not belong to this thread";
    }
};

/*
 * Thrown when libevent returns a failure code. These codes are not specific so it isn't possible
 * to tell what happened.
 */
class LibEventException : public voltdb::Exception {
public:
    LibEventException() : Exception() {}
    virtual const char* what() const throw() {
        return "Lib event generated an unexpected error";
    }
};

class ClusterInstanceMismatchException : public voltdb::Exception {
public:
    ClusterInstanceMismatchException() : Exception() {}
    virtual const char* what() const throw() {
        return "Attempted to connect a client to two separate VoltDB clusters";
    }
};
}

#endif /* VOLTDB_EXCEPTION_HPP_ */
