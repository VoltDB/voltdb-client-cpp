/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include "PlatformInterface.hpp"
#include <cstdio>
#include <exception>
#include <sstream>



namespace voltdb {
using std::ostringstream;

// when adding new Exception, add it's corresponding error code to enum entry below
enum nativeErrorCode{
    errException = 0,
    errNullPointerException,
    errInvalidColumnException,
    errOverflowUnderflowException,
    errIndexOutOfBoundException,
    errNonExpandableBufferException,
    errUninitializedParamsException,
    errParamMismatchException,
    errNoMoreRowsException,
    errStringToDecimalException,
    errConnectException,
    errNoConnectionException,
    errLibEventException,
    errClusterInstanceMismatchException,
    errColumnMismatchException,
    errMisplacedClientException,
    errElasticModeMismatchException,
    errUnknownProcedureException,
    errCoordinateOutOfRangeException,
    errUnsupportedTypeException,
};

/*
 * Base class for all exceptions thrown by the VoltDB client API
 */
class Exception : public std::exception {
public:
    Exception() : m_errorCode(errException){};
    virtual ~Exception() throw() {}
    virtual const char* what() const throw() {
        return "An unknown error occured in the VoltDB client API";
    }
protected:
    Exception(nativeErrorCode errCode) : m_errorCode(errCode){};
    nativeErrorCode m_errorCode;        // needed for ODBC driver
};

class NullPointerException : public Exception {
public:
    NullPointerException() :
        Exception(errNullPointerException) {}
    virtual const char* what() const throw() {
        return "Found a null pointer where an address was expected";
    }
};

/*
 * Thrown when attempting to retrieve a column from a Row using a column
 * index that is < 0, > num columns, or when using a getter with an inappropriate data type
 */
class InvalidColumnException : public voltdb::Exception {
    std::string m_what;
public:
    explicit InvalidColumnException() :
        Exception(errInvalidColumnException) {
        m_what = "Attempted to retrieve a column with an invalid index or name, or an invalid type for the specified column";
    }

    explicit InvalidColumnException(const size_t index) :
        Exception(errInvalidColumnException) {
        ostringstream strStream;
        strStream << "Attempted to retrieve a column with an invalid index: " << index;
        m_what = strStream.str();
    }

    explicit InvalidColumnException(const std::string& name) :
        Exception(errInvalidColumnException) {
        m_what = "Attempted to retrieve a column with an invalid name: " + name;
    }

    explicit InvalidColumnException(const std::string& columnName, const size_t type, const std::string& typeName, const std::string& expectedTypeName) :
        Exception(errInvalidColumnException) {
        ostringstream strStream;
        strStream << "Attempted to retrieve a column: " << columnName << " with an invalid type: " << typeName << "<" << type << "> expected: " <<expectedTypeName.c_str();
        m_what = strStream.str();
    }

    virtual ~InvalidColumnException() throw() {}

    virtual const char* what() const throw() {
        return m_what.c_str();
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
    OverflowUnderflowException() : Exception(errOverflowUnderflowException) {}
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
    IndexOutOfBoundsException() : Exception(errIndexOutOfBoundException) {}
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
    NonExpandableBufferException() : Exception(errNonExpandableBufferException) {}
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
    UninitializedParamsException() : Exception(errUninitializedParamsException) {}
    virtual const char* what() const throw() {
        return "Not all parameters were set";
    }
};

/*
 * Thrown when an attempt is made to add a parameter to a parameter set that is the wrong type for the argument
 * or an extra argument. Users may see this exception.
 */
class ParamMismatchException : public voltdb::Exception {
    std::string m_what;
public:
    explicit ParamMismatchException() : Exception(errParamMismatchException) {
        m_what = "Attempted to set a parameter using the wrong type";
    }
    explicit ParamMismatchException(const size_t type, const std::string& typeName) :
        Exception(errParamMismatchException) {
        ostringstream strStream;
        strStream << "Attempted to set a parameter using the wrong type: " << typeName << "<" << type << ">";
        m_what = strStream.str();
    }
    virtual ~ParamMismatchException() throw() {}
    virtual const char* what() const throw() {
        return m_what.c_str();
    }
};

/*
 * Thrown when a user attempts to use a type that is not supported.
 * Users may see this exception.
 */
class UnsupportedTypeException : public voltdb::Exception {
public:

    explicit UnsupportedTypeException(const std::string& typeName) :
        Exception(errUnsupportedTypeException) {
        ostringstream strStream;
        strStream << "Attempted to use a SQL type that is unsupported in the C++ client: " << typeName;
        m_what = strStream.str();
    }

    virtual ~UnsupportedTypeException() throw() {}

    virtual const char* what() const throw() {
        return m_what.c_str();
    }

private:

    std::string m_what;
};

/*
 * Thrown by Distributer when detects server run in LEGACY mode
 */
class ElasticModeMismatchException : public voltdb::Exception {
public:
    ElasticModeMismatchException() : Exception(errElasticModeMismatchException) {}
   virtual const char* what() const throw() {
       return "LEGACY mode is not supported";
   }
};

/*
 * Thrown by TableIterator when next() is invoked when there are no more rows to return. Users may see this exceptino.
 */
class NoMoreRowsException : public voltdb::Exception {
public:
    NoMoreRowsException() : Exception(errNoMoreRowsException) {}
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
    StringToDecimalException() : Exception(errStringToDecimalException) {}
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
    ConnectException() : Exception(errConnectException) {}
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
    NoConnectionsException() : Exception(errNoConnectionException) {}
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
    MisplacedClientException() : Exception(errMisplacedClientException) {}
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
    LibEventException() : Exception(errLibEventException) {}
    virtual const char* what() const throw() {
        return "Lib event generated an unexpected error";
    }
};

class ClusterInstanceMismatchException : public voltdb::Exception {
public:
    ClusterInstanceMismatchException() : Exception(errClusterInstanceMismatchException) {}
    virtual const char* what() const throw() {
        return "Attempted to connect a client to two separate VoltDB clusters";
    }
};

class UnknownProcedureException : public voltdb::Exception {
    std::string m_what;
public:
    explicit UnknownProcedureException() : Exception(errUnknownProcedureException) {
        m_what = "Unknown procedure invoked";
    }
    explicit UnknownProcedureException(const std::string& name) : Exception(errUnknownProcedureException) {
        m_what = "Unknown procedure invoked: " + name;
    }
    virtual ~UnknownProcedureException() throw() {}
    virtual const char* what() const throw() {
        return m_what.c_str();
    }
};

class CoordinateOutOfRangeException : public voltdb::Exception {
    std::string m_what;
public:
    explicit CoordinateOutOfRangeException() : Exception(errCoordinateOutOfRangeException) {
        m_what = "Coordinate out of Range";
    }
    explicit CoordinateOutOfRangeException(const std::string& name) : Exception(errCoordinateOutOfRangeException) {
        m_what = name + " coordinate out of range.";
    }
    virtual ~CoordinateOutOfRangeException() throw() {}
    virtual const char* what() const throw() {
        return m_what.c_str();
    }
};

}

#endif /* VOLTDB_EXCEPTION_HPP_ */
