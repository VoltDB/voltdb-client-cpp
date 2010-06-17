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

#ifndef VOLTDB_EXCEPTION_HPP_
#define VOLTDB_EXCEPTION_HPP_
#include <exception>

namespace voltdb {
class Exception : public std::exception {
public:
    Exception() : m_what("") {}
    Exception(const char *what) : m_what("") {}
    virtual ~Exception() throw() {}
    virtual const char* what() const throw() {
        return m_what;
    }
    const char *m_what;
};

class InvalidColumnException : public voltdb::Exception {
public:
    InvalidColumnException() :
        Exception("Attempted to retrieve a column with an invalid index or name, or an invalid type for the specified column") {}
};

class OverflowUnderflowException : public voltdb::Exception {
public:
    OverflowUnderflowException() : Exception("Overflow underflow exception") {}
};

class IndexOutOfBoundsException : public voltdb::Exception {
public:
    IndexOutOfBoundsException() : Exception("Index out of bounds exception") {}
};

class NonExpandableBufferException : public voltdb::Exception {
public:
    NonExpandableBufferException() : Exception("Attempted to add/expand a nonexpandable buffer") {}
};

class UninitializedParamsException : public voltdb::Exception {
public:
    UninitializedParamsException() : Exception("Not all parameters were set") {}
};

class ParamMismatchException : public voltdb::Exception {
public:
    ParamMismatchException() : Exception("Attempted to set a parameter using the wrong type") {}
};

class NoMoreRowsException : public voltdb::Exception {
public:
    NoMoreRowsException() : Exception("Requests another row when there are no more") {}
};

class StringToDecimalException : public voltdb::Exception {
public:
    StringToDecimalException() : Exception("Parse error constructing decimal from string") {}
};
}

#endif /* VOLTDB_EXCEPTION_HPP_ */
