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

#include "Client.h"

#include "Vertica.h"

class exception;
using namespace Vertica;
using namespace std;

/*
 * Call a stored proc in VoltDB
 */
class VoltDBCall : public ScalarFunction
{
public:
    std::string m_server;
    std::string m_procedure;

    /*
     * This method processes a block of rows in a single invocation.
     *
     * The inputs are retrieved via argReader
     * The outputs are returned via resWriter
     */
    virtual void processBlock(ServerInterface &srvInterface,
                              BlockReader &reader,
                              BlockWriter &resWriter)
    {
        try {
            
            log("Server is %s", m_server.c_str());
            log("Procedure is %s", m_procedure.c_str());

            const SizedColumnTypes &typemd = reader.getTypeMetaData();
	    voltdb::ClientConfig config("","");
            voltdb::Client client = voltdb::Client::create(config);
            client.createConnection(m_server);

            unsigned int numofcol = reader.getNumCols();
            vector<voltdb::Parameter> vals(numofcol);
            voltdb::ParameterSet params(numofcol);
            do {
                params.reset();
                for (unsigned int i = 0; i < numofcol; i++) {
                    VerticaType vt = typemd.getColumnType(i);
                    if (vt.isInt() || vt.isBool() || vt.isInterval()) {
                        vals[i] = voltdb::Parameter(voltdb::WIRE_TYPE_INTEGER);
			params.putInt32(reader.getIntRef(i));
                    } else if (vt.isStringType() || vt.isLongVarchar() || vt.isChar()) {
                        vals[i] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
			params.putString(reader.getStringRef(i).str());
                    } else if (vt.isFloat() || vt.isNumeric()) {
                        vals[i] = voltdb::Parameter(voltdb::WIRE_TYPE_FLOAT);
			params.putDouble(reader.getFloatRef(i));
                    } else if (vt.isTimestamp()) {
			time_t tval;
			tval = getUnixTimeFromTimestamp(reader.getTimestampRef(i));
			tval *= (1000000);
                        vals[i] = voltdb::Parameter(voltdb::WIRE_TYPE_TIMESTAMP);
			params.putTimestamp(tval);
                    } else if (vt.isDate()) {
			time_t tval;
			tval = getUnixTimeFromDate(reader.getDateRef(i));
			tval *= (1000000);
                        vals[i] = voltdb::Parameter(voltdb::WIRE_TYPE_TIMESTAMP);
			params.putTimestamp(tval);
                    } else {
			vt_report_error(0, "Unknown or unsupported type");
		    }
                }
                voltdb::Procedure iproc(m_procedure.c_str(), vals);
                try {
                    client.invoke(iproc, &params);
		} catch(...) {
		    log("Exception in invoking procedure on VoltDB");
		}
                resWriter.setInt(0);
		resWriter.next();
            } while (reader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }

};

class VoltDBCallFactory : public ScalarFunctionFactory
{
    std::string m_server;
    std::string m_procedure;

    // return an instance of VoltDBCall to perform the actual addition.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
    {
        ParamReader argReader = interface.getParamReader();

        int numCols = argReader.getNumCols();
        if (numCols < 2) {
            vt_report_error(0, "Must supply at least 2 arguments");
        }

        m_server = argReader.getStringRef("voltservers").str();
        m_procedure = argReader.getStringRef("procedure").str();

	VoltDBCall *callf = vt_createFuncObject<VoltDBCall>(interface.allocator);
	callf->m_server = m_server;
	callf->m_procedure = m_procedure;
	return callf;
    }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addAny();
        returnType.addInt();
    }

    virtual void getParameterType(ServerInterface &si,
                                 SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(1024, "voltservers");
        parameterTypes.addVarchar(1024, "procedure");
    }
};

RegisterFactory(VoltDBCallFactory);
