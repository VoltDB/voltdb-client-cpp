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

#define __STDC_CONSTANT_MACROS
#define __STDC_LIMIT_MACROS

#include <vector>
#include <string>
#include <iostream>
#include <cstdlib>
#include <stdio.h>
#include <time.h>
#include <algorithm>
#include <sys/time.h>
#include <boost/shared_ptr.hpp>
#include "Client.h"
#include "Table.h"
#include "TableIterator.h"
#include "Row.hpp"
#include "WireType.h"
#include "Parameter.hpp"
#include "ParameterSet.hpp"
#include "ProcedureCallback.hpp"

//#include <boost/date_time/posix_time/posix_time.hpp>

using namespace std;

int64_t minExecutionMilliseconds = 999999999;
int64_t maxExecutionMilliseconds = -1;
int64_t totExecutionMilliseconds = 0;
int64_t totExecutions = 0;
int64_t totExecutionsLatency = 0;
int64_t latencyCounter[] = {0, 0, 0, 0, 0, 0, 0, 0, 0};

int64_t voteResultCounter[] = {0, 0, 0};

bool checkLatency = false;

int64_t numSPCalls;
int64_t minAllowedOutstanding;
int64_t maxAllowedOutstanding;

int64_t millisec_time() {
    struct timeval tp;
    int res = gettimeofday(&tp, NULL);
    assert(res == 0);
    return (tp.tv_sec * 1000) + (tp.tv_usec / 1000);
}

class VoterCallback : public voltdb::ProcedureCallback
{
public:
	bool callback(voltdb::InvocationResponse response) throw (voltdb::Exception)
	{
		bool retVal = false;
		if (response.failure())
		{
			cout << "Failed to execute!!!" << endl;
			cout << response.toString();
			exit(-1);
		}
		else
		{
			totExecutions++;
			if (numSPCalls - totExecutions < minAllowedOutstanding)
				retVal = true;
			vector<voltdb::Table> vtResults = response.results();
			int32_t voteResult = (int32_t) (vtResults[0].iterator().next().getInt64(0));
			voteResultCounter[voteResult]++;

			if (checkLatency)
			{
				int64_t executionTime = response.clusterRoundTripTime();
				totExecutionsLatency++;
				totExecutionMilliseconds += executionTime;
				if (executionTime < minExecutionMilliseconds)
					minExecutionMilliseconds = executionTime;
				if (executionTime > maxExecutionMilliseconds)
					maxExecutionMilliseconds = executionTime;

				int32_t latencyBucket = (int32_t)(executionTime / 25);
				if (latencyBucket > 8)
					latencyBucket = 8;
				latencyCounter[latencyBucket]++;
			}
		}
		return retVal;
	}
};

void Tokenize(const string& str, vector<string>& tokens, const string& delimiters = " ")
{
    string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    string::size_type pos = str.find_first_of(delimiters, lastPos);

    while (string::npos != pos || string::npos != lastPos)
    {
        tokens.push_back(str.substr(lastPos, pos - lastPos));
        lastPos = str.find_first_not_of(delimiters, pos);
        pos = str.find_first_of(delimiters, lastPos);
    }
}

int main(int argc, char* argv[])
{
	if (argc != 12)
	{
		cout << "ClientVoter [number of contestants] [votes per phone number] [transactions per second] [minimum outstanding] [maximum outstanding] [client feedback interval (seconds)] [test duration (seconds)] [lag record delay (seconds)] [server list (comma separated)] username password" << endl;
		exit(1);
	}

	int32_t maxContestant = atoi(argv[1]);
	if (maxContestant < 1 || maxContestant > 12)
	{
		cout << "Number of contestants must be between 1 and 12" << endl;
		exit(1);
	}

	int64_t maxVotesPerPhoneNumber = atol(argv[2]);
	int64_t transactionsPerSecond = atol(argv[3]);
	int64_t transactionsPerMilli = transactionsPerSecond / 1000;
	minAllowedOutstanding = atol(argv[4]);
	maxAllowedOutstanding = atol(argv[5]);
	int64_t clientFeedbackIntervalSecs = atol(argv[6]);
	int64_t testDurationSecs = atol(argv[7]);
	int64_t lagLatencySeconds = atol(argv[8]);
	int64_t lagLatencyMillis = lagLatencySeconds * 1000;
	string serverList = argv[9];
	string username = argv[10];
	string password = argv[11];
	int64_t thisOutstanding = 0;
	int64_t lastOutstanding = 0;

	string cNames = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway,Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster,Kurt Walser,Ericka Dieter,Loraine Nygren,Tania Mattioli";

	cout << "Allowing " << maxVotesPerPhoneNumber << " votes per phone number" << endl;
	cout << "Allowing between " << minAllowedOutstanding << " and " << maxAllowedOutstanding << " oustanding SP calls at a time" << endl;
	cout << "Submitting " << transactionsPerSecond << " SP calls/sec" << endl;
	cout << "Feedback interval = " << clientFeedbackIntervalSecs << " second(s)" << endl;
	cout << "Running for " << testDurationSecs << " second(s)" << endl;
	cout << "Latency not recorded for " << lagLatencySeconds << " second(s)" << endl;

	int64_t phoneNumber;
	int8_t contestantNumber;

	int64_t transactionsThisSecond = 0;
	int64_t lastMillisecond = millisec_time();
	int64_t thisMillisecond = millisec_time();

    voltdb::ClientConfig config = voltdb::ClientConfig(username, password, voltdb::HASH_SHA256);
    config.m_useSSL = true;
	voltdb::Client client = voltdb::Client::create(config);
	vector<string> servers;
	Tokenize(serverList, servers, ",");
	for (int i = 0; i < (int)servers.size(); i++)
	{
		cout << "Connecting to server: '" << servers[i] << "'" << endl;
		client.createConnection(servers[i]);
	}

	vector<voltdb::Parameter> parameterTypes(2);
	parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_INTEGER);
	parameterTypes[1] = voltdb::Parameter(voltdb::WIRE_TYPE_STRING);
	voltdb::Procedure initProc("Initialize", parameterTypes);
	voltdb::ParameterSet* params = initProc.params();
	params->addInt32(maxContestant).addString(cNames);
	voltdb::InvocationResponse vtInitialize = client.invoke(initProc);
	maxContestant = (int32_t)vtInitialize.results()[0].iterator().next().getInt64(0);
	cout << "Running for " << maxContestant << " contestant(s)" << endl;

	int64_t startTime = millisec_time();
	int64_t endTime = startTime + testDurationSecs * 1000;
	int64_t currentTime = startTime;
	int64_t lastFeedbackTime = startTime;
	numSPCalls = 0;
	int64_t startRecordingLatency = startTime + lagLatencyMillis;
	parameterTypes.clear();
	parameterTypes.resize(3);

	boost::shared_ptr<VoterCallback> callback(new VoterCallback());

	while (endTime > currentTime)
	{
		numSPCalls++;
		if (numSPCalls - totExecutions > maxAllowedOutstanding)
			client.run();

		phoneNumber = rand() % 100000000000;
		contestantNumber = (int8_t) (((rand() % maxContestant) * (rand() % maxContestant)) % maxContestant + 1);
		if (numSPCalls % 100 == 0)
			contestantNumber = (int8_t)((rand() % maxContestant + 1) * 2);

		parameterTypes[0] = voltdb::Parameter(voltdb::WIRE_TYPE_BIGINT);
		parameterTypes[1] = voltdb::Parameter(voltdb::WIRE_TYPE_TINYINT);
		parameterTypes[2] = voltdb::Parameter(voltdb::WIRE_TYPE_BIGINT);
		voltdb::Procedure voteProc("Vote", parameterTypes);
		voltdb::ParameterSet* params = voteProc.params();
		params->addInt64(phoneNumber).addInt8(contestantNumber).addInt64(maxVotesPerPhoneNumber);
		client.invoke(voteProc, callback);

		transactionsThisSecond++;
		if (transactionsThisSecond >= transactionsPerMilli)
		{
			client.runOnce();
			thisMillisecond = millisec_time();
			while (thisMillisecond <= lastMillisecond)
			{
				thisMillisecond = millisec_time();
			}
			lastMillisecond = thisMillisecond;
			transactionsThisSecond = 0;
		}

		currentTime = millisec_time();

		if (!checkLatency && currentTime >= startRecordingLatency)
			checkLatency = true;

		if (currentTime >= (lastFeedbackTime + clientFeedbackIntervalSecs * 1000))
		{
			int64_t elapsedTimeMillis2 = millisec_time() - startTime;
			lastFeedbackTime = currentTime;
			int64_t runTimeMillis = endTime - startTime;

			if (totExecutionsLatency == 0)
				totExecutionsLatency = 1;

			float percentComplete = 100 * elapsedTimeMillis2 / runTimeMillis;
			if (percentComplete > 100.)
				percentComplete = 100.;

			thisOutstanding = numSPCalls - totExecutions;
			float avgLatency = (float) totExecutionMilliseconds / (float) totExecutionsLatency;

			cout << percentComplete << "% Complete | SP Calls: " << numSPCalls << " at " << ((numSPCalls * 1000) / elapsedTimeMillis2) << " SP/sec | outstanding = " << thisOutstanding << " (" << (thisOutstanding - lastOutstanding) << ") | min = " << minExecutionMilliseconds << " | max = " << maxExecutionMilliseconds << " | avg = " << avgLatency << endl;

			lastOutstanding = thisOutstanding;
		}
	}

	while (!client.drain()) {}

	int64_t elapsedTimeMillis = millisec_time() - startTime;

	cout << "\n" << endl;
	cout << "*******************************************************************" << endl;
	cout << "Voting Results" << endl;
	cout << "*******************************************************************" << endl;
	cout << " - Accepted votes = " << voteResultCounter[0] << endl;
	cout << " - Rejected votes (invalid contestant) = " << voteResultCounter[1] << endl;
	cout << " - Rejected votes (voter over limit) = " << voteResultCounter[2] << endl;
	cout << endl;

	string winnerName = "<<UNKNOWN>>";
	int64_t winnerVotes = -1;
	parameterTypes.clear();
	voltdb::Procedure resultProc("Results", parameterTypes);
	voltdb::InvocationResponse vtResults = client.invoke(resultProc);
	int32_t rowCount = (int32_t) vtResults.results()[0].rowCount();
	if (rowCount == 0)
	{
		cout << " - No results to report." << endl;
	}
	else
	{
		voltdb::TableIterator tableIter = vtResults.results()[0].iterator();
		for (int i = 0; i < rowCount; i++)
		{
			voltdb::Row row = tableIter.next();
			string resultName = row.getString(0);
			int64_t resultVotes = row.getInt64(2);
			cout << " - Contestant " << resultName << " received " << resultVotes << " vote(s)" << endl;

			if (resultVotes > winnerVotes)
			{
				winnerVotes = resultVotes;
				winnerName = resultName;
			}
		}
	}

	cout << "\n - Contestant " << winnerName << " was the winner with " << winnerVotes << " vote(s)" << endl;
	cout << "\n" << endl;
	cout << "*******************************************************************" << endl;
	cout << "System Statistics" << endl;
	cout << "*******************************************************************" << endl;
	cout << " - Ran for " << elapsedTimeMillis / 1000 << " seconds" << endl;
	cout << " - Performed " << numSPCalls << " Stored Procedure calls" << endl;
	cout << " - At " << ((numSPCalls * 1000) / elapsedTimeMillis) << " calls per second" << endl;
	cout << " - Average Latency = " << (float)(totExecutionMilliseconds / totExecutionsLatency) << " ms" << endl;

	for (int i = 0; i < 8; i++) {
		cout << " -  Latency " << (i * 25) << "ms\t- " << (i * 25 + 25) << "ms\t= " << latencyCounter[i] << endl;
	}
	cout << " -  Latency 200ms+\t\t= " << latencyCounter[8] << endl;
}
