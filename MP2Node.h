/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"
#include <unordered_map>
#include <set>

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node
{
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable *ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet *emulNet;
	// Object of Log
	Log *log;
	// transID generator
	int transID;
	// operation transID
	unordered_map<string, set<int>> operationTransID;
	// transaction replies
	unordered_map<int, vector<bool>> transRepliesStatus;
	// read transactions values
	unordered_map<int, vector<string>> readReplyVal;
	// transaction message
	unordered_map<int, string> transMsg;
	// waitTime for transID
	unordered_map<int, int> transIDWaitTime;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member *getMemberNode()
	{
		return this->memberNode;
	}
	int getTransID()
	{
		int res = this->transID++;
		return res;
	}
	void multicastToReplicas(Message *msg, bool isReplica);
	string latestValueFromReadReply(vector<string> &reply);
	vector<string> extractValuesFromReply(vector<string> &reply);
	bool checkPresence(Node n, vector<Node> &list);

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	vector<Node> findNeighbors(size_t hash, vector<Node> localNodes);

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica, int transID);
	string readKey(string key, int transID);
	bool updateKeyValue(string key, string value, ReplicaType replica, int transID);
	bool deletekey(string key, int transID);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol(vector<Node> currMemberList);

	~MP2Node();
};

#endif /* MP2NODE_H_ */
