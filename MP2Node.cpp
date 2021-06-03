/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address)
{
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->transID = 0;
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node()
{
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	if (ring.size() == 0)
	{
		ring = curMemList;
	}
	else if (ring.size() != curMemList.size())
	{
		change = true;
	}
	else
	{
		for (Node n : ring)
		{
			if (!checkPresence(n, curMemList))
			{
				change = true;
				break;
			}
		}
	}

	if (change)
	{
		stabilizationProtocol(curMemList);
		ring = curMemList;
	}
	else
	{
		ring = curMemList;
	}
	sort(this->ring.begin(), this->ring.end());
}

bool MP2Node::checkPresence(Node n, vector<Node> &list)
{
	auto itr = lower_bound(list.begin(), list.end(), n) - list.begin();
	if (itr == list.size())
	{
		return false;
	}
	else if (list[itr].nodeHashCode == n.nodeHashCode)
	{
		return true;
	}
	return false;
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList()
{
	unsigned int i;
	vector<Node> curMemList;
	for (i = 0; i < this->memberNode->memberList.size(); i++)
	{
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key)
{
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value)
{
	/*
	 * Implement this
	 */
	int _transID = getTransID();
	Address _currAddr = memberNode->addr;
	MessageType _msgType = CREATE;
	ReplicaType _replicaType = PRIMARY;

	Message *newMsg = new Message(_transID, _currAddr, _msgType, key, value, _replicaType);
	operationTransID["create"].insert(_transID);
	transMsg[_transID] = newMsg->toString();
	transIDWaitTime[_transID] = par->getcurrtime();
	multicastToReplicas(newMsg, true);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key)
{
	/*
	 * Implement this
	 */
	int _transID = getTransID();
	Address _currAddr = memberNode->addr;
	MessageType _msgType = READ;

	Message *newMsg = new Message(_transID, _currAddr, _msgType, key);
	operationTransID["read"].insert(_transID);
	transMsg[_transID] = newMsg->toString();
	transIDWaitTime[_transID] = par->getcurrtime();
	multicastToReplicas(newMsg, false);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value)
{
	/*
	 * Implement this
	 */
	int _transID = getTransID();
	Address _currAddr = memberNode->addr;
	MessageType _msgType = UPDATE;
	ReplicaType _replicaType = PRIMARY;

	Message *newMsg = new Message(_transID, _currAddr, _msgType, key, value, _replicaType);
	operationTransID["update"].insert(_transID);
	transMsg[_transID] = newMsg->toString();
	transIDWaitTime[_transID] = par->getcurrtime();
	multicastToReplicas(newMsg, true);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key)
{
	/*
	 * Implement this
	 */
	int _transID = getTransID();
	Address _currAddr = memberNode->addr;
	MessageType _msgType = DELETE;

	Message *newMsg = new Message(_transID, _currAddr, _msgType, key);
	operationTransID["delete"].insert(_transID);
	transMsg[_transID] = newMsg->toString();
	transIDWaitTime[_transID] = par->getcurrtime();
	multicastToReplicas(newMsg, false);
}

/**
 * FUNCTION NAME: multicastToReplicas
 * 
 * DESCRIPTION: This function multicast the message to replicas
 */
void MP2Node::multicastToReplicas(Message *msg, bool isReplica)
{
	vector<Node> replicas = findNodes(msg->key);
	Address toAddr;
	for (Node n : replicas)
	{
		toAddr = n.nodeAddress;
		emulNet->ENsend(&(memberNode->addr), &toAddr, msg->toString());
		if (isReplica)
		{
			if (msg->replica == PRIMARY)
			{
				msg->replica = SECONDARY;
			}
			else if (msg->replica == SECONDARY)
			{
				msg->replica = TERTIARY;
			}
			else if (msg->replica == TERTIARY)
			{
				msg->replica = PRIMARY;
			}
		}
	}
	replicas.clear();
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transID)
{
	/*
	 * Implement this
	 */
	Entry *newEntry = new Entry(value, par->getcurrtime(), replica);
	bool success = ht->create(key, newEntry->convertToString());

	if (success)
	{
		log->logCreateSuccess(&(memberNode->addr), false, transID, key, value);
	}
	else
	{
		log->logCreateFail(&(memberNode->addr), false, transID, key, value);
	}
	return success;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transID)
{
	/*
	 * Implement this
	 */
	string val = "";
	val = ht->read(key);

	if (val != "")
	{
		log->logReadSuccess(&(memberNode->addr), false, transID, key, val);
	}
	else
	{
		log->logReadFail(&(memberNode->addr), false, transID, key);
	}
	return val;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transID)
{
	/*
	 * Implement this
	 */
	Entry *updatedEntry = new Entry(value, par->getcurrtime(), replica);
	bool success = ht->update(key, updatedEntry->convertToString());

	if (success)
	{
		log->logUpdateSuccess(&(memberNode->addr), false, transID, key, value);
	}
	else
	{
		log->logUpdateFail(&(memberNode->addr), false, transID, key, value);
	}
	return success;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transID)
{
	/*
	 * Implement this
	 */
	bool success = ht->deleteKey(key);

	if (success)
	{
		log->logDeleteSuccess(&memberNode->addr, false, transID, key);
	}
	else
	{
		log->logDeleteFail(&memberNode->addr, false, transID, key);
	}
	return success;
}

int findNodePosition(Node n, vector<Node> &list)
{
	int sz = list.size();
	for (int i = 0; i < sz; i++)
	{
		if (n.nodeHashCode == list[i].nodeHashCode)
			return i;
	}
	return -1;
}
/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char *data;
	int size;

	/*
	 * Declare your local variables here
	 */
	Message *msgArrived;
	MessageType type;
	int transID;
	string key;
	string val;
	ReplicaType replicaType;
	Address fromAddr;

	Address currAddr = memberNode->addr;
	bool isSuccess;
	Message *replyMsg;
	vector<Node> nodesHavingKey;

	// dequeue all messages and handle them
	while (!memberNode->mp2q.empty())
	{
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		/*
		 * Handle the message types here
		 */

		msgArrived = new Message(message);
		transID = msgArrived->transID;
		type = msgArrived->type;
		fromAddr = msgArrived->fromAddr;
		isSuccess = false;

		switch (type)
		{
		case CREATE:
			key = msgArrived->key;
			val = msgArrived->value;
			isSuccess = createKeyValue(key, val, replicaType, transID);
			nodesHavingKey = findNodes(key);
			replicaType = msgArrived->replica;
			switch (replicaType)
			{
			case PRIMARY:
				for (int i = 1; i < min(3, (int)nodesHavingKey.size()); i++)
				{
					if (findNodePosition(nodesHavingKey[i], hasMyReplicas) == -1)
					{
						hasMyReplicas.emplace_back(nodesHavingKey[i]);
					}
				}
				break;
			case SECONDARY:
				if (nodesHavingKey.size() >= 1 && findNodePosition(nodesHavingKey[0], haveReplicasOf) == -1)
				{
					haveReplicasOf.emplace_back(nodesHavingKey[0]);
				}
				break;
			case TERTIARY:
				for (int i = 0; i < min(2, (int)nodesHavingKey.size()); i++)
				{
					if (findNodePosition(nodesHavingKey[i], haveReplicasOf) == -1)
					{
						haveReplicasOf.emplace_back(nodesHavingKey[i]);
					}
				}
				break;
			default:
				cout << "Can have atmost 3 replicas\n";
				exit(1);
			}
			replyMsg = new Message(transID, currAddr, REPLY, isSuccess);
			emulNet->ENsend(&currAddr, &fromAddr, replyMsg->toString());
			break;
		case UPDATE:
			key = msgArrived->key;
			val = msgArrived->value;
			replicaType = msgArrived->replica;
			isSuccess = updateKeyValue(key, val, replicaType, transID);
			replyMsg = new Message(transID, currAddr, REPLY, isSuccess);
			emulNet->ENsend(&currAddr, &fromAddr, replyMsg->toString());
			break;
		case READ:
			key = msgArrived->key;
			val = readKey(key, transID);
			replyMsg = new Message(transID, currAddr, val);
			emulNet->ENsend(&currAddr, &fromAddr, replyMsg->toString());
			break;
		case DELETE:
			key = msgArrived->key;
			isSuccess = deletekey(key, transID);
			replyMsg = new Message(transID, currAddr, REPLY, isSuccess);
			emulNet->ENsend(&currAddr, &fromAddr, replyMsg->toString());
			break;
		case REPLY:
			isSuccess = msgArrived->success;
			transRepliesStatus[transID].push_back(isSuccess);
			break;
		case READREPLY:
			val = msgArrived->value;
			readReplyVal[transID].push_back(val);
			break;
		default:
			cout << "No such message type!\n";
			exit(1);
		}
		free(msgArrived);
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	for (auto itr : operationTransID)
	{
		if (itr.first == "read")
		{
			set<int> temp = itr.second;
			for (int tId : temp)
			{
				Message *msg = new Message(transMsg[tId]);
				vector<string> valuesFromReplies = extractValuesFromReply(readReplyVal[tId]);
				string latestValue = latestValueFromReadReply(readReplyVal[tId]);
				if (latestValue != "" && count(valuesFromReplies.begin(), valuesFromReplies.end(), latestValue) > 1)
				{
					readReplyVal.erase(tId);
					transIDWaitTime.erase(tId);
					operationTransID["read"].erase(tId);
					log->logReadSuccess(&(msg->fromAddr), true, tId, msg->key, latestValue);
				}
				else
				{
					if (par->getcurrtime() - transIDWaitTime[tId] > 5)
					{
						readReplyVal.erase(tId);
						transIDWaitTime.erase(tId);
						operationTransID["read"].erase(tId);
						log->logReadFail(&(msg->fromAddr), true, tId, msg->key);
					}
				}
			}
		}
	}
	for (auto itr : operationTransID)
	{
		if (itr.first == "read")
		{
			continue;
		}
		else
		{
			set<int> temp = itr.second;
			for (int tId : temp)
			{
				Message *msg = new Message(transMsg[tId]);
				string key = msg->key;
				if (itr.first == "create")
				{
					string val = msg->value;
					if (count(transRepliesStatus[tId].begin(), transRepliesStatus[tId].end(), true) > 1)
					{
						transRepliesStatus.erase(tId);
						transIDWaitTime.erase(tId);
						operationTransID["create"].erase(tId);
						log->logCreateSuccess(&(msg->fromAddr), true, tId, key, val);
					}
					else
					{
						if (par->getcurrtime() - transIDWaitTime[tId] > 5)
						{
							transRepliesStatus.erase(tId);
							transIDWaitTime.erase(tId);
							operationTransID["create"].erase(tId);
							log->logCreateFail(&(msg->fromAddr), true, tId, key, val);
						}
					}
				}
				else if (itr.first == "update")
				{
					string val = msg->value;
					if (count(transRepliesStatus[tId].begin(), transRepliesStatus[tId].end(), true) > 1)
					{
						transRepliesStatus.erase(tId);
						transIDWaitTime.erase(tId);
						operationTransID["update"].erase(tId);
						log->logUpdateSuccess(&(msg->fromAddr), true, tId, key, val);
					}
					else
					{
						if (par->getcurrtime() - transIDWaitTime[tId] > 5)
						{
							transRepliesStatus.erase(tId);
							transIDWaitTime.erase(tId);
							operationTransID["update"].erase(tId);
							log->logUpdateFail(&(msg->fromAddr), true, tId, key, val);
						}
					}
				}
				else if (itr.first == "delete")
				{
					if (count(transRepliesStatus[tId].begin(), transRepliesStatus[tId].end(), true) > 1)
					{
						transRepliesStatus.erase(tId);
						transIDWaitTime.erase(tId);
						operationTransID["delete"].erase(tId);
						log->logDeleteSuccess(&(msg->fromAddr), true, tId, key);
					}
					else
					{
						if (par->getcurrtime() - transIDWaitTime[tId] > 5)
						{
							transRepliesStatus.erase(tId);
							transIDWaitTime.erase(tId);
							operationTransID["delete"].erase(tId);
							log->logDeleteFail(&(msg->fromAddr), true, tId, key);
						}
					}
				}
			}
		}
	}
}

/**
 * FUNCTION NAME: latestValueFromReadReply
 * 
 * DESCRIPTION: Returns latest value from read reply if exist
 * 				otherwise ""(empty string)
 */
string MP2Node::latestValueFromReadReply(vector<string> &reply)
{
	string latest = "";
	int mxTime = -1;
	for (string s : reply)
	{
		if (s != "")
		{
			Entry *en = new Entry(s);
			if (en->timestamp >= mxTime)
			{
				mxTime = en->timestamp;
				latest = en->value;
			}
		}
	}
	return latest;
}

/**
 * FUNCTION NAME: extractValuesFromReply
 * 
 * DESCRIPTION: extracts value from replies and returns a vector consisting of values.
 */
vector<string> MP2Node::extractValuesFromReply(vector<string> &reply)
{
	vector<string> values;
	for (string s : reply)
	{
		if (s != "")
		{
			Entry *en = new Entry(s);
			values.push_back(en->value);
		}
		else
		{
			values.push_back("");
		}
	}
	return values;
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key)
{
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3)
	{
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode())
		{
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else
		{
			// go through the ring until pos <= node
			for (int i = 1; i < ring.size(); i++)
			{
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode())
				{
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
					addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: findNeighbor
 * DESCRIPTION: Find the neighbor of the given hashcode
 */
vector<Node> MP2Node::findNeighbors(size_t hash, vector<Node> localNodes)
{

	vector<Node> addr_vec;
	if (localNodes.size() >= 3)
	{
		// if pos <= min || pos > max, the leader is the min
		if (hash <= localNodes.at(0).getHashCode() || hash > localNodes.at(localNodes.size() - 1).getHashCode())
		{
			addr_vec.emplace_back(localNodes.at(0));
			addr_vec.emplace_back(localNodes.at(1));
			addr_vec.emplace_back(localNodes.at(2));
		}
		else
		{
			// go through the localNodes until pos <= node
			for (int i = 1; i < localNodes.size(); i++)
			{
				Node addr = localNodes.at(i);
				if (hash <= addr.getHashCode())
				{
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(localNodes.at((i + 1) % localNodes.size()));
					addr_vec.emplace_back(localNodes.at((i + 2) % localNodes.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop()
{
	if (memberNode->bFailed)
	{
		return false;
	}
	else
	{
		return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size)
{
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol(vector<Node> currMemberList)
{
	/*
	 * Implement this
	 */
	int transactionID = 0;
	int index = 0;
	bool isPresent = false;
	bool ishasReplica = false;
	bool ishaveReplica = false;
	bool rep_success = false;
	size_t pos;
	Address fromAddr;
	Address toAddr;
	string key;
	string value;
	Entry *entryobj;
	Message *reply_msg;
	MessageType _type;
	ReplicaType _replica;
	Node selfNode;
	Node failedNode;
	Node failedHasMyReplicaNode;
	Node newPrimaryNode;
	vector<Node>::iterator ringNodes_Iterator;
	vector<Node>::iterator currMemberNodes_Iterator;
	vector<Node>::iterator failedNodes_Iterator;
	vector<Node>::iterator hasMyReplica_Iterator;
	vector<Node>::iterator haveMyReplica_Iterator;
	map<string, string>::iterator hashtable_Iterator;

	/* Detect Failure Node */
	vector<Node> failNodes;
	vector<Node> replicaNodes_vec;
	vector<Node> localNodes;

	pos = (Node(this->getMemberNode()->addr)).getHashCode();
	fromAddr = this->getMemberNode()->addr;
	selfNode = Node(this->getMemberNode()->addr);

	for (ringNodes_Iterator = this->ring.begin(); ringNodes_Iterator != this->ring.end(); ++ringNodes_Iterator)
	{
		isPresent = false;
		if (checkPresence(*ringNodes_Iterator, currMemberList))
			isPresent = true;
		if (!isPresent)
			failNodes.emplace_back(Node((*ringNodes_Iterator).nodeAddress));
	}

	// CASE 1 :- If the failed replica node is not in my has and have group. In that case just update the ring.

	for (failedNodes_Iterator = failNodes.begin(); failedNodes_Iterator < failNodes.end(); ++failedNodes_Iterator)
	{
		if (checkPresence(*failedNodes_Iterator, this->hasMyReplicas))
		{
			ishasReplica = true;
			failedHasMyReplicaNode = *failedNodes_Iterator;
			break;
		}
	}

	for (failedNodes_Iterator = failNodes.begin(); failedNodes_Iterator < failNodes.end(); ++failedNodes_Iterator)
	{
		if (checkPresence(*failedNodes_Iterator, this->haveReplicasOf))
		{
			ishaveReplica = true;
			failedNode = *failedNodes_Iterator;
			break;
		}
	}
	localNodes = currMemberList;
	replicaNodes_vec.clear();
	if (ishasReplica)
	{
		replicaNodes_vec = findNeighbors(pos, localNodes);

		index = findNodePosition(failedHasMyReplicaNode, this->hasMyReplicas);
		this->hasMyReplicas.erase(this->hasMyReplicas.begin() + index);
		this->hasMyReplicas.emplace_back(replicaNodes_vec.at(2));

		for (auto itr : ht->hashTable)
		{
			key = itr.first;
			value = itr.second;
			entryobj = new Entry(value);
			if (entryobj->replica == PRIMARY)
			{

				// CREATE Message for New Tertiary Node
				transactionID = getTransID();
				_type = CREATE;
				_replica = TERTIARY;
				toAddr = replicaNodes_vec.at(2).nodeAddress;

				reply_msg = new Message(transactionID, fromAddr, _type, key, entryobj->value, _replica);
				emulNet->ENsend(&fromAddr, &toAddr, reply_msg->toString());
			}
		}
		replicaNodes_vec.clear();
		ishasReplica = false;
	}
	replicaNodes_vec.clear();
	if (ishaveReplica)
	{

		// Find My Neighbors
		replicaNodes_vec = findNeighbors(pos, localNodes);

		if (this->haveReplicasOf[0].nodeHashCode == failedNode.getHashCode())
		{
			for (auto itr : ht->hashTable)
			{
				key = itr.first;
				value = itr.second;
				entryobj = new Entry(value);
				if (entryobj->replica == SECONDARY)
				{
					// CREATE Message for New Tertiary Node
					transactionID = getTransID();
					_type = CREATE;
					_replica = TERTIARY;
					toAddr = replicaNodes_vec.at(2).nodeAddress;

					reply_msg = new Message(transactionID, fromAddr, _type, key, entryobj->value, _replica);
					emulNet->ENsend(&fromAddr, &toAddr, reply_msg->toString());

					transactionID = getTransID();
					_replica = PRIMARY;
					rep_success = this->updateKeyValue(key, entryobj->value, _replica, transactionID);
				}
			}
		}

		replicaNodes_vec.clear();
		if (this->haveReplicasOf.at(1).nodeHashCode == failedNode.getHashCode())
		{
			pos = failedNode.getHashCode();
			// Find New Primary Node
			replicaNodes_vec = findNeighbors(pos, this->ring);
			newPrimaryNode = replicaNodes_vec.at(1);

			index = findNodePosition(failedNode, this->haveReplicasOf);
			this->haveReplicasOf.erase(this->haveReplicasOf.begin() + index);
			this->haveReplicasOf.emplace_back(newPrimaryNode);

			replicaNodes_vec.clear();
			pos = selfNode.getHashCode();
			replicaNodes_vec = findNeighbors(pos, localNodes);
			this->hasMyReplicas.emplace_back(replicaNodes_vec[1]);

			for (auto itr : ht->hashTable)
			{
				key = itr.first;
				value = itr.second;
				entryobj = new Entry(value);
				if (entryobj->replica == TERTIARY)
				{
					transactionID = getTransID();
					_type = UPDATE;
					_replica = PRIMARY;
					toAddr = newPrimaryNode.nodeAddress;

					reply_msg = new Message(transactionID, fromAddr, _type, key, entryobj->value, _replica);
					emulNet->ENsend(&fromAddr, &toAddr, reply_msg->toString());

					// CREATE Message for New Tertiary Node
					transactionID = getTransID();
					_type = CREATE;
					_replica = TERTIARY;
					toAddr = replicaNodes_vec.at(1).nodeAddress;

					reply_msg = new Message(transactionID, fromAddr, _type, key, entryobj->value, _replica);
					emulNet->ENsend(&fromAddr, &toAddr, reply_msg->toString());
				}
			}
		}
		replicaNodes_vec.clear();
		ishaveReplica = false;
	}
}
