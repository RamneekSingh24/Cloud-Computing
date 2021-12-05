/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"
#include "MP1Node.h"




/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
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
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool changed = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	
	sort(curMemList.begin(), curMemList.end());

	if(ring.size() != curMemList.size()){
		changed = true;
	}

	if(!changed && ring.size() != 0){
		for (int i = 0; i < ring.size(); i++){
			if (curMemList[i].getHashCode() != ring[i].getHashCode()){
				changed = true;
				break;
			}
		}
	}
	
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring

	ring = curMemList;
	if(changed){
		stabilizationProtocol();
	}	
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
vector<Node> MP2Node::getMembershipList() {

	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		if (this->memberNode->memberList.at(i).getheartbeat() == HeartBeat::FAILED) {
			continue;
		}
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.push_back(Node(addressOfThisMember));
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
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
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
void MP2Node::clientCreate(string key, string value) {
	vector<Node> replicas = findNodes(key);
	Transaction tr(MessageType::CREATE, this->par->getcurrtime(), key, value, false);
	transactions[tr.ID] = tr;
	for (int i = 0; i < replicas.size(); i++) {
		Message msg(tr.ID, this->memberNode->addr, MessageType::CREATE, key, value);
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
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
void MP2Node::clientRead(string key){

	vector<Node> replicas = findNodes(key);
	Transaction tr(MessageType::READ, this->par->getcurrtime(), key, "", false);
	transactions[tr.ID] = tr;
	for (int i =0; i < replicas.size(); i++) {
		Message msg(tr.ID, this->memberNode->addr, MessageType::READ, key);
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}

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
void MP2Node::clientUpdate(string key, string value){
	
	vector<Node> replicas = findNodes(key);
	Transaction tr(MessageType::UPDATE, this->par->getcurrtime(), key, value, false);
	transactions[tr.ID] = tr;
	for (int i =0; i < replicas.size(); i++) {
		Message msg(tr.ID, this->memberNode->addr, MessageType::UPDATE, key, value);
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
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
void MP2Node::clientDelete(string key){
	vector<Node> replicas = findNodes(key);
	Transaction tr(MessageType::DELETE, this->par->getcurrtime(), key, "", false);
	transactions[tr.ID] = tr;
	for (int i =0; i < replicas.size(); i++) {
		Message msg(tr.ID, this->memberNode->addr, MessageType::DELETE, key);
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */

bool MP2Node::createKeyValue(string key, string value, int transID) {
	// Insert key, value, replicaType into the hash table
	std::hash<string> hashFunc;
	size_t ret = hashFunc(this->memberNode->addr.addr);
	int hash = ret%RING_SIZE;

	bool success = ht->create(key, value);
	success &= ht->update(key, value);


	if (transID != STAB_TRANS) {
		if (success) log->logCreateSuccess(&memberNode->addr, false, transID, key, value);
		else log->logCreateFail(&memberNode->addr, false, transID, key, value);
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
string MP2Node::readKey(string key, int transID) {

	string value = ht->read(key);

	if (!value.empty()) log->logReadSuccess(&memberNode->addr, false, transID, key, value);
	else log->logReadFail(&memberNode->addr, false, transID, key);

	return value;
	
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, int transID) {

	bool success = ht->update(key, value);

	if (success) log->logUpdateSuccess(&memberNode->addr, false, transID, key, value);
	else log->logUpdateFail(&memberNode->addr, false, transID, key, value);

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
bool MP2Node::deletekey(string key, int transID) {

	bool success = ht->deleteKey(key);
	// << "DETETING:" << transID << " " << key << endl;

	if (success) log->logDeleteSuccess(&memberNode->addr, false, transID, key);
	else log->logDeleteFail(&memberNode->addr, false, transID, key);

	return success;
}

void MP2Node::sendReply(int transID, MessageType msgType, Address* toAddr, bool success, string value) {
	MessageType replyMsgType = msgType == MessageType::READ ? MessageType::READREPLY : MessageType::REPLY;
	if (replyMsgType == MessageType::READREPLY) {
		string msg = Message(transID, memberNode->addr, value).toString();
		emulNet->ENsend(&memberNode->addr, toAddr, msg);	
	}
	else{
		string msg = Message(transID, this->memberNode->addr, replyMsgType, success).toString();
		emulNet->ENsend(&memberNode->addr, toAddr, msg);
	}
}


/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		Message msg(message);

		switch (msg.type)
		{
		case MessageType::CREATE:{
			bool success = createKeyValue(msg.key, msg.value, msg.transID);
			if (msg.transID != STAB_TRANS) {
				sendReply(msg.transID, msg.type, &msg.fromAddr, success);
			}
			break;
		}
		case MessageType::UPDATE:{
			bool success = updateKeyValue(msg.key, msg.value, msg.transID);
			sendReply(msg.transID, msg.type, &msg.fromAddr, success);
			break;
		}
		case MessageType::READ:{
			string value = readKey(msg.key, msg.transID);
			sendReply(msg.transID, msg.type, &msg.fromAddr, true, value);
			break;
		}
		case MessageType::DELETE:{
			bool success = deletekey(msg.key, msg.transID);
			sendReply(msg.transID, msg.type, &msg.fromAddr, success, "");
			break;
		}
		case MessageType::REPLY:{
			auto it = transactions.find(msg.transID);
			if (it != transactions.end()) {
				it->second.replyCount++;
				if (msg.success) it->second.successCount++;
			}
			break;
		}
		case MessageType::READREPLY:{
			auto it = transactions.find(msg.transID);

			if (it != transactions.end()) {
				it->second.replyCount++;
				if (!msg.value.empty()) {
					it->second.value = msg.value;
					it->second.successCount++;
				}
			}
			break;
		}
		
		default:
			break;
		}
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */

	vector<int> loggedTrans(0);

	for (auto &it : transactions) {

		bool success = false;
		bool toLog = false;
		
		if (it.second.replyCount >= QUORM_SZ) {
			toLog = true;
			success = it.second.successCount >= QUORM_SZ;
		}
		else if (this->par->getcurrtime() - it.second.initTime > TIMEOUT_SEC) {
			toLog = true;
			success = false;
		}

		if (toLog) {
			
			logResult(it.second, success);
			loggedTrans.push_back(it.first);
		}
	}

	for (int id : loggedTrans) {
		transactions.erase(id);
	}

}


void MP2Node::logResult(Transaction &tr, bool success) {


	switch (tr.transType) {
		case MessageType::CREATE: {
			if (success) {
				log->logCreateSuccess(&memberNode->addr, true, tr.ID, tr.key, tr.value);
			} else {
				log->logCreateFail(&memberNode->addr, true, tr.ID, tr.key, tr.value);
			}
			break;
		}
			
		case MessageType::READ: {
			if (success) {
				log->logReadSuccess(&memberNode->addr, true, tr.ID, tr.key, tr.value);
			} else {
				log->logReadFail(&memberNode->addr, true, tr.ID, tr.key);
			}
			break;
		}
			
		case MessageType::UPDATE: {
			if (success) {
				log->logUpdateSuccess(&memberNode->addr, true, tr.ID, tr.key, tr.value);
			} else {
				log->logUpdateFail(&memberNode->addr, true, tr.ID, tr.key, tr.value);
			}
			break;
		}
			
		case MessageType::DELETE: {
			if (success) {
				log->logDeleteSuccess(&memberNode->addr, true, tr.ID, tr.key);
			} else {
				log->logDeleteFail(&memberNode->addr, true, tr.ID, tr.key);
			}
			break;
		}
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.push_back(ring.at(0));
			addr_vec.push_back(ring.at(1));
			addr_vec.push_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.push_back(addr);
					addr_vec.push_back(ring.at((i+1)%ring.size()));
					addr_vec.push_back(ring.at((i+2)%ring.size()));
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
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
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
void MP2Node::stabilizationProtocol() {


	for(auto entry: ht->hashTable) {

		auto neighbours = findNodes(entry.first);

		for(auto node: neighbours) {
			string key = entry.first, value = entry.second;
			Transaction tr(STAB_TRANS, this->par->getcurrtime(), key, value, true);
			transactions[tr.ID] = tr;
			string message = Message(tr.ID, memberNode->addr, MessageType::CREATE, entry.first, entry.second).toString();
			emulNet->ENsend(&memberNode->addr, node.getAddress(), message);
		}

	}

}
