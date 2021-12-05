/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"
#include <random>
#include <chrono>
#include <functional>
/**
 * Macros
 */
#define TREMOVE 15
#define TFAIL 10
#define PING_NBR_CNT 4
#define GOSSIP_NBR_CNT 5
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */

enum HeartBeat{
	FAILED = -1
};

enum MsgTypes{
    JOINREQ,
    JOINREP,
	PINGHEARTBEAT
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;


typedef struct HeartBeatEntry {
	char addr[6];
	long heartbeat_no;
}HeartBeatEntry;









/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	long localTime;
	int n_members; // Number of members in the group known to this node.
	char NULLADDR[6];


public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	void updateEntry(HeartBeatEntry *hb_entry);
	virtual ~MP1Node();
	default_random_engine rng;
	int getId(HeartBeatEntry* entry) {
		int id = *((int *) entry->addr);
		return id;
	}

	short getPort(HeartBeatEntry* entry) {
		short port = *((short *) (&entry->addr[4]));
		return port;
	}

	long get_heartbeat_no(HeartBeatEntry* entry) {
		return entry->heartbeat_no;
	}

	void init_entry(HeartBeatEntry* entry, int id, short port, int beat_no) {
		memcpy(entry->addr, &id, sizeof(int));
		memcpy(&(entry->addr[4]), &port, sizeof(short));
		entry->heartbeat_no = beat_no;
	}

};

#endif /* _MP1NODE_H_ */
