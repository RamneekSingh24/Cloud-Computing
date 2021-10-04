/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#ifndef DEBUGLOG
#define DEBUGLOG
#endif
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */

    

    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine e(seed);
    this->rng = e;

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = TREMOVE;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        log->logNodeAdd(&memberNode->addr, &memberNode->addr);
        this->n_members = 1;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(HeartBeatEntry);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        msg->msgType = JOINREQ;
        
        HeartBeatEntry* hb_entry = (HeartBeatEntry *) (msg + 1);
        memcpy(hb_entry->addr, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        
        hb_entry->heartbeat_no = memberNode->heartbeat;
        

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        this->n_members = 1;
        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    this->memberNode->memberList.clear();
    this->memberNode->inGroup = false;
    this->memberNode->bFailed = true;
    this->memberNode->~Member();
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    this->localTime++;
    if(memberNode->inGroup) {
        MemberListEntry &mle = memberNode->memberList[0];
        mle.heartbeat++;
        mle.timestamp = localTime;
    }
    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    MessageHdr* hdr = (MessageHdr *) data;
    
    switch (hdr->msgType)
    {
        case JOINREQ: {
            HeartBeatEntry* entry = (HeartBeatEntry *) (hdr + 1);
            assert(memberNode->addr == getJoinAddress());
            int id = getId(entry);
            short port = getPort(entry);
            int beat_no = get_heartbeat_no(entry);
            MemberListEntry mem_entry(id, port, beat_no, this->localTime);
            memberNode->memberList.push_back(mem_entry);
            this->n_members++;

            Address sendaddr;
            memset(&sendaddr, 0, sizeof(Address));
            *(int *)(&sendaddr.addr) = id;
            *(short *)(&sendaddr.addr[4]) = port;

            log->logNodeAdd(&memberNode->addr, &sendaddr);

            // Send JOINREP
            size_t msg_size = sizeof(MessageHdr) + sizeof(int) + this->n_members * sizeof(HeartBeatEntry);
            MessageHdr *msg = (MessageHdr *) malloc(msg_size * sizeof(char));
            msg->msgType = JOINREP;
            int *n_entries = (int *) (msg + 1);
            *n_entries = this->n_members;
            HeartBeatEntry *entries = (HeartBeatEntry *) (n_entries + 1);
            for (int i = 0; i < n_members; i++) {
                MemberListEntry &mle = memberNode->memberList[i];
                HeartBeatEntry entry;
                init_entry(&entry, mle.getid(), mle.getport(), mle.getheartbeat());
                memcpy(&entries[i], &entry, sizeof(HeartBeatEntry));
            }

            emulNet->ENsend(&memberNode->addr, &sendaddr , (char *)msg, msg_size);
            free(msg);
            break;
        }
        case JOINREP: {
            assert(!(memberNode->addr == getJoinAddress()));
            int *n_enries = (int *) (hdr + 1);
            HeartBeatEntry* entries = (HeartBeatEntry *)(n_enries + 1);
            for (int i = 0; i < (*n_enries); i++) {
                updateEntry(&entries[i]);
            }
            memberNode->inGroup = true;
        }
        case PINGHEARTBEAT: {
            int *n_enries = (int *) (hdr + 1);
            HeartBeatEntry* entries = (HeartBeatEntry *)(n_enries + 1);
            for (int i = 0; i < (*n_enries); i++) {
                updateEntry(&entries[i]);
            }
        }

    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    if (memberNode->memberList.size() < 2) return;


    vector<int> to_remove_indxs(0);
    for (int i = 0; i < this->n_members; i++) {
        MemberListEntry *mle = &memberNode->memberList[i];
        if (mle->heartbeat != HeartBeat::FAILED && this->localTime - mle->gettimestamp() > TFAIL) {
            mle->setheartbeat(HeartBeat::FAILED);
            mle->settimestamp(this->localTime);
        } 
        else if (mle->heartbeat == HeartBeat::FAILED && this->localTime - mle->gettimestamp() > TREMOVE) {
            to_remove_indxs.push_back(i);
        }
    }

    for (int i = 0; i < to_remove_indxs.size(); i++) {
        int idx = to_remove_indxs[i] - i;
        MemberListEntry &mle = *(memberNode->memberList.begin() + idx);
        int id = mle.getid();
        short port = mle.getport();
        Address failedaddr;
        memset(&failedaddr, 0, sizeof(Address));
        *(int *)(&failedaddr.addr) = id;
        *(short *)(&failedaddr.addr[4]) = port;
        log->logNodeRemove(&memberNode->addr, &failedaddr);
        memberNode->memberList.erase(memberNode->memberList.begin() + idx);

        // char str[512];
        // sprintf(str,"removed %d.%d.%d.%d:%d \n",  failedaddr.addr[0],failedaddr.addr[1],failedaddr.addr[2],failedaddr.addr[3], *(short*)&failedaddr.addr[4]);

        // log->LOG(&memberNode->addr, str);


        this->n_members--;

        for (auto &me : memberNode->memberList) {
            int id = me.getid();
            short port = me.getport();
            Address failedaddr;
            memset(&failedaddr, 0, sizeof(Address));
            *(int *)(&failedaddr.addr) = id;
            *(short *)(&failedaddr.addr[4]) = port;
        }
        
    }





    // send ping to random neighbours

    std::shuffle(memberNode->memberList.begin() + 1, memberNode->memberList.end(), this->rng);
    int n_ping = min(PING_NBR_CNT, this->n_members - 1);

    size_t msg_size = sizeof(MessageHdr) + sizeof(int) + this->n_members * sizeof(HeartBeatEntry);
    MessageHdr *msg = (MessageHdr *) malloc(msg_size * sizeof(char));
    msg->msgType = PINGHEARTBEAT;
    int *n_entries = (int *) (msg + 1);
    *n_entries = this->n_members;
    HeartBeatEntry *entries = (HeartBeatEntry *) (n_entries + 1);
    for (int i = 0; i < n_members; i++) {
        MemberListEntry &mle = memberNode->memberList[i];
        HeartBeatEntry entry;
        init_entry(&entry, mle.getid(), mle.getport(), mle.getheartbeat());
        memcpy(&entries[i], &entry, sizeof(HeartBeatEntry));
    }
    
    for (int i = 1; i <= n_ping; i++) {
        if (memberNode->memberList[i].heartbeat == HeartBeat::FAILED) {
            n_ping = min(n_ping + 1, this->n_members - 1);
            continue;
        }
        int id = memberNode->memberList[i].getid();
        short port = memberNode->memberList[i].getport();
        Address sendaddr;
        memset(&sendaddr, 0, sizeof(Address));
        *(int *)(&sendaddr.addr) = id;
        *(short *)(&sendaddr.addr[4]) = port;

        emulNet->ENsend(&memberNode->addr, &sendaddr , (char *)msg, msg_size);
    }



    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */

void MP1Node::updateEntry(HeartBeatEntry *hb_entry) {
    int id = getId(hb_entry);
    short port = getPort(hb_entry);
    long hb = get_heartbeat_no(hb_entry);
    bool found = false;
    for (int i = 0; i < this->n_members; i++) {
        MemberListEntry *table_entry = &memberNode->memberList[i];
        if (table_entry->getid() == id && table_entry->getport() == port) {
            found = true;
            if (table_entry->heartbeat != HeartBeat::FAILED  && hb == HeartBeat::FAILED) {

                table_entry->setheartbeat(HeartBeat::FAILED);
                table_entry->settimestamp(localTime);
            }
            else if (table_entry->heartbeat != HeartBeat::FAILED &&  table_entry->getheartbeat() < hb) {
                table_entry->setheartbeat(hb);
                table_entry->settimestamp(localTime);
            }       
        }
    }
    if (!found && hb != HeartBeat::FAILED) {

        

        MemberListEntry mle(id, port, hb, localTime);


    
        Address failedaddr;
        memset(&failedaddr, 0, sizeof(Address));
        *(int *)(&failedaddr.addr) = id;
        *(short *)(&failedaddr.addr[4]) = port;
        char str[512];

        memberNode->memberList.push_back(mle);

        Address newaddr;
        memset(&newaddr, 0, sizeof(Address));
        *(int *)(&newaddr.addr) = id;
        *(short *)(&newaddr.addr[4]) = port;
        log->logNodeAdd(&memberNode->addr, &newaddr);
        this->n_members++;
    }
}


void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    int id = *(int*)(&memberNode->addr.addr); // ip address 32 bit
	short port = *(short*)(&memberNode->addr.addr[4]); // 16 bit port
    MemberListEntry entry(id, port, memberNode->heartbeat, localTime);
    memberNode->memberList.push_back(entry);
    memberNode->myPos = memberNode->memberList.begin();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
