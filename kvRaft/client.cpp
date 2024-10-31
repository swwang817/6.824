#include"raft.hpp"
#include<bits/stdc++.h>
using namespace std;

#define EVERY_SERVER_PORT 3

int cur_portId=0;
locker port_lock;

class GetArgs{
public:
    string key;
    int clientId;
    int requestId;
    friend Serizlizer& operator<<(Serizlizer& out, GetArgs& args){
        out<<args.key<<args.clientId<<args.requestId;
        return out;
    }
    friend Serizlizer& operator>>(Deserizlizer& in, GetArgs& args){
        in>>args.key>>args.clientId>>args.requestId;
        return in;
    }
};

class GetReply{
public:
    string value;
    bool isWrongLeader;
    bool isKeyExist;
    friend Serizlizer& operator<<(Serizlizer& out, GetReply& reply){
        out<<reply.value<<reply.isWrongLeader<<reply.isKeyExist;
        return out;
    }
    friend Serizlizer& operator>>(Deserizlizer& in, GetReply& reply){
        in>>reply.value>>reply.isWrongLeader>>reply.isKeyExist;
        return in;
    }
};

class PutAppendArgs{
public:
    string key;
    string value;
    string op;                              // 区分put和append
    int clientId;
    int requestId;
    friend Serizlizer& operator<<(Serizlizer& out, PutAppendArgs& args){
        out<<args.key<<args.value<<args.op<<args.clientId<<args.requestId;
        return out;
    }
    friend Serizlizer& operator>>(Deserizlizer& in, PutAppendArgs& args){
        in>>args.key>>args.value>>args.op>>args.clientId>>args.requestId;
        return in;
    }
};

class PutAppendReply{
public:
    bool isWrongLeader;
};

class Clerk{
public:
    Clerk(vector<vector<int>>& servers);                    
    string get(string key);                                     // 对于kvServer的get请求
    void put(string key, string value);                         // 对于kvServer的put请求
    void append(string key, string value);                      // 对于kvServer的append请求
    void putAppend(string key, string value,string op);         // put和append的统一处理函数
    int getCurRequestId();                                     
    int getChangeLeader();
    int getCurLeader();
private:
    locker m_requestId_lock;
    vector<vector<int>> servers;
    int leaderId;                                               // 暂存的leaderID,不用每次都轮询一次
    int clientId;                                               // 独一无二的客户端ID
    int requestId;                                              // 指挥递增的该客户端的请求ID，保证按序执行
};

Clerk::Clerk(vector<vector<int>>& servers)
{
    this->servers=servers;
    this->leaderId=rand()%servers.size();
    this->clientId=rand()%10000+1;
    printf("Clerk clientId:%d\n",this->clientId);
    this->requestId=0;
}

string Clerk::get(string key)
{
    GetArgs args;
    args.key=key;
    args.clientId=clientId;
    args.requestId=getCurRequestId();
    int cur_leader=getCurLeader();
    port_lock.lock();
    /* 取得某个kvServer的一个PRC监听端口的索引，一个Server有多个PRC处理客户端请求的端口，取完递增 */
    int curPort=(cur_portId++)%EVERY_SERVER_PORT;
    port_lock.unlock();

    while(1){
        buttonrpe client;
        client.as_client("127.0.0.1",servers[cur_leader][curPort]);
        GetReply reply=client.call<GetReply>("get",args).val();
        if(reply.isWrongLeader){
            cur_leader=getChangeLeader();
            usleep(1000);
        } else {
            if(reply.isKeyExist){
                return reply.value;
            } else {
                return "";
            }
        }
    }
}

/* 
    取得当前clerk的请求号，取出来就递增 
    封装成原子操作，避免每次加解锁，代复用
*/
int Clerk::gerCurRequestId()                
{       
    m_requestId_lock.lock();
    int cur_requestId=requestId++;
    m_requestId_lock.unlock();
    return cur_requestId;
}

/* 取得当前暂存的kvServerLeaderID */
int Clerk::gerCurLeader()
{
    m_requestId_lock.lock();
    int cur_leader=leaderId;
    m_requestId_lock.unlock();
    return cur_leader;
}

/* leader不对更换leader */
int Clerk::getChangeLeader()
{
    m_requestId_lock.lock();
    leaderId=(leaderId+1)%servers.size();
    int new_leader=leaderId;
    m_requestId_lock.unlock();
    return new_leader;
}

void Clerk::put(string key, string value)
{
    putAppend(key,value,"put");
}

void Clerk::append(string key, string value)
{
    putAppend(key,value,"append");
}

void Clerk::putAppend(string key,string value,string op)
{
    PutAppendArgs args;
    args.key=key;
    args.value=value;
    args.op=op;
    args.clientId=clientId;
    args.requestId=getCurRequestId();
    int cur_leader=getCurLeader();

    port_lock.lock();
    int curPort=(cur_portId++)%EVERY_SERVER_PORT;
    port_lock.unlock();

    while(1){
        buttonrpe client;
        client.as_client("127.0.0.1",servers[cur_leader][curPort]);
        PutAppendReply reply=client.call<PutAppendReply>("putAppend",args).val();
        if(!reply.isWrongLeader){
            return;
        }
        printf("Clerk%d's leader %d is wrong\n",cliendId,cur_leader);
        cur_leader=getChangeLeader();
        usleep(1000);
    }
}

vector<vector<int>> getServerPort(int num){
    vector<vector<int>> kvServerPort(num);
    for(int i=0;i<num;i++){
        for(int j=0;j<EVERY_SERVER_PORT;j++){
            kvServerPort[i].emplace_back(COMMOM_PORT+i+(j+2)*num);
        }
    }
    return kvServerPort;
}

int main()
{
    srand((unsigned)time(NULL));
    vector<vector<int>> port=getServerPort(3);
    Clerk clerk(port);
    Clerk clerk2(port);
    Clerk clerk3(port);
    Clerk clerk4(port);
    Clerk clerk5(port);

    /*------------------------test-----------------------------*/
    while(1){
        clerk.put("abc", "123");
        cout << clerk.get("abc") << endl;
        clerk2.put("abc", "456");
        clerk3.append("abc", "789");
        cout << clerk.get("abc") << endl;
        clerk4.put("bcd", "111");
        cout << clerk.get("bcd") << endl;
        clerk5.append("bcd", "222");
        cout << clerk3.get("bcd") << endl;
        cout << clerk3.get("abcd") << endl;
        clerk5.append("bcd", "222");
        clerk4.append("bcd", "222");
        cout << clerk2.get("bcd") << endl;
        clerk3.append("bcd", "222");
        clerk2.append("bcd", "232");
        cout << clerk4.get("bcd") << endl;
        clerk.append("bcd", "222");
        clerk4.put("bcd", "111");
        cout << clerk3.get("bcd") << endl;
        usleep(10000);
    }
    /*------------------------test-----------------------------*/
}