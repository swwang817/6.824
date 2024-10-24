#include"raft.hpp"
#include<bits/stdc++.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<unistd.h>
#include<fcntl.h>
using namespace std;

#define EVERY_SERVER_PORT 3

typedef std::chrono::steady_clock myClock;
typedef std::chrono::steady_clock::time_point myTime;
#define myDuration std::chrono::duration_cast<std::chrono::microseconds>

class kvServerInfo{
public:
    PeersInfo peersInfo;
    vector<int> m_kvPort;
};

/* 用于定时的类，创建一个有名管道，若在指定时间内收到msg则处理业务逻辑，不然按照超时处理重试 */
class Select{
public:
    Select(string fifoName);
    string fifoName;
    bool isRecved;
    static void* work(void* arg);
};

Select::Select(string fifoName)
{
    this->fifoName=fifoName;
    isRecved=false;
    int ret=mkfifo(fifoName.c_str(),0664);
    pthread_t test_tid;
    pthread_create(&test_tid,NULL,work,this);
    pthread_detach(test_tid);
}

void* Select::work(void* arg)
{
    Select* select=(Select*)arg;
    char buf[100];
    int fd=open(select->fifoName.c_str(),O_RDONLY);
    read(fd,buf,sizeof(buf));
    select->isRecved=true;
    close(fd);
    unlink(select->fifoName.c_str());
}

/* 
    用于保存处理客户端RPC请求时的上下文信息，每次调用start()且为leader时会存到对应的map中
    key为start返回的日志index,独一无二
*/
class OpContext{
public:
    OpContext(Operation op);
    Operation op;
    string fifoName;            // 对应当前上下文的有名管道名称
    bool isWrongLeader;
    bool isIgnored;

    // 针对get请求
    bool isKeyExisted;
    string value;
};

OpContext::OpContext(Operation op)
{
    this->op=op;
    fifoName="fifo-"+to_string(op.clientId)+"-"+to_string(op.requestId);
    isWrongLeader=false;
    isIgnored=false;
    isKeyExisted=true;
    value="";
}

class GetArgs{
public:
    string key;
    int clientId;
    int requestId;
    friend Serializer& operator<<(Serializer& out, GetArgs args){
        out<<args.key<<args.clientId<<args.requestId;
        return out;
    }
    friend Serializer& operator>>(Deserializer& in, GetArgs& args){
        in>>args.key>>args.clientId>>args.requestId;
        return in;
    }
};

class GetReply{
public:
    string value;
    bool isWrongLeader;
    bool isKeyExist;
    friend Serializer& operator<<(Serializer& out, GetReply reply){
        out<<reply.value<<reply.isWrongLeader<<reply.isKeyExisted;
        return out;
    }
    friend Serializer& operator>>(Deserializer& in, GetReply& reply){
        in>>reply.value>>reply.isWrongLeader>>reply.isKeyExisted;
        return in;
    }
};

class PutAppendArgs{
public:
    string key;
    string value;
    string op;
    int clientId;
    int requestId;
    friend Serializer& operator<<(Serializer& out, PutAppendArgs args){
        out<<args.key<<args.value<<args.op<<args.clientId<<args.requestId;
        return out;
    }
    friend Serializer& operator>>(Deserializer& in, PutAppendArgs& args){
        in>>args.key>>args.value>>args.op>>args.clientId>>args.requestId;
        return in;
    }
};

class PutAppendReply{
public:
    bool isWrongLeader;
};

class KVServer{
public:
    static void* RPCserver(void* arg);
    static void* applyLoop(void* arg);                  //持续监听raft层提交的msg守护线程
    static void* snapShotLoop(void* arg);               //持续监听raft层日志是否超过给定大小，判断进行快照的守护线程
    void StartKvServer(vector<kvServerInfo>& kvInfo,int me,int maxRaftState);
    vector<PeersInfo> getRaftPort(vector<kvServerInfo>& kvInfo);
    GetReply get(GetArgs args);
    PutAppendReply putAppend(PutAppendArgs args);

    string test(string key){return m_database[key];}    // 测试其余不是leader的server状态机

    string getSnapShot();                               // 将kvServer的状态信息转化为snapShot
    void recoverySnapShot(string snapShot);             // 将从raft层获得的快照安装到kvServer即应用中(必然已经落后其它的server了，或者时初始化)

    /*---------------------------test--------------------------*/
    bool getRaftState();                                // 获取raft状态
    void killRaft();                                    // 测试安装快照功能时使用，让raft暂停接受日志
    void activateRaft();                                // 重新激活raft的功能
private:
    locker m_lock;
    Raft m_raft;
    int m_id;
    vector<int> m_port;
    int cur_portId;

    int m_maxraftstate;                                 // 超过这个大小就快照
    int m_lastAppliedIndex;

    unordered_map<string,string> m_database;            // 模拟数据库
    unordered_map<int,int> m_clientSeqMap;              // 只记录特定客户端已提交的最大请求ID
    unordered_map<int,OpContex*> m_requestMap;          // 记录当前PRC对应的上下文
};

void KVServer::StartKvServer(vector<kvServerInfo>& kvInfo,int me,int maxRaftState)
{
    this->m_id=me;
    m_port=kvInfo[me].m_kvPort;
    vector<PeersInfo> peers=getRaftPort(kvInfo);
    this->m_maxraftstate=maxRaftState;
    m_lastAppliedIndex=0;

    m_raft.setRecvSem(1);
    m_raft.setSendSem(0);
    m_raft.Make(peers.me);

    m_database.clear();
    m_clientSeqMap.clear();
    m_requestMap.clear();

    pthread_t listen_tid1[m_port.size()];                       // 创建多个用于监听客户端请求的PRCserver
    for(int i=0;i<m_port.size();i++){
        pthread_create(&listen_tid1+i,NULL,RPCserver,this);
        pthread_detach(listen_tid1[i]);
    }

    pthread_t listen_tid2;
    pthread_create(&listen_tid2,NULL,applyLoop,this);
    pthread_detach(listen_tid2);

    pthread_t listen_tid3;
    pthread_create(&listen_tid3,NULL,snapShotLoop,this);
    pthread_detach(listen_tid3);
}

void* KVServer::RPCserver(void* arg)
{
    KVServer* kv=(KVServer*)arg;
    buttonrpc server;
    int port=kv->cur_portId++;
    kv->m_lock.unlock();

    server.as_server(kv->m_port[port]);
    server.bind("get",&KVServer::get,kv);
    server.bind("putAppend",&KVServer::putAppend,kv);
    server.run();
}

GetReply KVServer::get(GetArgs args)
{
    GetReply reply;
    reply.isWrongLeader=false;
    reply.isKeyExisted=true;
    Operation operation;
    operation.op="get";
    operation.key=args.key;
    operation.value="random";
    operation.clientId=args.clientId;
    operation.requestId=args.requestId;

    StartRet ret=m_raft.start(operation);
    operation.term=ret.m_curTerm;
    operation.index=ret.m_curIndex;

    if(ret.isLeader==false){
        printf("client %d's get request is wrong leader %d\n",args.clientId,m_id);
        reply.isWrongLeader=true;
        return reply;
    }

    OpContext opctx(operation);             // 创建RPC时的上下文信息并暂存到map中，其key为start返回的该条请求在raft日志中的唯一索引
    m_lock.lock();
    m_requestMap[ret.m_cmdIndex]=&opctx;    // 创建监听管道数据的定时对象
    m_lock.unlock();
    Select s(opctx.fifoName);
    myTime curTime=myClock::now();
    /* 超过2000000毫米还没收到说明超时 */
    while(myDuration(myClock::now()-curTime).count()<2000000){
        if(s.isRecved){
            break;
        }
        usleep(10000);
    }

    if(s.isRecved){
        if(opctx.isWrongLeader){
            reply.isWrongLeader=true;
        }else if(!opctx.isKeyExisted){
            reply.isKeyExist=false;
        }else{
            reply.value=opctx.value;
        }
    }else{
        reply.isWrongLeader=true;
        cout<<"in get --------------timeout!!!\n";
    }
    m_lock.lock();
    m_requestMap.erase(ret.m_cmdIndex);
    m_lock.unlock();
    return reply;
}

PutAppendReply KVServer::putAppend(PutAppendArgs args)
{
    PutAppendReply reply;
    reply.isWrongLeader=false;
    Operation operation;
    operation.op=args.op;
    operation.key=args.key;
    operation.value=args.value;
    operation.clientId=args.clientId;
    operation.requestId=args.requestId;

    StartRet ret=m_raft.start(operation);
    operation.term=ret.m_curTerm;
    operation.index=ret.m_curIndex;
    if(ret.isLeader==false){
        printf("client %d's putAppend request is wrong leader %d\n",args.clientId,m_id);
        reply.isWrongLeader=true;
        return reply;
    }

    OpContext opctx(operation);
    m_lock.lock();
    m_requestMap[ret.m_cmdIndex]=&opctx;
    m_lock.unlock();

    Select s(opctx.fifoName);
    myTime curTime=myClock::now();
    while(myDuration(myClock::now()-curTime).count()<2000000){
        if(s.isRecved){
            break;
        }
        usleep(10000);
    }

    if(s.isRecved){
        if(opctx.isWrongLeader){
            reply.isWrongLeader=true;
        }
    }else{
        reply.isWrongLeader=true;
        cout<<"in putAppend --------------timeout!!!\n";
    }
    m_lock.lock();
    m_requestMap.erase(ret.m_cmdIndex);
    m_lock.unlock();
    return reply;
}

void* KVServer::applyLoop(void* arg)
{
    KVServer* kv=(KVServer*)arg;
    while(1){
        kv->m_raft.waitSendSem();
        ApplyMsg msg=kv->m_raft.getBackMsg();
    }
}