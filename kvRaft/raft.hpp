#include <iostream>
#include <bits/stdc++.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <chrono>
#include <unistd.h>
#include <fcntl.h>
#include "locker.h"
#include "./buttonrpc-master/buttonrpc.hpp"
using namespace std;

#define COMMOM_PORT 12345
#define HEART_BEART_PERIOD 1000

/* 
    新增的快照PRC需要传的参数，具体看论文section7关于日志压缩的内容
    term:当前任期
    leaderId:leader的id
    lastIncludedIndex:leader上次快照日志截断处的索引
    lastIncludedTerm:leader上次快照日志截断处的任期
    snapShot:leader存储的快照
*/
class InstallSnapShotArgs{
public:
    int term;
    int leaderId;
    int lastIncludedIndex;
    int lastIncludedTerm;
    string snapShot;

    friend Serializer& operator<<(Serializer& out, InstallSnapShotArgs args){
        out<<args.term<<args.leaderId<<args.lastIncludedIndex<<args.lastIncludedTerm<<args.snapShot;
        return out;
    }
    friend Serializer& operator>>(Deserializer& in, InstallSnapShotArgs& args){
        in>>args.term>>args.leaderId>>args.lastIncludedIndex>>args.lastIncludedTerm>>args.snapShot;
        return in;
    }
};

class InstallSnapShotReply{
public:
    int term;
};

class Operation{
public:
    string getCmd();
    string op;
    string key;
    string value;
    int clientId;
    int requestId;
    int term;
    int index;
};

string Operation::getCmd()
{
    string cmd=op+" "+key+" "+value+" "+to_string(clientId)+" "+to_string(requestId);
    return cmd;
}

class StartRet{
public:
    StartRet():m_curTerm(0),m_curIndex(0),isLeader(false){}
    int m_curTerm;
    int m_curIndex;
    bool isLeader;
};

class ApplyMsg{
public:
    bool commandValid;              // 用于标识是否是快照消息
    string command;
    int commandIndex;
    int commandTerm;
    Operation getOperation();

    int lastIncludedIndex;
    int lastIncludedTerm;
    string snapShot;
};

Operation ApplyMsg::getOperation()
{
    Operation operation;
    vector<string> str;
    string tmp;
    for(int i=0;i<command.size();i++){
        if(command[i]!=' '){
            tmp+=command[i];
        }else{
            if(tmp.size()!=0) str.emplace_back(tmp);
            tmp="";
        }
    }
    if(tmp.size()!=0) str.emplace_back(tmp);
    operation.op=str[0];
    operation.key=str[1];
    operation.value=str[2];
    operation.clientId=stoi(str[3]);
    operation.requestId=stoi(str[4]);
    operation.term=commandTerm;
    return operation;
}

/* 存储了每个raft的两个处理选举和Append的端口和raft的id */
class PeersInfo{
public:
    pair<int,int> m_port;
    int m_peerId;
    bool isInstallFlag;
};

/* 单条日志 */
class LogEntry{
public:
    LogEntry(string cmd="",int term=-1):m_command(cmd),m_term(term){}
    string m_command;
    int m_term;
};

/* 
    需要持久化的数据
    logs:日志
    snapShot:快照
    cur_term:当前任期
    voted_for:投票给谁
    lastIncludedIndex:上次快照日志截断处的索引
    lastIncludedTerm:上次快照日志截断处的任期
 
*/
class Persister{
public:
    vector<LogEntry> logs;
    string snapShot;
    int cur_term;
    int voted_for;
    int lastIncludedIndex;
    int lastIncludedTerm;
};

/* 
    AppendRPC需要的参数 
    term:当前任期
    leaderId:leader的id
    prevLogIndex:leader记录的当前接受Append的raft的prevLogIndex
    prevLogTerm:leader记录的当前接受Append的raft的prevLogTerm
    leaderCommit:leader已经提交给kvServer的最大日志索引
    sendLogs:leader发送的日志
*/
class AppendEntriseArgs{
public:
    int m_term;
    int m_leaderId;
    int m_prevLogIndex;
    int m_prevLogTerm;
    int m_leaderCommit;
    string m_sendLogs;
    friend Serializer& operator<<(Serializer& out, AppendEntriseArgs args){
        out<<args.m_term<<args.m_leaderId<<args.m_prevLogIndex<<args.m_prevLogTerm<<args.m_leaderCommit<<args.m_sendLogs;
        return out;
    }
    friend Serializer& operator>>(Serializer& in, AppendEntriseArgs& args){
        in>>args.m_term>>args.m_leaderId>>args.m_prevLogIndex>>args.m_prevLogTerm>>args.m_leaderCommit>>args.m_sendLogs;
        return in;
    }
};

/* 
    接受Append的raft的返回 
    term:当前任期
    success:是否成功
    conflict_index:冲突的索引
    conflict_term:冲突的任期
*/
class AppendEntriseReply{
public:
    int m_term;
    bool m_success;
    int m_conflict_index;
    int m_conflict_term;
};

/* 
    请求voteRPC的参数
    term:当前任期
    candidateId:候选人id
    lastLogIndex:候选人最后一条日志的索引
    lastLogTerm:候选人最后一条日志的任期
*/
class RequestVoteArgs{
public:
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
};

/*
    请求voteRPC的返回
    term:当前任期
    voteGranted:是否投票
*/
class RequestVoteReply{
public:
    int term;
    bool voteGranted;
};

class Raft{
public:
    static void* listenForVote(void* arg);                  // 用于监听voteRPC的server线程
    static void* listenForAppend(void* arg);                // 用于监听appendRPC的server线程
    static void* processEntriesLoop(void* arg);             // 持续处理日志同步的守护线程
    static void* electionLoop(void* arg);                   // 持续处理选举的守护线程
    static void* callRequestVote(void* arg);                // 发voteRPC的线程
    static void* sendAppendEntries(void* arg);              // 发appendRPC的线程                 
    static void* sendInstallSnapShot(void* arg);            // 向其他follower发送快照的函数，处理逻辑看论文
    static void* applyLogLoop(void* arg);                   // 持续向上层应用日志的守护线程
    

    enum RAFT_STATE {LEADER=0,CANDIDATE=1,FOLLOWER=2};              // 用枚举定义的raft三种状态
    void Make(vector<PeersInfo> peers,int id);                      // raft初始化
    int getMyduration(timeval last);                                // 传入某个特定计算到当下的持续时间
    void setBroadcastTime();                                        // 重新设定BroadcastTime,成为leader发心跳的时候需要重置
    pair<int,bool> getState();                                      // 判断是否leader
    RequestVoteReply requestVote(RequestVoteArgs args);             // vote的RPChandler
    AppendEntriesReply appendEntries(AppendEntriesArgs args);       // append的RPChandler
    InstallSnapShotReply installSnapShot(InstallSnapShotArgs args); // 安装快照的RPChandler
    bool checkLogUptodate(int term,int index);                      // 判断是否更新日志(两个准则),vote时会用到
    void push_backLog(LogEntry log);                                // 插入新日志
    vector<LogEntry> getCmdAndTerm(string text);                    // 用的RPC不支持传容器，所以封装成string，这是个解封装恢复函数
    StartRet start(Operation op);                                   // 向raft传日志的函数，只有leader响应并立即返回，应用层用到
    
    void printLogs();
    /* 用信号量维护raft和kvServer之间的通讯 */
    void setSendsem(int num);       // 初始化send的信号量，结合kvServer层的有名管道fifo模拟go的select及channel
    void setRecvsem(int num);       // 初始化recv的信号量，结合kvServer层的有名管道fifo模拟go的select及channel                        
    bool waitSendsem();             // 信号量函数封装，用于类复合时kvServer的类外调用
    bool waitRecvsem();             // 信号量函数封装，用于类复合时kvServer的类外调用
    bool postSendsem();             // 信号量函数封装，用于类复合时kvServer的类外调用
    bool postRecvsem();             // 信号量函数封装，用于类复合时kvServer的类外调用
    ApplyMsg getBackMsg();          // 取得一个msg，结合信号量和filo模拟go的select及channel，每次只取一个，处理完再取

    void serialize();               // 序列化 就是保存成文件
    bool deserialize();             // 反序列化 就是从文件中读取
    void saveRaftState();           // 持久化 修改persister中的数据并且调用serialize
    void readRaftState();           // 调用deserialize读取文件并修改persister中的数据 
    bool isKilled();                // check is killed
    void kill();                    // 设定raft状态为dead
    void activate();                                            

    bool ExceedLogSize(int size);                                   // 超出日志大小需要快照，kvServer层需要有个守护进程持续调用该函数判断
    void recvSnapShot(string snapShot,int lastIncludeIndex);        // 接受来自kvServer层的快照(就是当前kvServer的状态)，压缩日志并且持久化
    int idxToCompressLogPos(int indes);                             // 获得原先索引在截断日志后的索引
    bool readSnapShot();                                            // 从持久化文件中读取快照
    void saveSnapShot();                                            // 持久化快照到对应的文件中
    void installSnapShotTokvServer();                               // 落后的raft向对应的应用层安装快照
    int lastIndex();                                                // 截断日志后的lastIndex
    int lastTerm();                                                 // 截断日志后的lastTerm

private:
    locker m_lock;
    cond m_cond;
    vector<PeersInfo> m_peers;      // 用于存储所有raft的端口信息
    Persister persister;            // 用于持久化的类
    int m_peerId;
    int dead;

    /* 需要持久化的data */
    int m_curTerm;
    int m_votedFor;
    vector<LogEntry> m_logs;
    int m_lastIncludedIndex;        // 新增的持久化变量，存上次快照日志截断处的相关信息
    int m_lastIncludedTerm;         // 新增的持久化变量，存上次快照日志截断处的相关信息

    vector<int> m_nextIndex;        // leader为每一个follower维护的下一个要发送的日志索引
    vector<int> m_matchIndex;       // leader为每一个follower维护的已经复制的日志索引
    int m_lastApplied;              // 已经向上层应用的日志索引
    int m_commitIndex;              // leader维护的已经提交的日志索引，大多数服务器已经复制的日志索引

    int recvVotes;                  // 用于统计收到的投票数
    int finishedVote;               // 用于统计已经完成投票的数目
    int cur_peerId;                 // 用于记录请求投票时当前处理到那个peerId

    RAFT_STATE m_state;                     
    int m_leaderId;                         
    struct timeval m_lastWakeTime;          // 用于记录最后一次接受到leader传来消息的时间
    struct timeval m_lastBroadcastTime;     // leader最后一次广播时间    

    sem m_recvsem;                      // 结合kvServer层的有名管道fifo模拟go的select及channel
    sem m_sendsem;                      // 结合kvServer层的有名管道fifo模拟go的select及channel
    vector<ApplyMsg> m_msgs;            // 在applyLogLoop中存msg的容器，每次存一条，处理完再存一条

    unordered_set<int> isExistIndex;        // 用于在processEntriesLoop中标识append和install端口对应分配情况
};

/* 初始化raft 并且开三个线程：监听投票，向follower Append日志，向上层Apply日志*/
void Raft::Make(vector<PeersInfo> peers,int id)
{
    m_peers=peers;
    m_peerId=id;
    dead=0;

    m_state=FOLLOWER;
    m_curTerm=0;
    m_leaderId=-1;
    m_votedFor=-1;
    gettimeofday(&m_lastWakeTime,NULL);

    recvVotes=0;
    finishedVote=0;
    cur_peerId=0;

    m_lastApplied=0;
    m_commitIndex=0;
    m_nextIndex.resize(peers.size(),1);
    m_matchIndex.resize(peers.size(),0);

    m_lastIncludedIndex=0;
    m_lastIncludedTerm=0;
    isExistIndex.clear();

    readRaftState();
    installSnapShotTokvServer();

    pthread_t listen_tid1;
    pthread_create(&listen_tid1,NULL,listenForVote,this);
    pthread_detach(listen_tid1);
    pthread_t listen_tid2;
    pthread_create(&listen_tid2,NULL,listenForAppend,this);
    pthread_detach(listen_tid2);
    pthread_t listen_tid3;
    pthread_create(&listen_tid3,NULL,applyLogLoop,this);
    pthread_detach(listen_tid3);
}

/* 持续向上层应用日志的守护线程 */
void* Raft::applyLogLoop(void* arg)
{
    Raft* raft=(Raft*) arg;
    while(1){
        while(!raft->dead){
            usleep(10000);
            vector<ApplyMsg> msgs;
            raft->m_lock.lock();
            while(raft->m_lastApplied<raft->m_commitIndex){
                raft->m_lastApplied++;
                int appliedIndex=raft->idxToCompressLogPos(raft->m_lastApplied);
                ApplyMsg msg;
                msg.commandValid=true;
                msg.command=raft->m_logs[appliedIndex].m_command;
                msg.commandIndex=raft->m_lastApplied;
                msg.commandTerm=raft->m_logs[appliedIndex].m_term;
                msgs.emplace_back(msg);
            }
            raft->m_lock.unlock();
            for(int i=0;i<msgs.size();i++){
                raft->waitRecvsem();
                raft->m_msgs.emplace_back(msgs[i]);
                raft->postSendsem();
            }
        }
        usleep(10000);
    }
}

/* 传入某个特定时间计算到当下的持续时间 */
/* 用于判断是否超时？ */
int Raft::getMyduration(timeval last)
{
    struct timeval now;
    gettimeofday(&now,NULL);
    return (now.tv_sec-last.tv_sec)*1000000+(now.tv_usec-last.tv_usec);
}

/* 重新设定BroadcastTime,成为leader发心跳的时候需要重置 */
/* -200000us是为了让记录的m_lastBroadcastTime变早，这样在processEntriesLoop中getMyduration(m_lastBroadcastTime)直接达到要求 */
/* 为了让刚成为LEADER的服务器快速的向其它服务器发送心跳告知 */
void Raft::setBroadcastTime()
{
    gettimeofday(&m_lastBroadcastTime,NULL);
    if(m_lastBroadcastTime.tv_usec>=200000){
        m_lastBroadcastTime.tv_usec-=200000;
    }else{
        m_lastBroadcastTime.tv_sec-=1;
        m_lastBroadcastTime.tv_usec+=(1000000-200000);
    }
}

/* 
    用于监听voteRPC的
    绑定处理请求投票的函数requestVote到对应的端口
    同时开一个线程不断判断当前raft是否发起选举并且处理选举
*/
void* Raft::listenForVote(void* arg)
{
    Raft* raft=(Raft*) arg;
    buttonrpc server;
    server.as_server(raft->m_peers[raft->m_peerId].m_port.first);
    server.bind("requestVote",&Raft::requestVote,raft);

    pthread_t wait_tid;
    pthread_create(&wait_tid,NULL,electionLoop,raft);
    pthread_detach(wait_tid);
    
    server.run();
    printf("listenForVote exit!\n");
}


/* 
    用于监听appendRPC 
    把处理Append的函数appendEntries绑定到对应的端口  
    把处理快照的函数installSnapShot绑定到对应的端口
    同时开一个线程不断判断当前raft是否向其它follower发送日志并且处理日志同步(其实只有leader且到达心跳时间才会调用)
*/
void* Raft::listenForAppend(void* arg)
{
    Raft* raft=(Raft*)arg;
    buttonrpc server;
    server.as_server(raft->m_peers[raft->m_peerId].m_port.second);
    server.bind("appendEntries",&Raft::appendEntries,raft);
    server.bind("installSnapShot",&Raft::installSnapShot,raft);

    pthread_t heart_tid;
    pthread_create(&heart_tid,NULL,processEntriesLoop,raft);
    pthread_detach(heart_tid);

    server.run();
    printf("listenForAppend exit!\n");
}

/* 持续处理选举 */
void* Raft::electionLoop(void* arg)
{
    Raft* raft=(Raft*) arg;
    bool resetFlag=false;
    while(!raft->dead){
        int timeOut=rand()%200000+200000;
        while(1){
            usleep(1000);
            raft->m_lock.lock();

            int during_time=raft->getMyduration(raft->m_lastWakeTime);
            /* 超时便开始选举，把自己改为候选人 */
            if(raft->m_state==FOLLOWER&&during_time>timeOut){
                raft->m_state=CANDIDATE;
            }

            if(raft->m_state==CANDIDATE&&during_time>timeOut){
                printf(" %d attempt election at term %d,timeOut is %d\n",raft->m_peerId,raft->m_curTerm,timeOut);
                gettimeofday(&raft->m_lastWakeTime,NULL);
                resetFlag=true;
                /* 重新选举需要把curTerm++ */
                raft->m_curTerm++;
                raft->m_votedFor=raft->m_peerId;
                raft->saveRaftState();

                raft->recvVotes=1;
                raft->finishedVote=1;
                raft->cur_peerId=0;

                pthread_t tid[raft->m_peers.size()-1];
                int i=0;
                /* 向其它所有raft请求投票 */
                for(auto server:raft->m_peers){
                    if(server.m_peerId==raft->m_peerId) continue;
                    pthread_create(tid+i,NULL,callRequestVote,raft);
                    pthread_detach(tid[i]);
                    i++;
                }

                /* 还没拿到多数投票而且没有投票完，阻塞进程并释放互斥锁等待投票完成 */
                while(raft->recvVotes<=raft->m_peers.size()/2&&raft->finishedVote!=raft->m_peers.size()){
                    raft->m_cond.wait(raft->m_lock.getlock());
                }
                if(raft->m_state!=CANDIDATE){
                    raft->m_lock.unlock();
                    continue;
                }
                /* 获得超过半数投票 */
                if(raft->recvVotes>raft->m_peers.size()/2){
                    raft->m_state=LEADER;
                    for(int i=0;i<raft->m_peers.size();i++){
                        raft->m_nextIndex[i]=raft->lastIndex()+1;
                        raft->m_matchIndex[i]=0;
                    }
                    printf(" %d become new leader at term %d\n",raft->m_peerId,raft->m_curTerm);
                    raft->setBroadcastTime();
                }
            }
            raft->m_lock.unlock();
            if(resetFlag){
                resetFlag=false;
                break;
            }
        }
    }
}

/* 发送voteRPC */
void* Raft::callRequestVote(void* arg)
{
    Raft* raft=(Raft*) arg;
    buttonrpc client;
    raft->m_lock.lock();
    /* 设置请求voteRPC的参数 */
    RequestVoteArgs args;
    args.candidateId=raft->m_peerId;
    args.term=raft->m_curTerm;
    args.lastLogIndex=raft->lastIndex();
    args.lastLogTerm=raft->lastTerm();

    if(raft->cur_peerId==raft->m_peerId){
        raft->cur_peerId++;
    }
    int clientPeerId=raft->cur_peerId;
    client.as_client("127.0.0.1",raft->m_peers[raft->cur_peerId++].m_port.first);

    if(raft->cur_peerId==raft->m_peers.size()||
    (raft->cur_peerId==raft->m_peers.size()-1&&raft->cur_peerId==raft->m_peerId)){
        raft->cur_peerId=0;
    }
    raft->m_lock.unlock();

    /* 获得其它raft的投票结果 */
    RequestVoteReply reply=client.call<RequestVoteReply>("requestVote",args).val();

    raft->m_lock.lock();
    raft->finishedVote++;
    raft->m_cond.signal();
    /* 如果其它raft的term大于当前raft则退出选举变为follower */
    if(reply.term>raft->m_curTerm){
        raft->m_state=FOLLOWER;
        raft->m_curTerm=reply.term;
        raft->m_votedFor=-1;
        raft->readRaftState();
        raft->m_lock.unlock();
        return NULL;
    }
    if(reply.VoteGranted){
        raft->recvVotes++;
    }
    
    raft->m_lock.unlock();
}

/* 判断是否更新日志(两个准则),vote时会用到 */
/* 通过比较LastTerm和LastIndex来确定是否投票 */
bool Raft::checkLogUptodate(int term,int index)
{
    int lastTerm=this->lastTerm();
    if(term>lastTerm){
        return true;
    }
    if(term==lastTerm&&index>=this->lastIndex()){
        return true;
    }
    return false;
}

/* candidate请求vote时调用的别的raft的请求vote函数 */
RequestVoteReply Raft::requestVote(RequestVoteArgs args)
{
    RequestVoteReply reply;
    reply.VoteGranted=false;
    m_lock.lock();
    reply.term=m_curTerm;

    if(m_curTerm>=args.term){
        m_lock.unlock();
        return reply;
    }

    if(m_curTerm<args.term){
        m_state=FOLLOWER;
        m_curTerm=args.term;
        m_votedFor=-1;
    }

    if(m_votedFor==-1||m_votedFor==args.candidateId){
        bool ret=checkLogUptodate(args.lastLogTerm,args.lastLogIndex);
        if(!ret) {
            m_lock.unlock();
            return reply;
        }
        m_votedFor=args.candidateId;
        reply.VoteGranted=true;
        printf("[%d] vote to [%d] at %d,duration is %d\n",m_peerId,args.candidateId,m_curTerm,getMyduration(m_lastWakeTime));
        gettimeofday(&m_lastWakeTime,NULL);
    }
    saveRaftState();
    m_lock.unlock();
    return reply;
}

/* 处理日志同步 其实只有LEADER会操作这个函数 LEADER没有达到心跳时间也不会操作这个函数 */
void* Raft::processEntriesLoop(void* arg)
{
    Raft* raft=(Raft*)arg;
    while(!raft->dead){
        usleep(1000);
        raft->m_lock.lock();
        /* 不是LEADER直接返回 */
        if(raft->m_state!=LEADER){
            raft->m_lock.unlock();
            continue;
        }

        /* 是LEADER但没有达到心跳时间 */
        int during_time=raft->getMyduration(raft->m_lastBroadcastTime);
        if(during_time<HEART_BEART_PERIOD){
            raft->m_lock.unlock();
            continue;
        }

        /* 更新上次心跳时间 向每个FOLLOWER发送AppendRPC */
        gettimeofday(&raft->m_lastBroadcastTime,NULL);
        pthread_t tid[raft->m_peers.size()-1];
        int i=0;
        for(auto& server:raft->m_peers){
            if(server.m_peerId==raft->m_peerId) continue;
            /* 进入install分支的条件，如果接受Append的follower日志落后于leader的快照 */
            if(raft->m_nextIndex[server.m_peerId]<=raft->m_lastIncludedIndex){
                printf("%d send install rpc to %d,whose nextIdx is %d,but leader's lastIncludeIdx is %d\n",
                        raft->m_peerId,server.m_peerId,raft->m_nextIndex[server.m_peerId],raft->m_lastIncludedIndex);
                server.isInstallFlag=true;
                pthread_create(tid+i,NULL,sendInstallSnapShot,raft);
                pthread_detach(tid[i]);
            } else { // 正常的appendRPC
                printf("%d send append rpc to %d,whose nextIdx is %d\n",
                        raft->m_peerId,server.m_peerId,raft->m_nextIndex[server.m_peerId]);
                pthread_create(tid+i,NULL,sendAppendEntries,raft);
                pthread_detach(tid[i]);
            }
            i++;
        }
        raft->m_lock.unlock();
    }
}

/* Leader定时发送的appendRPC的函数 follower日志落后于leader的快照 */
void* Raft::sendInstallSnapShot(void* arg)
{
    Raft* raft=(Raft*)arg;
    buttonrpc client;
    InstallSnapShotArgs args;
    int clientPeerId;
    raft->m_lock.lock();
    for(int i=0;i<raft->m_peers.size();i++){
        if(raft->m_peers[i].m_peerId==raft->m_peerId) continue;     // 忽略自己(leader)
        if(!raft->m_peers[i].isInstallFlag) continue;               // 忽略不需要install的follower
        if(raft->isExistIndex.count(i)) continue;                   // 忽略已经处理过的follower
        clientPeerId=i;
        raft->isExistIndex.insert(i);
        break;
    }

    client.as_client("127.0.0.1",raft->m_peers[clientPeerId].m_port.second);

    /* 如果所有Follower的请求都已发送完毕，重置isInstallFlag和isExistIndex，以便下一个周期继续处理 */
    if(raft->isExistIndex.size()==raft->m_peers.size()-1){
        for(int i=0;i<raft->m_peers.size();i++){
            raft->m_peers[i].isInstallFlag=false;
        }
        raft->isExistIndex.clear();
    }

    args.lastIncludedIndex=raft->m_lastIncludedIndex;
    args.lastIncludedTerm=raft->m_lastIncludedTerm;
    args.term=raft->m_curTerm;
    args.leaderId=raft->m_peerId;
    args.snapShot=raft->persister.snapShot;

    printf("in send install snapShot is %s\n",args.snapShot.c_str());
    raft->m_lock.unlock();

    InstallSnapShotReply reply=client.call<InstallSnapShotReply>("installSnapShot",args).val();

    raft->m_lock.lock();
    if(raft->m_curTerm!=args.term){
        raft->m_lock.unlock();
        return NULL;
    }

    /* 如果LEADER发现有FOLLOWER的term比自己大就重置自己的状态，变成FOLLOWER */
    if(raft->m_curTerm<reply.term){
        raft->m_curTerm=reply.term;
        raft->m_state=FOLLOWER;
        raft->m_votedFor=-1;
        raft->saveRaftState();
        raft->m_lock.unlock();
        return NULL;
    }

    /* 更新FOLLOWER的nextIndex和matchIndex,因为follower接受快照之后会改变日志大小 */
    raft->m_nextIndex[clientPeerId]=raft->lastIndex()+1;
    raft->m_matchIndex[clientPeerId]=args.lastIncludedIndex;


    raft->m_matchIndex[raft->m_peerId]=raft->lastIndex();
    vector<int> tmpIndex=raft->m_matchIndex;
    sort(tmpIndex.begin(),tmpIndex.end());
    int realMajotiryMatchIndex=tmpIndex[tmpIndex.size()/2];
    if(realMajotiryMatchIndex>raft->m_commitIndex
        &&(realMajorityMatchIndex<=raft->m_lastIncludedIndex||raft->m_logs[raft->idxToCompressLogPos(realMajorityMatchIndex)].m_term==raft->m_curTerm)){
        raft->m_commitIndex=realMajotiryMatchIndex;
    }
    raft->m_lock.unlock();
}

/* follower执行的installSnapShot的处理函数 */
InstallSnapShotReply Raft::installSnapShot(InstallSnapShotArgs args)
{
    InstallSnapShotReply reply;
    m_lock.lock();
    reply.term=m_curTerm;

    /* 如果leader的term小于follower的term返回失败 */
    if(args.term<m_curTerm){
        m_lock.unlock();
        return reply;
    }

    /*
        leader的term大于等于当前服务器的term，需要修改当前服务器的term为leader的term
        并且当前服务器需要重置自己在这个term的投票对象，并且把自己转为follower
    */
    if(args.term>=m_curTerm){
        if(args.term>m_curTerm){
            m_votedFor=-1;
            saveRaftState();
        }
        m_curTerm=args.term;
        m_state=FOLLOWER;
    }
    /* 更新follower的lastWakeTime 防止错误的voteRPC出现 */
    gettimeofday(&m_lastWakeTime,NULL);

    printf("install rpc,agrs.last is %d, but selfLast is %d,size is %d\n",
        args.lastIncludedIndex,m_lastIncludedIndex,lastIndex());
    if(args.lastIncludedIndex<=m_lastIncludedIndex){
        m_lock.unlock();
        return reply;
    } else {  // 快照正常接受
        if(args.lastIncludedIndex<=lastIndex()){ // 如果快照的lastindex小于当前日志的lastindex，截断日志
            if(m_logs[idxToCompressLogPos(lastIndex())].m_term!=args.lastIncludedTerm){
                m_logs.clear();
            } else {
                vector<LogEntry> tmpLog(m_logs.begin()+idxToCompressLogPos(args.lastIncludedIndex),m_logs.end());
                m_logs=tmpLog;
            }
        }else{
            m_logs.clear();
        }
    }

    m_lastIncludedIndex=args.lastIncludedIndex;
    m_lastIncludedTerm=args.lastIncludedTerm;
    persister.snapShot=args.snapShot;
    printf("in raft stall rpc,snapShot is %s\n",persister.snapShot.c_str());
    saveRaftState();
    saveSnapShot();

    m_lock.unlock();
    installSnapShotTokvServer();
    return reply;
}

/* leader发送的Append中的日志是string类型的，需要用这个函数读出正确的vector<LogEntry> */
vector<LogEntry> Raft::getCmdAndTerm(string text)
{
    vector<LogEntry> logs;
    int n=text.size();
    vector<string> str;
    string tmp="";
    for(int i=0;i<n;i++){
        if(text[i]!=';'){
            tmp+=text[i];
        } else {
            if(tmp.size()!=0) str.emplace_back(tmp);
            tmp="";
        }
    }
    for(int i=0;i<str.size();i++){
        tmp="";
        int j=0;
        for(j=0;j<str[i].size();j++){
            if(str[i][j]!=','){
                tmp+=str[i][j];
            }else break;
        }
        string number(str[i].begin()+j,str[i].end());
        int num=atoi(number.c_str());
        logs.emplace_back(LogEntry(tmp,num));
    }
    return logs;
}

/* 把log添加到当前raft的logs的末尾 */
void Raft::push_backLog(LogEntry log)
{
    m_logs.emplace_back(log);
}

/* leader对每一个follower都会执行的appendRPC的处理函数 */
void* Raft::sendAppendEntries(void* arg)
{
    Raft* raft=(Raft*)arg;
    buttonrpc client;
    AppendEntriesArgs args;
    raft->m_lock.lock();
    int clientPeerId;

    for(int i=0;i<raft->m_peers.size();i++){
        if(raft->m_peers[i].m_peerId==raft->m_peerId) continue; // 忽略自身(leader)
        if(raft->m_peers[i].isInstallFlag) continue;            // isInstallFlag用来表示当前followr是否需要安装快照
        if(raft->isExistIndex.count(i)) continue;               // 忽略已发送请求的节点
        clientPeerId=i;
        raft->isExistIndex.insert(i);
        break;
    }

    /* 获得每个FOLLOWR绑定的处理AppendRPC的端口 */
    client.as_client("127.0.0.1",raft->m_peers[clientPeerId].m_port.second);
    
    /* 如果所有Follower的请求都已发送完毕，重置isInstallFlag和isExistIndex，以便下一个周期继续处理 */
    if(raft->isExistIndex.size()==raft->m_peers.size()-1){
        for(int i=0;i<raft->m_peers.size();i++){
            raft->m_peers[i].isInstallFlag=false;
        }
        raft->isExistIndex.clear();
    }

    /* 设置发送的AppendRPC的参数 */
    args.m_term=raft->m_curTerm;
    args.m_leaderId=raft->m_peerId;
    args.m_leaderCommit=raft->m_commitIndex;
    /* m_nextIndex表示的是下一个要发送的log的index+1 m_prevLogIndex表示的是当前要对比的log的index+1*/
    args.m_prevLogIndex=raft->m_nextIndex[clientPeerId]-1;  
    
    /* 将index在m_prevLogIndex-1之后的log都发送给FOLLOWER */
    for(int i=raft->idxToCompressLogPos(args.m_prevLogIndex)+1;i<raft->m_logs.size();i++){
        args.m_sendLogs+=(raft->m_logs[i].m_command+','+to_string(raft->m_logs[i].m_term)+';');
    }

    /*
    if(args.m_prevLogIndex==0){
        args.m_prevLogTerm=0;
        if(raft->m_logs.size()!=0){
            args.m_prevLogTerm=raft->m_logs[0].m_term;
        }
    } else {
        args.m_prevLogTerm=raft->m_logs[args.m_prevLogIndex-1].m_term;
    }

    printf("[%d] -> [%d]'s prevLogIndex: %d, prevLofTerm: %d\n", raft->m_peerId,clientPeerId,args.m_prevLogIndex,args.m_prevLogTerm);
    */

    if(args.m_prevLogIndex==raft->m_lastIncludedIndex){
        args.m_prevLogTerm=raft->m_lastIncludedTerm;
    } else {
        args.m_prevLogTerm=raft->m_logs[raft->idxToCompressLogPos(args.m_prevLogIndex)].m_term;
    }
    
    raft->m_lock.unlock();
    /* 返回follower是否接受append,不接受append则会给出conflictIndex和conflictTerm */
    AppendEntriesReply reply=client.call<AppendEntriesReply>("appendEntries",args).val();

    raft->m_lock.lock();

    if(raft->m_curTerm!=args.m_term){
        raft->m_lock.unlock();
        return NULL;
    }

    /* 如果LEADER发现有FOLLOWER的term比自己大就重置自己的状态，变成FOLLOWER */
    if(reply.m_term>raft->m_curTerm){
        raft->m_state=FOLLOWER;
        raft->m_curTerm=reply.m_term;
        raft->m_votedFor=-1;
        raft->saveRaftState();
        raft->m_lock.unlock();
        return NULL;
    }

    // append成功
    if(reply.m_success){
        /* 修改follower对应的nextIndex和matchIndex */
        raft->m_nextIndex[clientPeerId]=args.m_prevLogIndex+raft->getCmdAndTerm(args.m_sendLogs).size();
        raft->m_matchIndex[clientPeerId]=raft->m_nextIndex[clientPeerId]-1;
        raft->m_matchIndxe[raft->m_peerId]=raft->lastIndex();

        vector<int> tmpIndex=raft->m_matchIndex;
        sort(tmpIndex.begin(),tmpIndex.end());
        /* 判断是否超过半数已经更新日志超过m_commitIndex */
        int realMajotiryMatchIndex=tmpIndex[tmpIndex.size()/2];
        if(realMajotiryMatchIndex>raft->m_commitIndex&&
            (realMajorityMatchIndex<=raft->m_lastIncludedIndex||raft->m_logs[raft->idxToCompressLogPos(realMajorityMatchIndex)].m_term==raft->m_curTerm)){
            raft->m_commitIndex=realMajotiryMatchIndex;
        }
    } else {// append失败
        /* 
            
        */
        if(reply.m_conflict_term!=-1&&reply.m_conflict_term!=-100){
            int leader_conflict_index=-1;
            /* 找到reply.m_conflict_term最后一次出现的index */
            for(int index=args.m_prevLogIndex;index>m_lastIncludedIndex;index--){
                if(raft->m_logs[raft->idxToCompressLogPos(index)].m_term==reply.m_conflict_term){
                    leader_conflict_index=index;
                    break;
                }
            }
            /* leader_conflict_index记录了leader中最后一个m_conflict_term的位置 m_conflict_term是follower实际上的term*/
            /* 判断leader存不存在m_conflict_term */
            if(leader_conflict_index!=-1){ // 存在
                raft->m_nextIndex[clientPeerId]=leader_conflict_index+1;
            } else { // 不存在
                raft->m_nextIndex[clientPeerId]=reply.m_conflict_index;
            }
        }else{ 
            if(reply.m_conflict_term == -100){}
            else raft->m_nextIndex[clientPeerId]=reply.m_conflict_index;
        }
    }
    raft->saveRaftState();
    raft->m_lock.unlock();
}

/* 这个函数是follower收到appendLog请求执行的函数 返回appendLog是否成功等 */
AppendEntriesReply Raft::appendEntries(AppendEntriesArgs args){
    /* 
        解析LEADER传来的后续log 这里的log是LEADER通过判断记录的nextIndex得出的应该发给FOLLOWER的log 
        但这个log不一定对 还需要FOLLOWER自己判断是否和自己的log冲突
     */
    vector<LogEntry> recvLog=getCmdAndTerm(args.m_sendLogs);
    AppendEntriesReply reply;
    m_lock.lock();
    reply.m_term=m_curTerm;
    reply.m_success=false;
    reply.m_conflict_index=-1;
    reply.m_conflict_term=-1;

    /* leader的term小于当前服务器的term直接返回false leader得到这个reply会把自己变成follower*/
    if(args.m_term<m_curTerm){
        m_lock.unlock();
        return reply;
    }

    /* 
        leader的term大于等于当前服务器的term，需要修改当前服务器的term为leader的term
        并且当前服务器需要重置自己在这个term的投票对象，并且把自己转为follower
    */
    if(args.m_term>=m_curTerm){
        if(args.m_term>m_curTerm){
            m_votedFor=-1;
            saveRaftState();
        }
        m_curTerm=args.m_term;
        m_state=FOLLOWER;
    }

    printf("[%d] recv append from [%d] at self term%d,send term%d,duration is %d\n",
            m_peerId,args.m_leaderId,m_curTerm,args.m_term,getMyduration(m_lastWakeTime));
    /* 修改lastWakeTime，防止不必要的voteRPC */
    gettimeofday(&m_lastWakeTime,NULL);

    /*----------------------------test------------------------------------*/
    if(dead){
        reply.m_conflict_term=-100;
        m_lock.unlock();
        return reply;
    }
    /*----------------------------test------------------------------------*/
    /* 后续代码是FOLLOWER自己判断log是否冲突 要如何写入log */
    if(args.m_prevLogIndex<m_lastIncludedIndex){
        printf("[%d]'s m_lastIncludedIndex is %d, but args.m_prevLogIndex is %d\n",m_peerId,m_lastIncludedIndex,args.m_prevLogIndex);
        reply.m_conflict_index=1;
        m_lock.unlock();
        return reply;
    } else if(args.m_prevLogIndex==m_lastIncludedIndex){
        printf("[%d]'s m_lastIncludedIndex is %d, args.m_prevLogTerm is %d\n",m_peerId,m_lastIncludedIndex,args.m_prevLogTerm);
        /* 脑裂分区，少数派的snapShot不对，回归集群后需要更新自己的snapShot及log */
        if(args.m_prevLogTerm!=m_lastIncludedTerm){
            reply.m_conflict_index=1;
            m_lock.unlock();
            return reply;
        }
    } else {
        if(lastIndex()<args.m_prevLogIndex){
            /*
            索引要加1,很关键，避免快照安装一直循环(直到下次快照)，这里加不加1最多影响到回滚次数多一次还是少一次
            如果不加1，先dead在activate，那么log的size一直都是lastincludedindx，next = conflict = last一直循环，
            知道下次超过maxstate，kvserver发起新快照才行
            */
            reply.m_conflict_index=lastIndex()+1;
            printf(" [%d]'s logs.size : %d < [%d]'s prevLogIdx : %d, ret conflict idx is %d\n", 
                m_peerId,lastIndex(),args.m_leaderId,args.m_prevLogIndex,reply.m_conflict_index);
            m_lock.unlock();
            reply.m_success=false;
            return reply;
        }
        /* 走到这里m_prevLogIndex对应的下标必然有日志，且prevLogIndex>0 */
        /* 如果m_prevLogTerm对不上则寻找第一个m_conflict_term出现的index记为m_conflict_index */
        if(m_logs[idxToCompressLogPos(args.m_prevLogIndex)].m_term!=args.m_prevLogTerm){
            printf("[%d]'s prevLogterm:%d!=[%d]'s prevLogterm:%d\n",
            m_peerId,m_logs[idxToCompressLogPos(args.m_prevLogIndex)].m_term,args.m_leaderId,args.m_prevLogTerm);

            reply.m_conflict_term=m_logs[idxToCompressLogPos(args.m_prevLogIndex)].m_term;
            for(int index=m_lastIncludedIndex+1;index<=args.m_prevLogIndex;index++){
                if(m_logs[idxToCompressLogPos(index)].m_term==reply.m_conflict_term){
                    reply.m_conflict_index=index;
                    break;
                }
            }
            m_lock.unlock();
            reply.m_success=false;
            return reply;
        }
    }
    /* 走到这里必然PrevLogterm与对应follower的index处term相等，进行日志覆盖 */
    int logSize=lastIndex();
    for(int i=args.m_prevLogIndex;i<logSize;i++){
        m_logs.pop_back();
    }
    for(int i=0;i<recvLog.size();i++){
        push_backLog(recvLog[i]);
    }
    saveRaftState();
    if(m_commitIndex<args.m_leaderCommit){
        m_commitIndex=min(args.m_leaderCommit,lastIndex());
    }
    m_lock.unlock();
    reply.m_success=true;
    return reply;
}

/* first:CurTerm second:是否等于LEADER */
pair<int,bool> Raft::getState()
{
    pair<int,bool> serverState;
    serverState.first=m_curTerm;
    serverState.second=(m_state==LEADER);
    return serverState;
}

void Raft::activate(){
    dead=0;
    printf("raft %d activate\n",m_peerId);
}

/* 向leader输入操作 */
StartRet Raft::start(Operation op)
{
    StartRet ret;
    m_lock.lock();
    RAFT_STATE state=m_state;
    if(state!=LEADER){
        m_lock.unlock();
        return ret;
    }

    LogEntry log;
    log.m_command=op.getcmd();
    log.m_term=m_curTerm;
    push_backLog(log);

    ret.m_cmdIndex=lastIndex();
    ret.m_curTerm=m_curTerm;
    ret.isLeader=true;
    m_lock.unlock();

    return ret;
}

void Raft::printLogs()
{
    for(auto a:m_logs){
        printf("logs : %d\n", a.m_term);
    }
    cout<<endl;
}

/* 序列化 就是保存成文件 */
void Raft::serialize()
{
    string str;
    str+=to_string(this->persister.cur_term)+";"+to_string(this->persister.votedFor)+";";
    str+=to_string(this->persister.lastIncludedIndex)+","+to_string(this->persister.lastIncludedTerm)+";";
    for(const auto& log:this->persister.logs){
        str+=log.m_command+","+to_string(log.m_term)+".";
    }
    string filename="persister-"+to_string(m_peerId);
    int fd=open(filename.c_str(),O_WRONLY|O_CREAT,0664);
    if(fd==-1){
        perror("open");
        exit(-1);
    }
    int len=write(fd,str.c_str(),str.size());
    close(fd);
}

/* 反序列化 就是从文件读出来 */
bool Raft::deserialize()
{
    string filename="persister-"+to_string(m_peerId);
    if(access(filename.c_str(),F_OK)==-1) return false;
    int fd=open(filename.c_str(),O_RDONLY);
    if(fd==-1){
        perror("open");
        return false;
    }
    int length=lseek(fd,0,SEEK_END);
    lseek(fd,0,SEEK_SET);
    char buf[length];
    bzero(buf,length);
    int len=read(fd,buf,length);
    if(len!=length){
        perror("read");
        exit(-1);
    }
    close(fd);
    string content(buf);
    vector<string> persist;
    string tmp="";
    for(int i=0;i<content.size();i++){
        if(content[i]!=';'){
            tmp+=content[i];
        } else {
            if(tmp.size()!=0) persist.emplace_back(tmp);
            tmp="";
        }
    }
    persist.emplace_back(tmp);
    this->persister.cur_term=atoi(persist[0].c_str());
    this->persister.votedFor=atoi(persist[1].c_str());
    this->persister.lastIncludedIndex=atoi(persist[2].c_str());
    this->persister.lastIncludedTerm=atoi(persist[3].c_str());
    vector<string> log;
    vector<LogEntry> logs;
    tmp="";
    for(int i=0;i<persist[4].size();i++){
        if(persist[4][i]!='.'){
            tmp+=persist[4][i];
        } else {
            if(tmp.size()!=0) log.emplace_back(tmp);
            tmp="";
        }
    }
    for(int i=0;i<log.size();i++){
        tmp="";
        int j=0;
        for(j=0;j<log[i].size();j++){
            if(log[i][j]!=','){
                tmp+=log[i][j];
            } else break;
        }
        string number(log[i].begin()+j+1,log[i].end());
        int num=atoi(number.c_str());
        logs.emplace_back(LogEntry(tmp,num));
    }
    this->persister.logs=logs;
    return true;
}

/* 只有在初始化的时候调用 */
void Raft::readRaftState()
{
    bool ret=this->deserialize();
    if(!ret) return;
    this->m_curTerm=this->persister.cur_term;
    this->m_votedFor=this->persister.votedFor;

    for(const auto& log:this->persister.logs){
        push_backLog(log);
    }
    printf("[%d]'s term: %d, votefor : %d,logs.size(): %d\n",m_peerId,m_votedFor,m_logs.size());
}

/* 序列化需要持久化的数据 */
void Raft::saveRaftState()
{
    persister.cur_term=m_curTerm;
    persister.votedFor=m_votedFor;
    persister.logs=m_logs;
    persister.lastIncludedIndex=m_lastIncludedIndex;
    persister.lastIncludedTerm=m_lastIncludedTerm;
    serialize();
}

void Raft::setSendSem(int num)
{
    m_sendSem.init(num);
}
void Raft::setRecvSem(int num)
{
    m_recvSem.init(num);
}

bool Raft::waitSendSem()
{
    return m_sendSem.wait();
}
bool Raft::waitRecvSem()
{
    return m_recvSem.wait();
}
bool Raft::postSendSem()
{
    return m_sendSem.post();
}
bool Raft::postRecvSem()
{
    return m_recvSem.post();
}

ApplyMsg Raft::getBackMsg()
{
    return m_msgs.back();
}

/* 判断Log长度是否达到进行快照的长度 */
bool Raft::ExceedLogSize(int size)
{
    bool ret=false;
    m_lock.lock();
    int sum=8;
    for(int i=0;i<persister.logs.size();i++){
        sum+=persister.logs[i].m_command.size()+3;
    }
    ret=(sum>=size?true:false);
    if(ret) printf("[%d] in Exceed the log size is %d\n",m_peerId,sum);
    m_lock.unlock();
    return ret;
}

/* 快照保存了KvServer的状态，这个函数是raft来接收KvServe传来的快照的 */
void Raft::recvSnapShot(string snapShot,int lastIncludedIndex)
{
    m_lock.lock();

    /* 快照无效 */
    if(lastIncludedIndex<=m_lastIncludedIndex){
        m_lock.unlock();
        return;
    }
    /* 计算需要压缩的log大小 */
    int compressLen=lastIncludedIndex-this->m_lastIncludedIndex;
    printf("[%d] before log.size is %d,compressLen is %d,lastIncludedIndex is %d\n",
            m_peerId,m_logs.size(),compressLen,lastIncludedIndex);
    printf("[%d]:%d-%d=compressLen is %d\n",m_peerId,lastIncludedIndex,this->m_lastIncludedIndex,compressLen);
    this->m_lastIncludedTerm=m_logs[idxToCompressLogPos(lastIncludedIndex)].m_term;
    this->m_lastIncludeedIndex=lastIncludedIndex;

    /* 压缩日志，快照保存了的日志就不再需要了 */
    vector<LogEntry> tmpLog;
    for(int i=compressLen;i<m_logs.size();i++){
        tmpLog.emplace_back(m_logs[i]);
    }
    m_logs=tmpLog;
    printf("[%d] after log.size is %d\n",m_peerId,m_logs.size());

    /* 持久化快照 */
    persister.snapShot=snapShot;
    saveRaftState();
    saveSnapShot();
    m_lock.unlock();
}

/* 获得原先索引在截断日志后的索引 */
int Raft::idxToCompressLogPos(int idx)
{
    return idx-m_lastIncludedIndex-1;
}

/* 从持久化的文件中读取快照 */
bool Raft::readSnapShot()
{
    string filename="snapShot-"+to_string(m_peerId);
    if(access(filename.c_str(),F_OK)==-1) return false;
    int fd=open(filename.c_str(),O_RDONLY);
    if(fd==-1){
        perror("open");
        return false;
    }
    int length=lseek(fd,0,SEEK_END);
    lseek(fd,0,SEEK_SET);
    char buf[length];
    bzero(buf,length);
    int len=read(fd,buf,length);
    if(len!=length){
        perror("read");
        exit(-1);
    }
    close(fd);
    string snapShot(buf);
    persister.snapShot=snapShot;
    return true;
}

/* 保存快照到持久化的文件中 */
void Raft::saveSnapShot()
{
    string filename="snapShot-"+to_string(m_peerId);
    int fd=open(filename.c_str(),O_WRONLY|O_CREAT,0664);
    if(fd==-1){
        perror("open");
        exit(-1);
    }
    int len=write(fd,persister.snapShot.c_str(),persister.snapShot.size()+1);
    close(fd);
}

/* follower接收leader发来的快照后需要把快照应用到服务器上 */
void Raft::installSnapShotTokvServer()
{
    m_lock.lock();
    bool ret=readSnapShot();                            // 读取快照，follower接收leader发来的快照后会把快照保存到文件中

    if(!ret){
        m_lock.unlock();
        return;
    }

    ApplyMsg msg;
    msg.commandValid=false;                    
    msg.snapShot=persister.snapShot;
    msg.lastIncludedIndex=persister.lastIncludedIndex;
    msg.lastIncludedTerm=persister.lastIncludedTerm;

    m_lastApplied=m_lastIncludedIndex;
    m_lock.unlock();

    waitRecvSem();
    m_msgs.emplace_back(msg);
    postRecvSem();

    printf("%d call install RPC\n",m_peerId);
}

int Raft::lastIndex()
{
    return m_logs.size()+m_lastIncludedIndex;
}

int Raft::lastTerm()
{
    int lastTerm=m_lastIncludedTerm;
    if(m_logs.size()!=0){
        lastTerm=m_logs[m_logs.size()-1].m_term;
    }
    return lastTerm;
}