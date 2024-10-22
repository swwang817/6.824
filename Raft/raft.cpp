#include<iostream>
#include<bits/stdc++.h>
#include<sys/time.h>
#include<time.h>
#include<unistd.h>
#include<chrono>
#include<unistd.h>
#include<fcntl.h>
#include"locker.h"
#include"./buttonrpc-master/buttonrpc.hpp"
using namespace std;

#define COMMOM_PORT 1234
#define HEART_BEART_PERIOD 100000

class Operation{
public:
    string getcmd();
    string op;
    string key;
    string value;
    int clientId;
    int requestId;
};

string Operation::getcmd(){
    string cmd=op+" "+key+" "+value;
    return cmd;
}

/* 通过传入raft.start()得到的返回值，封装成类 */
class StartRet{
public:
    StartRet():m_cmdIndex(-1),m_curTerm(-1),isLeader(false){}
    int m_cmdIndex,m_curTerm;
    bool isLeader;
};

/* 同应用层交互的需要提交到应用层并apply的封装成applyMsg的日志信息 */
class ApplyMsg{
    bool CommandValid;
    string command;
    int CommandIndex;
};


/* 一个存放当前raft的ID及自己两个RPC端口号的class(为了减轻负担，一个选举，一个日志同步，分开来) */
class PeersInfo{
public:
    pair<int,int> m_port;
    int m_peerId;
};

/* 日志 */
class LogEntry{
public:
    LogEntry(string cmd="",int term=-1):m_command(cmd),m_term(term){}
    string m_command;
    int m_term;
};

/* 持久化类，LAB2中需要持久化的内容就这3个 */
class Persister{
public:
    vector<LogEntry> logs;
    int cur_term;
    int votedFor;
};

class AppendEntriesArgs{
public:
    int m_term;
    int m_leaderId;
    int m_prevLogIndex;
    int m_prevLogTerm;
    int m_leaderCommit;
    string m_sendLogs;
    friend Serializer& operator >> (Serializer& in,AppendEntriesArgs& d){
        in>>d.m_term>>d.m_leaderId>>d.m_prevLogIndex>>d.m_prevLogTerm>>d.m_prevLogTerm>>d.m_sendLogs;
        return in;
    }
    friend Serializer& operator << (Serializer& out,AppendEntriesArgs& d){
        out<<d.m_term<<d.m_leaderId<<d.m_prevLogIndex<<d.m_prevLogTerm<<d.m_prevLogTerm<<d.m_sendLogs;
        return out;
    }
};

class AppendEntriesReply{
public:
    int m_term;
    bool m_success;
    int m_conflict_term;       // 用于冲突时日志快速匹配
    int m_conflict_index;      // 用于冲突时日志快速匹配
};

class RequestVoteArgs{
public:
    int term;
    int candidateId;
    int lastLogTerm;
    int LastLogIndex;
};

class RequestVoteReply{
public:
    int term;
    bool VoteGranted;
};

class Raft{
public:
    static void* listenForVote(void* arg);                  // 用于监听voteRPC的server线程
    static void* listenForAppend(void* arg);                // 用于监听appendRPC的server线程
    static void* processEntriesLoop(void* arg);             // 持续处理日志同步的守护线程
    static void* electionLoop(void* arg);                   // 持续处理选举的守护线程
    static void* callRequestVote(void* arg);                // 发voteRPC的线程
    static void* sendAppendEntries(void* arg);              // 发appendRPC的线程
    static void* applyLogLoop(void* arg);                   // 持续向上层应用日志的守护线程

    enum RAFT_STATE {LEADER=0,CANDIDATE,FOLLOWER};              // 用枚举定义的raft三种状态
    void Make(vector<PeersInfo> peers,int id);                  // raft初始化
    int getMyduration(timeval last);                            // 传入某个特定计算到当下的持续时间
    void setBroadcastTime();                                    // 重新设定BroadcastTime,成为leader发心跳的时候需要重置
    pair<int,bool> getState();                                  // 在LAB3中会用到，提前留出来的接口判断是否leader
    RequestVoteReply requestVote(RequestVoteArgs args);         // vote的RPChandler
    AppendEntriesReply appendEntries(AppendEntriesArgs args);   // append的RPChandler
    bool checkLogUptodate(int term,int index);                  // 判断是否更新日志(两个准则),vote时会用到
    void push_backLog(LogEntry log);                            // 插入新日志
    vector<LogEntry> getCmdAndTerm(string text);                // 用的RPC不支持传容器，所以封装成string，这是个解封装恢复函数
    StartRet start(Operation op);                               // 向raft传日志的函数，只有leader响应并立即返回，应用层用到
    void printLogs(); 

    void serialize();                                           // 序列化
    bool deserialize();                                         // 反序列化
    void saveRaftState();                                       // 持久化
    void readRaftState();                                       // 读取持久化状态
    bool isKilled();                                            // check is killed
    void kill();                                                // 设定raft状态为dead

private:
    locker m_lock;
    cond m_cond;
    vector<PeersInfo> m_peers;
    Persister persister;
    int m_peerId;
    int dead;

    /* 需要持久化的data */
    int m_curTerm;
    int m_votedFor;
    vector<LogEntry> m_logs;

    vector<int> m_nextIndex;
    vector<int> m_matchIndex;
    int m_lastApplied;
    int m_commitIndex;

    int recvVotes;
    int finishedVote;
    int cur_peerId;

    RAFT_STATE m_state;
    int m_leaderId;
    struct timeval m_lastWakeTime;
    struct timeval m_lastBroadcastTime;
};

/* raft初始化 */
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

    readRaftState();

    /* 创建监听线程 */
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

/* 持续向上层应用日志的线程 */
void* Raft::applyLogLoop(void* arg)
{
    Raft* raft=(Raft*) arg;
    while(!raft->dead){
        usleep(10000);
        raft->m_lock.lock();
        for(int i=raft->m_lastApplied;i<raft->m_commitIndex;i++){
            /**
             * @brief 封装好消息发回给客户端，LAB3中会用
             *  ApplyMsg msg;
            */
        }
        raft->m_lastApplied=raft->m_commitIndex;
        raft->m_lock.unlock();
    }
}

/* 传入某个特定时间计算到当下的持续时间 */
/* 用于判断是否超时？ */
int Raft::getMyduration(timeval last)
{
    struct timeval now;
    gettimeofday(&now,NULL);

    return ((now.tv_sec-last.tv_sec)*1000000+(now.tv_usec-last.tv_usec));
}
/* 重新设定BroadcastTime,成为leader发心跳的时候需要重置 */
/* -200000us是为了让记录的m_lastBroadcastTime变早，这样在appendLoop中getMyduration(m_lastBroadcastTime)直接达到要求 */
void Raft::setBroadcastTime()
{
    gettimeofday(&m_lastBroadcastTime,NULL);
    printf("before: %ld,%ld\n",m_lastBroadcastTime.tv_sec,m_lastBroadcastTime.tv_usec);
    if(m_lastBroadcastTime.tv_usec>=200000){
        m_lastBroadcastTime.tv_usec-=200000;
    }else{
        m_lastBroadcastTime.tv_sec-=1;
        m_lastBroadcastTime.tv_usec+=(1000000-200000);
    }
}

/* 用于监听voteRPC的 */
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

/* 用于监听appendRPC */
void* Raft::listenForAppend(void* arg)
{
    Raft* raft=(Raft*)arg;
    buttonrpc server;
    server.as_server(raft->m_peers[raft->m_peerId].m_port.second);
    server.bind("appendEntries",&Raft::appendEntries,raft);

    pthread_t wait_tid;
    pthread_create(&wait_tid,NULL,processEntriesLoop,raft);
    pthread_detach(wait_tid);

    server.run();
    printf("listenForAppend exit!\n");
}

/* 持续处理选举 */
void* Raft::electionLoop(void* arg)
{
    Raft* raft=(Raft*) arg;
    bool resetFlag=false;
    while(!raft->dead){
        int timeOut=rand()*200000+200000;
        while(1){
            usleep(1000);
            raft->m_lock.lock();

            int during_time=raft->getMyduration(raft->m_lastWakeTime);
            if(raft->m_state==FOLLOWER&&during_time>timeOut){
                raft->m_state=CANDIDATE;
            }

            if(raft->m_state==CANDIDATE&&during_time>timeOut){
                printf(" %d attempt election at term %d,timeOut is %d\n",raft->m_peerId,raft->m_curTerm,timeOut);
                gettimeofday(&raft->m_lastWakeTime,NULL);
                resetFlag=true;
                raft->m_curTerm++;
                raft->m_votedFor=raft->m_peerId;
                raft->saveRaftState();

                raft->recvVotes=1;
                raft->finishedVote=1;
                raft->cur_peerId=0;
                pthread_t tid[raft->m_peers.size()-1];
                int i=0;
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
                if(raft->recvVotes>raft->m_peers.size()/2){
                    raft->m_state=LEADER;
                    for(int i=0;i<raft->m_peers.size();i++){
                        raft->m_nextIndex[i]=raft->m_logs.size()+1;
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
    RequestVoteArgs args;
    args.candidateId=raft->m_peerId;
    args.term=raft->m_curTerm;
    args.LastLogIndex=raft->m_logs.size();
    args.lastLogTerm=raft->m_logs.size()!=0?raft->m_logs.back().m_term:0;

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

    RequestVoteReply reply=client.call<RequestVoteReply>("requestVote",args).val();

    raft->m_lock.lock();
    raft->finishedVote++;
    raft->m_cond.signal();
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
    m_lock.lock();
    if(m_logs.size()==0){
        m_lock.unlock();
        return true;
    }
    if(term>m_logs.back().m_term){
        m_lock.unlock();
        return true;
    }
    if(term==m_logs.back().m_term&&index>=m_logs.size()){
        m_lock.unlock();
        return true;
    }
    m_lock.unlock();
    return false;
}

/* candidate请求vote时调用的别的raft的请求vote函数 */
RequestVoteReply Raft::requestVote(RequestVoteArgs args)
{
    RequestVoteReply reply;
    reply.VoteGranted=false;
    m_lock.lock();
    reply.term=m_curTerm;
    if(m_curTerm>args.term){
        m_lock.unlock();
        return reply;
    }
    if(m_curTerm<args.term){
        m_state=FOLLOWER;
        m_curTerm=args.term;
        m_votedFor=-1;
    }
    if(m_votedFor==-1||m_votedFor==args.candidateId){
        m_lock.unlock();
        bool ret=checkLogUptodate(args.lastLogTerm,args.LastLogIndex);
        if(!ret) return reply;

        m_lock.lock();
        m_votedFor=args.candidateId;
        reply.VoteGranted=true;
        printf("[%d] vote to [%d] at %d,duration is %d\n",m_peerId,args.candidateId,m_curTerm,getMyduration(m_lastWakeTime));
        gettimeofday(&m_lastWakeTime,NULL);
    }
    saveRaftState();
    m_lock.unlock();
    return reply;
}

/* 处理日志同步 */
void* Raft::processEntriesLoop(void* arg)
{
    Raft* raft=(Raft*)arg;
    while(!raft->dead){
        usleep(1000);
        raft->m_lock.lock();
        if(raft->m_state!=LEADER){
            raft->m_lock.unlock();
            continue;
        }

        int during_time=raft->getMyduration(raft->m_lastBroadcastTime);
        if(during_time<HEART_BEART_PERIOD){
            raft->m_lock.unlock();
            continue;
        }

        gettimeofday(&raft->m_lastBroadcastTime,NULL);
        raft->m_lock.lock();
        pthread_t tid[raft->m_peers.size()-1];
        int i=0;
        for(auto server:raft->m_peers){
            if(server.m_peerId==raft->m_peerId) continue;
            pthread_create(tid+i,NULL,sendAppendEntries,raft);
            pthread_detach(tid[i]);
            i++;
        }
    }
}

/* Leader定时发送appendRPC的函数 */
void* Raft::sendAppendEntries(void* arg)
{
    Raft* raft=(Raft*)arg;
    buttonrpc client;
    AppendEntriesArgs args;
    raft->m_lock.lock();

    if(raft->cur_peerId==raft->m_peerId){
        raft->cur_peerId++;
    }
    int clientPeerId=raft->cur_peerId;
    client.as_client("127.0.0.1",raft->m_peers[raft->cur_peerId++].m_port.second);
    if(raft->cur_peerId==raft->m_peers.size()||
        raft->cur_peerId==raft->m_peers.size()-1&&raft->m_peerId==raft->cur_peerId){
        raft->cur_peerId=0;
    }

    args.m_term=raft->m_curTerm;
    args.m_leaderId=raft->m_peerId;
    args.m_leaderCommit=raft->m_commitIndex;
    args.m_prevLogIndex=raft->m_nextIndex[clientPeerId]-1;

    for(int i=args.m_prevLogIndex;i<raft->m_logs.size();i++){
        args.m_sendLogs+=(raft->m_logs[i].m_command+','+to_string(raft->m_logs[i].m_term)+';');
    }
    if(args.m_prevLogIndex==0){
        args.m_prevLogTerm=0;
        if(raft->m_logs.size()!=0){
            args.m_prevLogTerm=raft->m_logs[0].m_term;
        }
    } else {
        args.m_prevLogTerm=raft->m_logs[args.m_prevLogIndex-1].m_term;
    }

    printf("[%d] -> [%d]'s prevLogIndex: %d, prevLofTerm: %d\n", raft->m_peerId,clientPeerId,args.m_prevLogIndex,args.m_prevLogTerm);

    raft->m_lock.unlock();
    AppendEntriesReply reply=client.call<AppendEntriesReply>("appendEntries",args).val();

    raft->m_lock.lock();
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
        raft->m_nextIndex[clientPeerId]+=raft->getCmdAndTerm(args.m_sendLogs).size();
        raft->m_matchIndex[clientPeerId]=raft->m_nextIndex[clientPeerId]-1;

        vector<int> tmpIndex=raft->m_matchIndex;
        sort(tmpIndex.begin(),tmpIndex.end());
        int realMajotiryMatchIndex=tmpIndex[tmpIndex.size()/2];
        if(realMajotiryMatchIndex>raft->m_commitIndex&&
                raft->m_logs[realMajotiryMatchIndex-1].m_term==raft->m_curTerm){
            raft->m_commitIndex=realMajotiryMatchIndex;
        }
    } else {// append失败
        if(reply.m_conflict_term!=-1){
            int leader_conflict_index=-1;
            for(int index=args.m_prevLogIndex;index>=1;index--){
                if(raft->m_logs[index-1].m_term==reply.m_conflict_term){
                    leader_conflict_index=index;
                    break;
                }
            }
            if(leader_conflict_index!=-1){
                raft->m_nextIndex[clientPeerId]=leader_conflict_index+1;
            } else {
                raft->m_nextIndex[clientPeerId]=reply.m_conflict_index;
            }
        }else{ // m_conflict_term==1代表follower的log长度小于leader记录的nextIndex-1
            raft->m_nextIndex[clientPeerId]=reply.m_conflict_index+1;
        }
    }
    raft->saveRaftState();
    raft->m_lock.unlock();
}

/* 这个函数是client收到appendLog请求执行的函数 返回appendLog是否成功等 */
AppendEntriesReply Raft::appendEntries(AppendEntriesArgs args){
    vector<LogEntry> recvLog=getCmdAndTerm(args.m_sendLogs);
    AppendEntriesReply reply;
    m_lock.lock();
    reply.m_term=m_curTerm;
    reply.m_success=false;
    reply.m_conflict_index=-1;
    reply.m_conflict_term=-1;

    // leader的term小于当前服务器的term直接返回false
    if(args.m_term<m_curTerm){
        m_lock.unlock();
        return reply;
    }

    // leader的term大于等于当前服务器的term，需要修改当前服务器的term
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
    gettimeofday(&m_lastWakeTime,NULL);

    int logSize=0;
    // 如果当前服务器没有log 就可以直接把leader传来的log写入
    if(m_logs.size()==0){
        for(const auto& log:recvLog){
            push_backLog(log);
        }
        saveRaftState();
        logSize=m_logs.size();
        if(m_commitIndex<args.m_leaderCommit){ // 将follower的commit和leader同步
            m_commitIndex=min(args.m_leaderCommit,logSize);
        }
        m_lock.unlock();
        reply.m_success=true;
        return reply;
    }
    // 如果follower的log长度小于leader记录的nextIndex-1就需要返回false并且记录conflict_index
    if(m_logs.size()<args.m_prevLogIndex){
        printf("[%d]'s logs.size:%d < [%d]'s prevLogIdx: %d\n",m_peerId,m_logs.size(),args.m_leaderId,args.m_prevLogIndex);
        reply.m_conflict_index=m_logs.size();
        m_lock.unlock();
        reply.m_success=false;
        return reply;
    }
    // 如果follower的log长度不小于leader记录的nextIndex-1 但follower记录的term和leader记录的不同
    // 就需要返回false并且记录conflict_index和conflict_term
    // 通过判断conflict_term来判断是哪一种冲突
    if(args.m_prevLogIndex>0&&m_logs[args.m_prevLogIndex-1].m_term!=args.m_prevLogTerm){
        printf("[%d]'s prevLogterm : %d != [%d]'s prevLogTerm : %d\n",m_peerId,m_logs[args.m_prevLogIndex-1].m_term,args.m_leaderId,args.m_prevLogTerm);

        reply.m_conflict_term=m_logs[args.m_prevLogIndex-1].m_term;
        for(int index=1;index<=m_logs[args.m_prevLogIndex-1].m_term;index++){
            if(m_logs[index-1].m_term==reply.m_conflict_term){
                reply.m_conflict_index=index;
                break;
            }
        }
        m_lock.unlock();
        reply.m_success=false;
        return reply;
    }
    // 如果不冲突就把follower多出来的log删除
    logSize=m_logs.size();
    for(int i=args.m_prevLogIndex;i<logSize;i++){
        m_logs.pop_back();
    }
    for(const auto& log:recvLog){
        push_backLog(log);
    }
    saveRaftState();
    logSize=m_logs.size();
    if(m_commitIndex<args.m_leaderCommit){
        m_commitIndex=min(logSize,args.m_leaderCommit);
    }

    for(auto a:m_logs) printf("%d ",a.m_term);
    printf("[%d] sync success\n",m_peerId);
    m_lock.unlock();
    reply.m_success=true;
    return reply;
}

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

void Raft::push_backLog(LogEntry log)
{
    m_logs.emplace_back(log);
}

pair<int,bool> Raft::getState()
{
    pair<int,bool> serverState;
    serverState.first=m_curTerm;
    serverState.second=(m_state==LEADER);
    return serverState;
}

void Raft::kill()
{
    dead=1;
}

StartRet Raft::start(Operation op)
{
    StartRet ret;
    m_lock.lock();
    RAFT_STATE state=m_state;
    if(state!=LEADER){
        m_lock.unlock();
        return ret;
    }
    ret.m_cmdIndex=m_logs.size();
    ret.m_curTerm=m_curTerm;
    ret.isLeader=true;

    LogEntry log;
    log.m_command=op.getcmd();
    log.m_term=m_curTerm;
    push_backLog(log);
    m_lock.unlock();

    return ret;
}

void Raft::printLogs()
{
    for(auto a:m_logs){
        printf("logs : %d\n",a.m_term);
    }
    cout<<endl;
}

/* 序列化 */
void Raft::serialize()
{
    string str;
    str+=to_string(this->persister.cur_term)+";"+to_string(this->persister.votedFor)+";";
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
}