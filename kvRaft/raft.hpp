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

/* 新增的快照PRC需要传的参数，具体看论文section7关于日志压缩的内容 */
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
    bool commandValid;
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