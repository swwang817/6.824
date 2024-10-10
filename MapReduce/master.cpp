#include<iostream>
#include<stdio.h>
#include<stdlib.h>
#include<string>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<fcntl.h>
#include"locker.h"
#include"./buttonrpc-master/buttonrpc.hpp"
#include<bits/stdc++.h>
using namespace std;

#define MAP_TASK_TIMEOUT 3
#define REDUCE_TASK_TIMEOUT 5

class Master
{
public:
    static void* waitMapTask(void *arg);       // 回收map的定时线程
    static void* waitReduceTask(void *arg);    // 回收reduce的定时线程
    static void* waitTime(void *arg);          // 用于定时的线程
    Master(int mapNum=8,int reduceNum=8);      // 带缺省值的有参构造，也可以通过命令行参数传入
    void GetAllFile(char* file[],int index);   // 从argv[]中获取待处理的文件名
    int getMapNum(){
        return m_mapNum;
    }
    int getReduceNum(){
        return m_reduceNum;
    }
    string assignTask();                      // 分配map任务的函数，RPC
    int assignReduceTask();                    // 分配reduce任务的函数,RPC
    void setMapStat(string filename);          // 设置特定map任务完成的函数,RPC
    bool isMapDone();                          // 检验所有map任务是否完成，RPC
    void setReduceStat(int taskIndex);         // 设置特定reduce任务完成的函数,RPC
    void waitMap(string filename);
    void waitReduce(int reduceIdx);
    bool Done();                               // 判断reduce任务是否已经完成
    bool getFinaStat(){                        // 所有任务是否完成，实际上reduce完成就完成了
        return m_done;
    }
private:
    bool m_done;
    list<char *> m_list;                      // 所有map任务的工作队列
    locker m_assign_lock;                     // 保护共享数据的锁
    int fileNum;                              // 从命令行读到的文件总数
    int m_mapNum;                             // map任务总数
    int m_reduceNum;                          // reduce任务总数
    unordered_map<string,int> finishedMapTask;// 存放所有完成的map任务对应的文件名
    unordered_map<int,int> finishedReduceTask;// 存放所有完成的reduce任务对应的reduce编号
    vector<int> reduceIndex;                  // 所有reduce任务的工作队列
    int curMapIndex;                          // 当前处理第几个map任务
    int curReduceIndex;                       // 当前处理第几个reduce任务
    vector<string> runningMapWork;            // 正在处理的map任务，分配出去就加到这个队列，用于判断超时处理重发
    vector<int> runningReduceWork;            // 正在处理的reduce任务，分配出去就加到这个队列，用于判断超时处理重发
};

Master::Master(int mapNum,int reduceNum):m_done(false),m_mapNum(mapNum),m_reduceNum(reduceNum)
{
    m_list.clear();
    finishedMapTask.clear();
    finishedReduceTask.clear();
    runningMapWork.clear();
    runningReduceWork.clear();
    curMapIndex=0;
    curReduceIndex=0;
    if(m_mapNum<=0||m_reduceNum<=0){
        throw exception();
    }
    for(int i=0;i<reduceNum;i++){
        reduceIndex.emplace_back(i);
    }
}

void Master::GetAllFile(char* file[],int argc)
{
    for(int i=1;i<argc;i++){
        m_list.emplace_back(file[i]);
    }
    fileNum=argc-1;
}

// map的worker只需要拿到对应的文件名就可以进行map
string Master::assignTask()
{
    if(isMapDone()) return "empty";
    if(!m_list.empty()){
        m_assign_lock.lock();
        char* task=m_list.back(); // 从工作队列中取出一个待map的文件名
        m_list.pop_back();
        m_assign_lock.unlock(); 
        waitMap(string(task));    // 调用waitMap将取出的任务加入正在运行的map任务队列并等待计时线程
        return string(task);
    }
    return "empty";
}

/* 回收map的定时线程 */
void* Master::waitMapTask(void* arg){
    Master* map=(Master*)arg;
    void* status;
    pthread_t tid;
    char op='m';
    pthread_create(&tid,NULL,waitTime,&op);
    pthread_join(tid,&status);
    map->m_assign_lock.lock();
    // 若超时后在对应的hashmap中没有该map任务完成的记录，重新将该任务加入工作队列
    if(!map->finishedMapTask.count(map->runningMapWork[map->curMapIndex])){
        printf("filename : %s is timeout\n",map->runningMapWork[map->curMapIndex].c_str());
        const char* text=map->runningMapWork[map->curMapIndex].c_str();
        map->m_list.emplace_back(const_cast<char*>(text));
        map->curMapIndex++;
        map->m_assign_lock.unlock();
        return NULL;
    }
    printf("filename : %s is finished at idx : %d\n",map->runningMapWork[map->curMapIndex].c_str(),map->curMapIndex);
    map->curMapIndex++;
    map->m_assign_lock.unlock();
}

void Master::waitMap(string filename){
    m_assign_lock.lock();
    runningMapWork.emplace_back(string(filename));
    m_assign_lock.unlock();
    pthread_t tid;
    pthread_create(&tid,NULL,waitMapTask,this);    // 创建一个用于回收计时线程及处理超时逻辑的线程
    pthread_detach(tid);
}

/* 分map任务还是reduce任务进行不同时间计时的计时线程 */
void* Master::waitTime(void* arg)
{
    char *op=(char*) arg;
    if(*op=='m') {
        sleep(MAP_TASK_TIMEOUT);
    }
    else{
        sleep(REDUCE_TASK_TIMEOUT);
    }
}

int Master::assignReduceTask()
{
    if(Done()) return -1;
    if(!reduceIndex.empty()){
        m_assign_lock.lock();
        int reduceIdx=reduceIndex.back();
        reduceIndex.pop_back();
        m_assign_lock.unlock();
        waitReduce(reduceIdx);
        return reduceIdx;
    }
    return -1;
}

void* Master::waitReduceTask(void* arg){
    Master* reduce = (Master*) arg;
    void* status;
    pthread_t tid;
    char op='r';
    pthread_create(&tid,NULL,waitTime,&op);
    pthread_join(tid,&status);
    reduce->m_assign_lock.lock();
    // 若超时后在对应的hashmap中没有该reduce任务完成的记录，将该任务重新加入工作队列
    if(!reduce->finishedReduceTask.count(reduce->runningReduceWork[reduce->curReduceIndex])){
        for(auto a:reduce->m_list) printf(" before insert %s\n", a);
        reduce->reduceIndex.emplace_back(reduce->runningReduceWork[reduce->curReduceIndex]);
        reduce->curReduceIndex++;
        for(auto a:reduce->m_list) printf(" after insert %s\n", a);
        reduce->m_assign_lock.unlock();
        return NULL;
    }
    printf("reduce idx : %d is finished\n",reduce->runningReduceWork[reduce->curReduceIndex]);
    reduce->curReduceIndex++;
    reduce->m_assign_lock.unlock();
}

void Master::waitReduce(int reduceIdx)
{
    m_assign_lock.lock();
    runningReduceWork.emplace_back(reduceIdx);      // 将分配出去的reduce任务加入正在运行的工作队列
    m_assign_lock.unlock();
    pthread_t tid;
    pthread_create(&tid,NULL,waitReduceTask,this);  // 创建一个用于回收计时线程及处理超时逻辑的线程
    pthread_detach(tid);
}

void Master::setMapStat(string filename)
{
    m_assign_lock.lock();
    finishedMapTask[filename]=1;   // 通过worker的RPC调用修改map任务的完成状态
    m_assign_lock.unlock();
}

bool Master::isMapDone()
{
    m_assign_lock.lock();
    if(finishedMapTask.size()!=fileNum){
        m_assign_lock.unlock();
        return false;
    }
    m_assign_lock.unlock();
    return true;
}

void Master::setReduceStat(int taskIndex)
{
    m_assign_lock.lock();
    finishedReduceTask[taskIndex]=1; // 通过worker的RPC调用修改reduce任务的完成状态
    m_assign_lock.unlock();
}

bool Master::Done()
{
    m_assign_lock.lock();
    int len = finishedReduceTask.size();
    m_assign_lock.unlock();
    return len==m_reduceNum; //reduce的hashmap若是达到reduceNum，reduce任务及总任务完成
}
int main(int argc,char* argv[])
{
    if(argc<2){
        cout<<"missing parameter! The format is ./Master pg*.txt"<<endl;
        exit(-1);
    }

    buttonrpc server;
    server.as_server(5555);
    Master master(13,9);
    master.GetAllFile(argv,argc);
    server.bind("getMapNum", &Master::getMapNum, &master);
    server.bind("getReduceNum", &Master::getReduceNum, &master);
    server.bind("assignTask", &Master::assignTask, &master);
    server.bind("setMapStat", &Master::setMapStat, &master);
    server.bind("isMapDone", &Master::isMapDone, &master);
    server.bind("assignReduceTask", &Master::assignReduceTask, &master);
    server.bind("setReduceStat", &Master::setReduceStat, &master);
    server.bind("Done", &Master::Done, &master);
    server.run();
    return 0;
}