#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <signal.h>
#include <sys/time.h>
#include "locker.h"
#include "./buttonrpc-master/buttonrpc.hpp"
#include <bits/stdc++.h>
#include <pthread.h>
#include <dlfcn.h>

using namespace std;

#define LIB_CACULATE_PATH "./libmrFunc.so" // 用于加载的动态库的路径
#define MAX_REDUCE_NUM 15

int disabledMapId=0;                       // 用于人为让特定map任务超时的Id
int disabledReduceId=0;                    // 用于人为让特定reduce任务超时的Id

/* 定义master分配给自己的map和reduce任务数 */
int map_task_num,reduce_task_num;

class KeyValue{
public:
    string key;
    string value;
};

/* 定义函数指针用于动态加载动态库里的map和reduce函数 */
typedef vector<KeyValue> (*MapFunc)(KeyValue kv);
typedef vector<string> (*ReduceFunc)(vector<KeyValue> kvs);
MapFunc mapF;
ReduceFunc reduceF;

/* 给每个map线程分配的任务ID，用于写中间文件时的命名 */
int MapId=0;
pthread_mutex_t map_mutex;
pthread_cond_t cond;
int fileId=0;

/* 对每个字符串求hash找到其对应要分配到哪一个reduece线程 */
int ihash(string str)
{
    int sum=0;
    for(int i=0;i<str.size();i++){
        sum+=(str[i]-'0');
    }
    return sum%reduce_task_num;
}

/* 删除所有写入中间值的临时文件 */
void removeFiles(){
    string path;
    for(int i=0;i<map_task_num;i++){
        for(int j=0;j<reduce_task_num;j++){
            path="mr-"+to_string(i)+"-"+to_string(j);
            int ret=access(path.c_str(),F_OK);
            if(ret==0) remove(path.c_str());
        }
    }
}

/* 取得key：filename value:content的kv对作为map任务的输入 */
KeyValue getContent(char* file){
    int fd=open(file,O_RDONLY);
    int length=lseek(fd,0,SEEK_END);
    lseek(fd,0,SEEK_SET);
    char buf[length];
    bzero(buf,length);
    int len=read(fd,buf,length);
    if(len!=length){
        perror("read");
        exit(-1);
    }
    KeyValue kv;
    kv.key=string(file);
    kv.value=string(buf);
    close(fd);
    return kv;
}

/* 将map任务产生的中间值写入临时文件 */
void writeKV(int fd,const KeyValue& kv)
{
    string temp=kv.key+",1 ";
    int len=write(fd,temp.c_str(),temp.size());
    if(len==-1){
        perror("write");
        exit(-1);
    }
    close(fd);
}

/* 创建每个map任务对应的不同reduce号的中间文件并调用writeKV写入磁盘 */
void writeInDisk(const vector<KeyValue>& kvs,int mapTaskIdx)
{
    for(const auto&v:kvs){
        int reduce_idx=ihash(v.key);
        string path;
        path="mr-"+to_string(mapTaskIdx)+"-"+to_string(reduce_idx);
        int ret=access(path.c_str(),F_OK);
        if(ret==-1){
            int fd=open(path.c_str(),O_WRONLY|O_CREAT|O_APPEND, 0664);
            writeKV(fd,v);
        } else if(ret==0){
            int fd=open(path.c_str(),O_WRONLY|O_APPEND);
            writeKV(fd,v);
        }
    }
}

/* 以char类型的op为分割符拆分字符串 */
vector<string> split(string text,char op){
    int n=text.size();
    vector<string> str;
    string tmp="";
    for(int i=0;i<n;i++){
        if(text[i]!=op){
            tmp+=text[i];
        }else{
            if(tmp.size()!=0) str.emplace_back(tmp);
            tmp="";
        }
    }
    return str;
}

/* 以逗号为分割拆分字符串 */
string split(string text){
    string tmp = "";
    for(int i = 0; i < text.size(); i++){
        if(text[i] != ','){
            tmp += text[i];
        }else break;
    }
    return tmp;
}

/* 获取对应reduce编号的所有中间文件 op就是reduce的编号*/
vector<string> getAllfile(string path,int op)
{
    DIR* dir=opendir(path.c_str());
    vector<string> ret;
    if(dir==NULL){
        printf("[ERROR] %s is not a directory or not exist!",path.c_str());
        return ret;
    }
    struct dirent* entry;
    while((entry=readdir(dir))!=NULL){
        int len=strlen(entry->d_name);
        int oplen=to_string(op).size();
        if(len-oplen<5) continue;
        string filename(entry->d_name);
        if(!(filename[0]=='m'&&filename[1]=='r'&&filename[len-oplen-1]=='-')) continue;
        string cmp_str=filename.substr(len-oplen,oplen);
        if(cmp_str==to_string(op)){
            ret.emplace_back(entry->d_name);
        }
    }
    closedir(dir);
    return ret;
}

/* 对于每一个reduceTask ，获取所有相关文件并将value的list以string写入vector */
/* vector中每个元素的形式为"abc 11111" */
vector<KeyValue> Myshuffle(int reduceTaskNum)
{
    string path;
    vector<string> str;
    str.clear();
    vector<string> filename=getAllfile(".",reduceTaskNum);
    unordered_map<string,string> hash;
    for(int i=0;i<filename.size();i++){
        path=filename[i];
        char text[path.size()+1];
        strcpy(text,path.c_str());
        KeyValue kv=getContent(text);
        string context=kv.value;
        vector<string> retStr=split(context,' ');
        str.insert(str.end(),retStr.begin(),retStr.end());
    }
    for(const auto& a:str){
        hash[split(a)]+="1";
    }
    vector<KeyValue> retKVs;
    for(const auto& a:hash){
        KeyValue temp;
        temp.key=a.first;
        temp.value=a.second;
        retKVs.emplace_back(temp);
    }
    sort(retKVs.begin(),retKVs.end(),[](KeyValue& a,KeyValue& b)->bool{
        return a.key<b.key;
    });
    return retKVs;
}

void* mapWorker(void* arg)
{
    // 1.初始化client连接用于后续RPC；获取自己唯一的MapTaskID
    buttonrpc client;
    client.as_client("127.0.0.1", 5555);
    pthread_mutex_lock(&map_mutex);
    int mapTaskIdx=MapId++;
    pthread_mutex_unlock(&map_mutex);
    bool ret=false;
    while(1){
    // 2.通过RPC从master获取任务
        ret=client.call<bool>("isMapDone").val();
        if(ret){
            pthread_cond_broadcast(&cond);
            return NULL;
        }
        string taskTmp=client.call<string>("assignTask").val(); // 通过RPC返回值取得任务，在map中即为文件名
        if(taskTmp=="empty") continue;
        printf("%d get the task: %s\n",mapTaskIdx,taskTmp.c_str());
        pthread_mutex_lock(&map_mutex);

        /* ------------------------超时重发测试部分-------------------------------- */
        if(disabledMapId==1||disabledMapId==3||disabledMapId==5){
            disabledMapId++;
            pthread_mutex_unlock(&map_mutex);
            printf("%d recv task : %s is stop\n",mapTaskIdx,taskTmp.c_str());
            while(1){
                sleep(2);
            }
        }else{
            disabledMapId++;
        }
        pthread_mutex_unlock(&map_mutex);
        /* ------------------------超时重发测试部分-------------------------------- */

    // 3.拆分任务，任务返回为文件path以及map任务编号，将filename以及content封装到kv的key及value中
        char task[taskTmp.size()+1];
        strcpy(task,taskTmp.c_str());
        KeyValue kv=getContent(task);

    // 4.执行map函数，然后将中间值写入本地
        vector<KeyValue> kvs=mapF(kv);
        writeInDisk(kvs,mapTaskIdx);
    
    // 5.发送RPC给master告知任务已完成
        printf("%d finish the task : %s\n",mapTaskIdx,taskTmp.c_str());
        client.call<void>("setMapStat",taskTmp);
    }
}

/* 用于最后写入磁盘的函数 输出最终结果*/
void myWrite(int fd,vector<string>& str)
{
    int len=0;
    char buf[2];
    sprintf(buf,"\n");
    for(auto s:str){
        len=write(fd,s.c_str(),s.size());
        write(fd,buf,strlen(buf));
        if(len==-1){
            perror("write");
            exit(-1);
        }
    }
}

void* reduceWorker(void* arg)
{
    buttonrpc client;
    client.as_client("127.0.0.1",5555);
    bool ret=false;
    while(1){
        // 若工作完成直接退出reduce的worker线程
        ret=client.call<bool>("Done").val();
        if(ret) return NULL;
        int reduceTaskIdx=client.call<int>("assignReduceTask").val();
        if(reduceTaskIdx==-1) continue;
        printf("%ld get the task%d\n",pthread_self(),reduceTaskIdx);
        pthread_mutex_lock(&map_mutex);

        /* ------------------------超时重发测试部分-------------------------------- */
        if(disabledReduceId==1||disabledReduceId==3||disabledReduceId==5){
            disabledReduceId++;
            pthread_mutex_unlock(&map_mutex);
            printf("recv task%d reduceTaskIdx is stop in %ld\n",reduceTaskIdx,pthread_self());
            while(1){
                sleep(2);
            }
        }
        else disabledReduceId++;
        pthread_mutex_unlock(&map_mutex);
        /* ------------------------超时重发测试部分-------------------------------- */

        // 取得reduce任务，读取对应文件，shuffle后调用reduceFunc进行reduce处理
        vector<KeyValue> kvs=Myshuffle(reduceTaskIdx);
        vector<string> ret=reduceF(kvs);
        vector<string> str;// 存了最终结果 {"my 3","you 3"};
        for(int i=0;i<kvs.size();i++){
            str.emplace_back(kvs[i].key+" "+ret[i]);
        }
        string filename="mr-out"+to_string(reduceTaskIdx);
        int fd=open(filename.c_str(),O_WRONLY|O_CREAT|O_APPEND,0664);
        myWrite(fd,str);
        close(fd);
        printf("%ld finsih the task%d\n",pthread_self(),reduceTaskIdx);
        client.call<bool>("setReduceStat",reduceTaskIdx);
    }
}

// 删除最终输出文件，用于程序第二次执行时清除上次保存结果
void removeOutputFiles()
{
    string path;
    for(int i=0;i<MAX_REDUCE_NUM;i++){
        path="mr-out-"+to_string(i);
        int ret=access(path.c_str(),F_OK);
        if(ret==0) remove(path.c_str());
    }
}

int main()
{
    pthread_mutex_init(&map_mutex,NULL);
    pthread_cond_init(&cond,NULL);

    // 运行时从动态库加载map及reduce函数
    void* handle=dlopen("./libmrFunc.so",RTLD_LAZY);
    if(!handle){
        cerr<<"Can't open library: "<<dlerror()<<"\n";
        exit(-1);
    }
    mapF=(MapFunc)dlsym(handle,"mapF");
    if(!mapF){
        cerr<<"Can't load symbol 'mapF': "<<dlerror()<<"\n";
        dlclose(handle);
        exit(-1);
    }
    reduceF=(ReduceFunc)dlsym(handle,"reduceF");
    if(!reduceF){
        cerr<<"Can't load symbol 'reduceF': "<<dlerror()<<"\n";
        dlclose(handle);
        exit(-1);
    }

    // 作为RPC请求端
    buttonrpc work_client;
    work_client.as_client("127.0.0.1",5555);
    work_client.set_timeout(5000);
    map_task_num=work_client.call<int>("getMapNum").val();
    reduce_task_num=work_client.call<int>("getReduceNum").val();
    removeFiles();             // 若有清理上次输出的中间文件
    removeOutputFiles();       // 清理上次输出的最终文件

    // 创建多个map及reduce的worker线程
    pthread_t tidMap[map_task_num];
    pthread_t tidReduce[reduce_task_num];
    for(int i=0;i<map_task_num;i++){
        pthread_create(&tidMap[i],NULL,mapWorker,NULL);
        pthread_detach(tidMap[i]);
    }
    /* 主线程在这里等待所有 map 线程完成它们的任务 */
    pthread_mutex_lock(&map_mutex);
    pthread_cond_wait(&cond,&map_mutex);
    pthread_mutex_unlock(&map_mutex);
    for(int i=0;i<reduce_task_num;i++){
        pthread_create(&tidReduce[i],NULL,reduceWorker,NULL);
        pthread_detach(tidReduce[i]);
    }
    while(1){
        if(work_client.call<bool>("Done").val()) break;
        sleep(1);
    }

    // 任务完成后清理中间文件，关闭打开的动态库，释放资源
    removeFiles(); 
    dlclose(handle);
    pthread_mutex_destroy(&map_mutex);
    pthread_cond_destroy(&cond);
}