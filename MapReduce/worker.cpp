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

