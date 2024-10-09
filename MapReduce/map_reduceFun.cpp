#include<iostream>
#include<stdio.h>
#include<stdlib.h>
#include<string>
#include<sys/stat.h>
#include<unistd.h>
#include<fcntl.h>
#include<string.h>
#include<sys/types.h>
#include<bits/stdc++.h>
using namespace std;

class KeyValue
{
public:
    string key;
    string value;
};

/**
* @brief: split the text into words
* @param: text: the text to be split
* @param: length: the length of the text
* @return: a vector of words
*/
vector<string> split(char* text,int length)
{
    vector<string> str;
    string temp="";
    for(int i=0;i<length;i++){
        if(text[i]>='a' && text[i]<='z'||text[i]>='A' && text[i]<='Z'){
            temp+=text[i];
        }
        else{
            if(text[i]==' ') str.emplace_back(temp);
            temp="";
        }
    }
    if(temp!="") str.emplace_back(temp);
    return str;
}

/**
 * @brief mapF 需要打包成动态库，并且在worker中通过dlopen以及dlsym运行时加载
 * @param kv 将文本按单词划分并以出现次数代表value长度存入keyvalue中
 * @return 类似{"my:1","my:1","you:1"}即文章中my出现了2次 you出现了1次
*/
extern "C" vector<KeyValue> mapF(KeyValue kv)
{
    vector<KeyValue> kvs;
    int len=kv.value.size();
    char content[len+1];
    strcpy(content,kv.value.c_str());
    vector<string> str=split(content,len);
    for(const auto& s:str){
        KeyValue temp;
        temp.key=s;
        temp.value="1";
        kvs.emplace_back(temp);
    }
    return kvs;
}

/**
 * @brief reduceFunc,也是动态加载，输出对特定keyalue的reduce结果
 * @param 
 * @return 
*/

extern "C" vector<string> reduceF(vector<KeyValue> kvs)
{
    vector<string> str;
    for(const auto&kv:kvs){
        str.emplace_back(to_string(kv.value.size()));
    }
    return str; 
}
