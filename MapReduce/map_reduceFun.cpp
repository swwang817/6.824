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





