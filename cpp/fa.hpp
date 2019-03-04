#pragma once
#include <bits/stdc++.h>
using namespace std;

struct FA {
    vector<string> wstring;
    vector<int> wcount;
    double epsilon;
    int numCounter;

    FA() {}
    void init(double _epsilon) {
        epsilon = _epsilon;
        wstring = vector<string>();
        wcount = vector<int>();
        numCounter = ceil(1.0/_epsilon);
    }

    int find(const string& word) {
        for(int i = 0; i < (int) wstring.size(); i++) {
            if(wstring[i] == word) return i;
        }
        return -1;
    }

    void update(const string& word) {
        int p = find(word);
        if(p == -1) {
            if((int)wstring.size() < numCounter) {
                wstring.push_back(word);
                wcount.push_back(1);
                return;
            }
            else {
                int pp = numCounter - 1;
                for(int i = 0; i <= pp; i++) {
                    wcount[i] -= 1;
                    if(wcount[i] == 0) {
                        swap(wstring[i], wstring[pp]);
                        swap(wcount[i], wcount[pp]);
                        wstring.pop_back();
                        wcount.pop_back();
                        i--;
                        pp--;
                    }
                }
                if(pp < numCounter) update(word);
                else return;
            }
        }
        else {
            wcount[p] += 1;
        }
    }
    vector<string> AHH(
        const vector<string>& data, 
        double _epsilon) {
        init(_epsilon);
        for(int i = 0; i < (int) data.size(); i++) {
            update(data[i]);
        }
        return wstring;
    }
};
