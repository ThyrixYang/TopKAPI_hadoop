#pragma once
#include <bits/stdc++.h>

using namespace std;
struct TopKAPI {
    TopKAPI() {}
    void init() {

    }

    int stringHash(const string& s) {
        long long mod = 1e9 + 7;
        long long a = 123;
        long long hash = 0;
        for(char c: s) {
            hash = (hash*a + c) % mod;
        }
        return hash;
    }

    vector<string> AHH(
        const vector<string>& data,
        int num_hash,
        int num_counter) {
        srand(2);
        vector<vector<int> > CMSCounter = vector<vector<int> >(num_hash, vector<int>(num_counter, 0));
        vector<vector<int> > LHHCounter = vector<vector<int> >(num_hash, vector<int>(num_counter, 0));
        vector<vector<string> > LHHString = vector<vector<string> >(num_hash, vector<string>(num_counter, string()));
        vector<int> hasha, hashb;
        for(int i = 0; i < num_hash; i++) hasha.push_back(rand()%(int)(1e9 + 7) + 1);
        for(int i = 0; i < num_hash; i++) hashb.push_back(rand()%(int)(1e9 + 7));
        map<pair<int, int> , set<string> > oc;
        int max_crash = 0;
        //set<string> tw;
        cout << num_hash << endl;
        for(auto& word: data) {
            int h = stringHash(word);
            //tw.insert(word);
            for(int i = 0; i < num_hash; i++) {
                long long hh = ((long long)h*(long long)hasha[i] + hashb[i])%(long long)(1e9 + 7);
                
                hh %= num_counter;
                //auto& ocs = oc[make_pair(i, hh)];
                /*
                if(ocs.find(word) == ocs.end() && ocs.size() >= 5) {
                    //cout << i << endl;
                    //cout << hasha[i] << " " << hashb[i] << endl;
                    //cout << hasha[1-i] << " " << hashb[1-i] << endl;
                    for(auto x: ocs) {
                        int hx = stringHash(x);

                        long long hhx = (long long)hx*(long long)hasha[i] + hashb[i];
                        //cout << hhx%num_counter << " " << hx << " " << x << endl;
                    }
                    for(auto x: ocs) {
                        int hx = stringHash(x);
                        long long hhx = (long long)hx*(long long)hasha[1-i] + hashb[1-i];
                        //cout << hhx%num_counter << " " << hx << " " << x << endl;
                    }
                    //exit(0);
                }
                */
                //if(ocs.find(word) == ocs.end()) {
                //    ocs.insert(word);
                //    max_crash = max(max_crash, (int)ocs.size());
                //}
                //cout << hh << endl;
                CMSCounter[i][hh] += 1;
                if(LHHCounter[i][hh] == 0) {
                    LHHCounter[i][hh] += 1;
                    LHHString[i][hh] = word;
                }
                else if(word == LHHString[i][hh]) {
                    LHHCounter[i][hh] += 1;
                }
                else {
                    LHHCounter[i][hh] -= 1;
                }
            }
        }
        //cout << "max_crash: " << max_crash << endl;
        //cout << tw.size() << endl;
        set<string> candidates;
        for(auto& row: LHHString) {
            for(auto& word: row) {
                candidates.insert(word);
            }
        }
        vector<pair<int, string> > output;
        for(auto& word: candidates) {
            int h = stringHash(word);
            int ub = 1e9;
            for(int i = 0; i < num_hash; i++) {
                long long hh = ((long long)h*(long long) hasha[i] + hashb[i])%(long long)(1e9 + 7);
                hh %= num_counter;
                //if(CMSCounter[i][hh] < ub && i >= 1) {
                //cout << "update: " << i << " " << word << " " << ub << " to " << CMSCounter[i][hh] << endl;
                //}
                ub = min(ub, CMSCounter[i][hh]);
            }

            output.push_back(make_pair(ub, word));
        }
        sort(output.begin(), output.end(), greater<pair<int, string> >());
        for(int i = 0; i < 50; i++) {
            cout << output[i].second << " " << output[i].first << endl;
        }
        return vector<string>();
    }

    vector<string> AHH(
        const vector<string>& data,
        double phi,
        double epsilon,
        double delta) {
        int N = data.size();
        int _l = log(N*2/delta);
        int _b = 1.0 / epsilon;
        vector<vector<int> > CMSCounter = vector<vector<int> >(_l, vector<int>(_b, 0));
        vector<vector<int> > LHHCounter = vector<vector<int> >(_l, vector<int>(_b, 0));
        return vector<string>();
    }
};