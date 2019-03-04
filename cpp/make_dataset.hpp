#include <bits/stdc++.h>
using namespace std;

// generate data by the Zipf's law
// https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4176592/
vector<string> getZipfData(int N, int Nw=2000, double alpha=1.01, double beta=2.7) {
    vector<double> num(Nw, 0);
    double sum = 0;
    for(int i = 1; i <= Nw; i++) {
        num[i - 1] = 1.0/pow(i + beta, alpha);
        sum += num[i - 1];
    }
    for(int i = 0; i < Nw; i++) {
        num[i] *= N / sum;
    }
    vector<int> seq;
    for(int i = 0; i < Nw; i++) {
        for(int j = 0; j < num[i]; j++) {
            seq.push_back(i);
        }
    }
    
    for(int i = 0; i < 1000; i++) {
        if(num[i] / N < 0.01) break;
        cout << i << " " << num[i] << endl;
    }
    
    while((int)seq.size() > N) seq.pop_back();
    while((int)seq.size() < N) seq.push_back(seq.back());
    random_shuffle(seq.begin(), seq.end());
    vector<string> output;
    char tmp_s[100];
    for(int i = 0; i < N; i++) {
        sprintf(tmp_s, "%011d", seq[i]);
        output.push_back(tmp_s);
    }
    return output;
}

// output integer to avoid waste of memory
vector<int> getZipfDataInt(int N, int Nw=2000, double alpha=1.01, double beta=2.7) {
    vector<double> num(Nw, 0);
    double sum = 0;
    for(int i = 1; i <= Nw; i++) {
        num[i - 1] = 1.0/pow(i + beta, alpha);
        sum += num[i - 1];
    }
    for(int i = 0; i < Nw; i++) {
        num[i] *= N / sum;
    }
    vector<int> seq;
    for(int i = 0; i < Nw; i++) {
        for(int j = 0; j < num[i]; j++) {
            seq.push_back(i);
        }
    }
    
    for(int i = 0; i < 1000; i++) {
        if(num[i] / N < 0.01) break;
        cout << i << " " << num[i] << endl;
    }
    
    
    while((int)seq.size() > N) seq.pop_back();
    while((int)seq.size() < N) seq.push_back(seq.back());
    random_shuffle(seq.begin(), seq.end());
    return seq;
}
