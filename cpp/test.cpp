#include "make_dataset.hpp"
#include "fa.hpp"
#include "topkapi.hpp"

void test_data() {
    vector<string> dataset = getZipfData(1e5);
    for(int i = 0; i < 100; i++) {
        cout << dataset[i] << " ";
        if(i % 10 == 0) {
            cout << endl;
        }
    }
}

void test_fa() {
    vector<string> dataset = getZipfData(1e6);
    FA fa;
    vector<string> freq = fa.AHH(dataset, 0.01);
    for(int i = 0; i < (int) freq.size(); i++) {
        cout << freq[i] << endl;
    }
}

void test_topkapi() {
    vector<string> dataset = getZipfData(1e8, 20000);
    TopKAPI topkapi;
    topkapi.AHH(dataset, 4, 1024);
}

void make_data(string path, int N, int Nw) {
    vector<int> dataset = getZipfDataInt(N, Nw);
    ofstream ofile;
    ofile.open(path);
    int cnt = 0;
    for(int w: dataset) {
        cnt += 1;
        ofile << to_string(w) << " ";
        if(cnt % 50 == 0) {
            ofile << endl;
        }
    }
    ofile.close();
}


int main() {
    //test_fa();
    //test_topkapi();
    make_data("./data_1e9_20000.txt", 1e9, 20000);
    return 0;
}