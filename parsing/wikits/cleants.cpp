#include <fstream>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <string>
 
using namespace std;

int main (int argc, char* argv[])
{
    // output help if insufficient params
    if(argc != 4) {
        cerr << argv[0] << " tsFile traceFile outputFile" << endl;
        return 1;
    }
    
    // trace properties
    const char* tsFileName = argv[1];
    ifstream tsFile(tsFileName);
    const char* traceFileName = argv[2];
    ifstream trFile(traceFileName);
    const char* outFileName = argv[3];
    ofstream outFile(outFileName);

    // ts file parsing
    int64_t ts, tmp1, tmp2, reqcount;
    int64_t ts_old, reqcount_old;
    tsFile >> ts_old >> tmp1 >> tmp2 >> reqcount_old; // first line (discard tmps)
    tsFile >> ts >> tmp1 >> tmp2 >> reqcount; // second line (overwrite tmps)
    int64_t time_delta = ts - ts_old;
    int64_t time = 0;
    int64_t time_increment = reqcount_old / time_delta;
    int64_t reqs_since_last_row = 0;
    cerr << "rate " << reqcount / time_delta << " time_increment " << time_increment << "\n";
    // trace file parsing
    int64_t discard, id, size, appid;
    while (trFile >> discard >> id >> size >> appid) {
        outFile << time << " " << id << " " << size << " " << appid << "\n";
        time+=time_increment;
        reqs_since_last_row++;
        if(reqs_since_last_row >= reqcount_old) {
            if(tsFile.good()) {
                // move forward in ts file
                ts_old = ts;
                reqcount_old = reqcount;
                tsFile >> ts >> tmp1 >> tmp2 >> reqcount; // next line
                time_delta = ts - ts_old;
                time_increment = reqcount_old / time_delta;
                cerr << "rate " << reqcount / time_delta << " time_increment " << time_increment << "\n";
            }
            // if not good, just keep incrementing with last known rate
            reqs_since_last_row = 0;
        }
    }

    tsFile.close();
    trFile.close();
    outFile.close();

    return 0;
}
