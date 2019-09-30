#include <fstream>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <chrono>
 
using namespace std;

struct CacheObject
{
    std::string id;
    uint64_t size;

    CacheObject(std::string i, uint64_t s)
        : id(i), size(s)
    {
    }
        
    bool operator==(const CacheObject &rhs) const {
        return (rhs.id == id) && (rhs.size == size);
    }
};

// forward definition of extendable hash
template <class T>
void hash_combine(std::size_t & seed, const T & v);

// definition of a hash function on CacheObjects
// required to use unordered_map<CacheObject, >
namespace std
{
    template<> struct hash<CacheObject>
    {
        inline size_t operator()(const CacheObject cobj) const
        {
            size_t seed = 0;
            hash_combine<std::string>(seed, cobj.id);
            hash_combine<uint64_t>(seed, cobj.size);
            return seed;
        }
    };
}

// hash_combine derived from boost/functional/hash/hash.hpp:212
// Copyright 2005-2014 Daniel James.
// Distributed under the Boost Software License, Version 1.0.
// (See http://www.boost.org/LICENSE_1_0.txt)
template <class T>
inline void hash_combine(std::size_t & seed, const T & v)
{
    std::hash<T> hasher;
    seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

 
int main (int argc, char* argv[])
{

    // output help if insufficient params
    if(argc != 5) {
        cerr << argv[0] << " inputFile outputFile extraFields replaceTimeStamp" << endl;
        return 1;
    }

    // trace properties
    const char* inputFile = argv[1];
    const char* outputFile = argv[2];
    const int extraFields = stoi(argv[3]);
    const int replaceTimeStamp = stoi(argv[4]);

    ifstream infile(inputFile);
    ofstream outfile(outputFile);

    cerr<< "started "
        << inputFile << " "
        << outputFile << "\n";
    
    int64_t newt=0, lastT=0;
    uint64_t idcount = 0;
    unordered_map<CacheObject, std::vector<int64_t> > newid;
    std::string id;
    int64_t t, size, appid, tmp;
    int64_t largeCounter = 0, fractionCounter = 0, reqCounter = 0, sizeZeroCounter = 0;

    uint64_t printCounter = 0;

    while (infile >> t >> id >> appid >> size)
    {
        for(int i=0; i<extraFields; i++) {
            infile >> tmp;
        }
        if(size==0) {
            sizeZeroCounter++;
            continue;
        }
        if(tmp==0) {
            tmp=size;
        }
        CacheObject obj(id, size);
        if(newid.count(obj)==0) {
            newid[obj].push_back(idcount++);
        }
        if(size>1073741824) {
            // larger than 1GB
            largeCounter++;
            while(size>1073741824) {
                // create new id
                newid[obj].push_back(idcount++);
                size -= 1073741824;
            }
            // remainder
            newid[obj].push_back(idcount++);
        }
        if(tmp<size) {
            fractionCounter++;
        }
        reqCounter++;
        if(printCounter++ > 1000000) {
            std::cerr << t << " " << reqCounter << " " << size << "\n";
            printCounter=0;
        }
    }

    std::cerr << t << " " << reqCounter << " " << size << "\n";

    std::cerr << inputFile << " reqs " << reqCounter << " large " << largeCounter << " fraction " << fractionCounter << " zeros " << sizeZeroCounter << "\n";
    // reset file
    infile.clear();
    infile.seekg(0, ios::beg);

    //    int thour = -1, twday = -1;

    while (infile >> t >> id >> appid >> size)
    {
        for(int i=0; i<extraFields; i++) {
            infile >> tmp;
        }
        if(size==0) {
            continue;
        }
        if(tmp==0) {
            tmp=size;
        }
        // create index
        CacheObject obj(id, size);
        // rewrite time
        if(lastT==0) {
            if(replaceTimeStamp==0) {
                // leave original timestamp
                newt = t;
            } else {
                newt = 0;
            }
            lastT = t;
            // parse time stamp
            // time_t time(t);
            // struct tm *tmp2 = gmtime(&time);
            // thour = tmp2->tm_hour;
            // twday = tmp2->tm_wday;
        }
        if(t>lastT) {
            if(replaceTimeStamp==0) {
                // leave original timestamp
                newt = t;
            } else {
                newt += (t-lastT);
            }
            lastT = t;
            // parse time stamp
            // time_t time(t);
            // struct tm *tmp2 = gmtime(&time);
            // thour = tmp2->tm_hour;
            // twday = tmp2->tm_wday;
        } else {
            // reuse old thour and twday
        }
        if(size>1073741824) {
            size_t ididx = 0;
            // larger than 1GB
            while(size>1073741824) {
                if(extraFields == 0) {
                    outfile << newt << " " << (newid[obj])[ididx] << " " << 1073741824 << "\n";
                } else {
                    outfile << newt << " " << (newid[obj])[ididx] << " " << 1073741824 << " " << appid << "\n";
                }
                ididx++;
                size -= 1073741824;
                if(tmp>1073741824) {
                    tmp -= 1073741824;
                }
            }
            if(extraFields == 0) {
                outfile << newt << " " << (newid[obj])[ididx] << " " << size << "\n";
            } else {
                outfile << newt << " " << (newid[obj])[ididx] << " " << size << " " << appid << "\n";
            }
            //            std::cerr << "lf " << newt << " " << (newid[obj])[ididx] << "\n";
        } else {
            if(extraFields == 0) {
                outfile << newt << " " << (newid[obj])[0] << " " << size << "\n";
            } else {
                outfile << newt << " " << (newid[obj])[0] << " " << size << " " << appid << "\n";
            }
        }
    }


    infile.close();
    outfile.close();

    return 0;
}
