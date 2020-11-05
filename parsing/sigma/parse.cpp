#include <iostream>
#include <fstream>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <math.h>
#include <vector>

template <class T>
inline void endswap(T *objp)
{
  unsigned char *memp = reinterpret_cast<unsigned char*>(objp);
  std::reverse(memp, memp + sizeof(T));
}
using namespace std;

#pragma pack(1)
struct Req
{
    int64_t ts;
    int64_t client_machine;
    int64_t rack ;
    int64_t dataset;
    int64_t oid;
    int64_t readid1;
    int64_t readid2;
    int64_t offset;
    int32_t length;
    int64_t jobid;
};

unordered_map< string, unordered_map < int64_t, uint64_t > > dists;
inline void dist(string name, int64_t value) {
    (dists[name])[value]++;
}

unordered_map<string, uint64_t > readids;

int64_t prev_ts = -1;
uint64_t reqs=0;
uint64_t global_id=0;

int main (int args, char *argv[]) {
    Req CurReq;

    map<uint64_t, vector< pair <uint64_t,uint64_t>>> objs;
//    map<uint64_t, vector<uint64_t>> objs_ids;
    for(int ag=1; ag<args; ag++) {
        //        cerr << argv[ag] << "\n";

        std::fstream fh;
        fh.open(argv[ag], std::fstream::in | std::fstream::binary);

        // get distributions of IA-time, etc.

        while(!fh.eof() && !fh.fail() && !fh.bad()) {
            fh.read(reinterpret_cast<char*>(&CurReq), sizeof(CurReq));
            // recall: endianness
            endswap(&CurReq.ts);
            if(prev_ts!=-1) {
                int64_t logDiff;
                if(CurReq.ts==prev_ts) {
                    logDiff = 0;
                } else {
                    logDiff = log10(100.0*(CurReq.ts-prev_ts));
                }
                dist("ia",logDiff);
            }
            prev_ts=CurReq.ts;
        
            endswap(&CurReq.client_machine);
            dist("cm",CurReq.client_machine);

            endswap(&CurReq.rack);
            dist("rack",CurReq.rack);

            endswap(&CurReq.oid);
            dist("oid",CurReq.oid);
            
	    endswap(&CurReq.readid1);
            endswap(&CurReq.readid2);
            readids[to_string(CurReq.readid1)+to_string(CurReq.readid2)]++;

            endswap(&CurReq.offset);
            int64_t logOff = log2(CurReq.offset);
            dist("offset",logOff);

            endswap(&CurReq.length);
            int64_t logLen = log2(CurReq.length);
            dist("length",logLen);

            endswap(&CurReq.jobid);
            dist("jobid",CurReq.jobid);

            reqs++;
	    uint64_t block_id = 0;
	    if (objs.count(CurReq.oid)) {
		    for (auto & it: objs[CurReq.oid]) {
			double diff = it.first - CurReq.offset;
//			cout << diff << "\n";
			if (abs(diff) <= 4202528) {
				block_id = it.second;
				break;
			}
		    }
		    if (block_id == 0) {
		    	global_id++;
			block_id = global_id;
			objs[CurReq.oid].emplace_back(make_pair(CurReq.offset, block_id));
		    }
//		    objs_ids[CurReq.oid].emplace_back(block_id);
	    } else {
		    vector<pair<uint64_t,uint64_t>> v;
		    global_id++;
		    block_id = global_id;
		    v.emplace_back(make_pair(CurReq.offset, block_id));
		    objs[CurReq.oid] = v;
//		    vector<uint64_t> v_id;
//		    v_id.emplace_back(block_id);
//		    objs_ids[CurReq.oid] = v_id;
	    }


	    cout << CurReq.ts << " " << block_id << " " << 1 << "\n";
//            if(reqs % 10000000 == 0) {
            //     cerr << reqs << " " << CurReq.ts << " " << CurReq.oid << " " << CurReq.jobid << "\n";
//                 cout << CurReq.ts << " " << (uint64_t)CurReq.oid << " " << CurReq.length << "\n";
//	    }
//            cout << CurReq.offset << "\n";
	}
        fh.close();

/*	for (auto & pair: objs_ids) {
		cout << "obj id " << pair.first;
		for (auto & it: pair.second) {
			cout << " pair " << it; // .first << " " << it.second;
		}
		cout << "\n";
	}
*/	
/*        map<uint64_t, uint64_t> counts;
        for(auto & nd: dists) {
            if(nd.first == "ia" || nd.first == "offset" || nd.first == "length") {
                for(auto & val: nd.second) {
                    cout << argv[ag] << " " << nd.first << " " << val.first << " " << val.second << "\n";
                }
            } else {
                counts.clear();
                for(auto & val: nd.second) {
                    counts[val.second]++;
                }
                for(auto & val: counts) {
                    cout << argv[ag] << " " << nd.first << " " << val.first << " " << val.second << "\n";
                }
            }
        }
        counts.clear();
        for(auto & val: readids) {
            counts[val.second]++;
        }
        for(auto & val: counts) {
            cout << argv[ag] << " " << "readid" << " " << val.first << " " << val.second << "\n";
        }

        cout << argv[ag] << " reqs " << reqs << " " << reqs << "\n";
*/
    }
    return 0;
    }
