#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <sched.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <map>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
using namespace std;
uint64_t now(){return chrono::duration_cast<chrono::nanoseconds>(chrono::steady_clock::now().time_since_epoch()).count();}
void require(bool b,const string&s){if(!b)throw runtime_error(s);}
string resp(const vector<string>&a){string s="*"+to_string(a.size())+"\r\n";for(auto&v:a)s+="$"+to_string(v.size())+"\r\n"+v+"\r\n";return s;}
uint64_t mix(uint64_t x){x+=0x9e3779b97f4a7c15ULL;x=(x^(x>>30))*0xbf58476d1ce4e5b9ULL;x=(x^(x>>27))*0x94d049bb133111ebULL;return x^(x>>31);}
string member(int i,bool added=false){string s(32,'0');s[0]=added?'X':'M';const char*a="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";uint64_t x=mix(i+20261029ULL);for(int j=1;j<24;j++){if(j%9==0)x=mix(x);s[j]=a[x&63];x>>=6;}char b[9];snprintf(b,9,"%08x",i);s.replace(24,8,b);return s;}
// Incremental RESP framing: already consumed elements are never rescanned.
// Only a bounded prefix is retained as token views; large replies remain one
// receive buffer, and full content is verified in separate correctness runs.
struct Frame{
 size_t pos=0;long todo=1,root_count=-1;char root=0;long integer=0;uint64_t bulks=0;vector<pair<size_t,size_t>> spans;bool nil=false;
 void reset(){pos=0;todo=1;root_count=-1;root=0;integer=0;bulks=0;spans.clear();nil=false;}
 bool scan(const string&s){while(todo){if(pos>=s.size())return false;size_t e=s.find("\r\n",pos);if(e==string::npos)return false;char t=s[pos];string val=s.substr(pos+1,e-pos-1);size_t end=e+2;long n=0;
  if(t=='-')throw runtime_error("server error: "+val);
  if(t=='$'||t=='*'||t==':'){n=stol(val);require(n>=-1,"negative length");}
  if(t=='$'&&n>=0){end+=size_t(n)+2;if(end>s.size())return false;require(s.compare(end-2,2,"\r\n")==0,"bulk terminator");}
  if(!root){root=t;root_count=t=='*'?n:-1;integer=t==':'?n:0;nil=t=='$'&&n==-1;}
  --todo;if(t=='*'&&n>=0){require(n<=3000000,"array too large");todo+=n;}else if(t=='$'){bulks++;if(n>=0&&spans.size()<16384)spans.emplace_back(e+2,n);}else require(t==':'||t=='+',"unsupported RESP");pos=end;
 }return true;}
};
struct Stats{vector<uint32_t> us;uint64_t sum=0;void add(uint64_t n){auto x=uint32_t(min<uint64_t>(UINT32_MAX,(n+500)/1000));us.push_back(x);sum+=x;}};
struct Client{int fd=-1,id=0,stage=0,slot=0,loadpos=0,loadcount=0,key_index=0;uint64_t seq=0,started=0,pair_start=0,cycle_start=0,cycle_members=0;string in,out,cursor="0",saved_member,saved_score;size_t sent=0;Frame frame;};
string host,key,op,phase,outpath;int port=6379,N,connections,threads,key_count=1,hot_percent=0;vector<string> keys;vector<vector<uint64_t>> key_operations;double seconds;uint64_t seed;
atomic<int> ready=0;atomic<bool> failed=false;atomic<uint64_t> start=0,finish=0;mutex error_mutex;string error;
vector<map<string,Stats>> stats;vector<vector<uint64_t>> persecond;vector<uint64_t> bytes_received;
bool paired(){return op=="add_local"||op=="add_jump"||(op=="incr"||op=="incr_local"||op=="incr_2048")||op=="insert_remove"||op=="remove_add"||op=="popmin"||op=="popmax";}
bool ranged(){return op=="top10"||op=="top100"||op=="top1000"||op=="middle100"||op=="tail100"||op=="byscore100"||op=="offset100"||op=="full";}
long score(int i){return long(i)*1024;}
string scalar(const Client&c,size_t i=0){require(i<c.frame.spans.size(),"missing bulk");auto [p,n]=c.frame.spans[i];return c.in.substr(p,n);}
int socket_open(){int fd=socket(AF_INET,SOCK_STREAM,0),one=1;require(fd>=0,"socket");setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof one);sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(port);inet_pton(AF_INET,host.c_str(),&a.sin_addr);require(connect(fd,(sockaddr*)&a,sizeof a)==0,"connect: "+string(strerror(errno)));fcntl(fd,F_SETFL,O_NONBLOCK);return fd;}
string make(Client&c,mt19937_64&rng){
 if(phase=="load"||phase=="verify"){
  c.loadcount=min(8192,N-c.loadpos);
  // Verify disjoint score intervals to avoid quadratic deep-rank scans in
  // backends without rank indexes. Every returned member and score is checked.
  if(phase=="verify")return resp({"ZRANGEBYSCORE",keys[c.key_index],to_string(score(c.loadpos)),to_string(score(c.loadpos+c.loadcount-1)),"WITHSCORES"});
 }
 if(phase!="load"&&phase!="verify"&&c.stage==0&&(op!="scan"||c.cursor=="0")){
  auto pick=mix(seed+uint64_t(c.id)*0x9e3779b97f4a7c15ULL+c.seq);
  c.key_index=key_count==1?0:hot_percent?(pick%100<uint64_t(hot_percent)?0:1+mix(pick)%(key_count-1)):pick%key_count;
 }
 const auto& key=keys[c.key_index];
 if(phase=="load"){vector<string>a={"ZADD",key};c.loadcount=min(8192,N-c.loadpos);for(int j=0;j<c.loadcount;j++){int i=c.loadpos+j;a.push_back(to_string(score(i)));a.push_back(member(i));}return resp(a);}
 if(c.stage==0){c.slot=c.id+connections*(rng()%((N-1-c.id)/connections+1));c.seq++;}
 int i=c.slot;string m=member(i);long orig=score(i);
 if(op=="card")return resp({"ZCARD",key});
 if(op=="score"||op=="miss")return resp({"ZSCORE",key,op=="miss"?member(i,true):m});
 if(op=="mscore32"){vector<string>a={"ZMSCORE",key};for(int j=0;j<32;j++)a.push_back(member((i+j)%N));return resp(a);}
 if(op=="rank"||op=="revrank")return resp({op=="rank"?"ZRANK":"ZREVRANK",key,m});
 if(op=="count")return resp({"ZCOUNT",key,to_string(score(N/4)),to_string(score(3*N/4))});
 if(op=="scan")return resp({"ZSCAN",key,c.cursor,"COUNT","1000"});
 if(ranged()){
  if(op=="byscore100")return resp({"ZRANGEBYSCORE",key,to_string(score(N/2)),to_string(score(N/2+99)),"WITHSCORES"});
  if(op=="offset100")return resp({"ZRANGEBYSCORE",key,"-inf","+inf","WITHSCORES","LIMIT",to_string(N/2),"100"});
  int offset=op=="middle100"?N/2:op=="tail100"?N-100:0;
  int count=op=="top10"?10:op=="top1000"?1000:op=="full"?N:100;
  return resp({"ZREVRANGE",key,to_string(offset),to_string(offset+count-1),"WITHSCORES"});
 }
 if(op=="add_local"||op=="add_jump")return resp({"ZADD",key,"XX",to_string(c.stage?orig:op=="add_local"?orig+1:orig+long(N)*2048),m});
 if((op=="incr"||op=="incr_local"||op=="incr_2048"))return resp({"ZINCRBY",key,to_string((c.stage?-1L:1L)*(op=="incr_local"?1L:op=="incr_2048"?2048L:long(N)*2048)),m});
 if(op=="insert_remove")return c.stage?resp({"ZREM",key,member(i,true)}):resp({"ZADD",key,"NX",to_string(orig),member(i,true)});
 if(op=="remove_add")return c.stage?resp({"ZADD",key,to_string(orig),m}):resp({"ZREM",key,m});
 if(op=="popmin"||op=="popmax")return c.stage?resp({"ZADD",key,c.saved_score,c.saved_member}):resp({op=="popmin"?"ZPOPMIN":"ZPOPMAX",key,"1"});
 throw runtime_error("unknown operation");
}
void check(Client&c){auto&f=c.frame;auto integer=[&](long n){require(f.root==':'&&f.integer==n,"integer mismatch "+to_string(f.integer)+" expected "+to_string(n));};
 if(phase=="load"){integer(c.loadcount);return;}
 if(phase=="verify"){
  require(f.root_count==2*c.loadcount,"verification cardinality");
  for(int j=0;j<c.loadcount;j++)require(scalar(c,2*j)==member(c.loadpos+j)&&stol(scalar(c,2*j+1))==score(c.loadpos+j),"full content verification");
  return;
 }
 if(op=="card"){integer(N);return;}if(op=="rank"){integer(c.slot);return;}if(op=="revrank"){integer(N-1-c.slot);return;}if(op=="count"){integer(3*N/4-N/4+1);return;}
 if(op=="miss"){require(f.root=='$'&&f.nil,"missing member expected");return;}
 if(op=="score"){require(stol(scalar(c))==score(c.slot),"score mismatch");return;}
 if(op=="mscore32"){require(f.root_count==32,"mscore length");for(int j=0;j<32;j++)require(stol(scalar(c,j))==score((c.slot+j)%N),"mscore value");return;}
 if(op=="scan"){require(f.root_count==2&&f.bulks%2==1,"scan shape");c.cursor=scalar(c);c.cycle_members+=(f.bulks-1)/2;return;}
 if(ranged()){int cnt=op=="top10"?10:op=="top1000"?1000:op=="full"?N:100;require(f.root_count==cnt*2&&f.bulks==uint64_t(cnt*2),"range count");int first=op=="middle100"?N-1-N/2:op=="tail100"?99:op=="byscore100"||op=="offset100"?N/2:N-1;bool ascending=op=="byscore100"||op=="offset100";
  // Bounded checks keep full-read client validation from dominating its timing.
  int checked=min(cnt,8);for(int j=0;j<checked;j++){int id=first+(ascending?j:-j);require(scalar(c,2*j)==member(id)&&stol(scalar(c,2*j+1))==score(id),"range order/value");}return;}
 if(op=="add_local"||op=="add_jump"){integer(0);return;}
 if((op=="incr"||op=="incr_local"||op=="incr_2048")){long expected=score(c.slot)+(c.stage?0:op=="incr_local"?1L:op=="incr_2048"?2048L:long(N)*2048);require(stol(scalar(c))==expected,"increment value");return;}
 if((op=="popmin"||op=="popmax")&&!c.stage){require(f.root_count==2,"pop shape");c.saved_member=scalar(c);c.saved_score=scalar(c,1);require(c.saved_member.size()==32,"pop member");return;}
 integer(1);
}
void worker(int tid){vector<Client>cs;int ep=-1;try{
 cpu_set_t mask;CPU_ZERO(&mask);CPU_SET(tid,&mask);require(sched_setaffinity(0,sizeof mask,&mask)==0,"affinity");
 mt19937_64 rng(seed+tid*0x9e3779b97f4a7c15ULL);ep=epoll_create1(0);require(ep>=0,"epoll");
 for(int id=tid;id<connections;id+=threads){Client c;c.id=id;c.key_index=(phase=="load"||phase=="verify")?id:0;c.fd=socket_open();cs.push_back(move(c));}
 auto arm=[&](int j,bool add){epoll_event e{};e.events=EPOLLIN|(cs[j].sent<cs[j].out.size()?EPOLLOUT:0);e.data.u32=j;require(epoll_ctl(ep,add?EPOLL_CTL_ADD:EPOLL_CTL_MOD,cs[j].fd,&e)==0,"epoll_ctl");};
 ready++;while(!start&&!failed)this_thread::yield();if(failed)throw runtime_error("cancelled");
 for(int j=0;j<(int)cs.size();j++){auto&c=cs[j];c.out=make(c,rng);c.started=c.pair_start=c.cycle_start=now();arm(j,true);}
 int active=cs.size();uint64_t last=now();epoll_event events[128];
 while(active&&!failed){int n=epoll_wait(ep,events,128,100);require(n>=0||errno==EINTR,"epoll_wait");require(now()-last<180000000000ULL,"180s without progress");
  for(int j=0;j<n;j++){int ix=events[j].data.u32;auto&c=cs[ix];if(c.fd<0)continue;require(!(events[j].events&(EPOLLHUP|EPOLLERR)),"socket error");
   if(events[j].events&EPOLLOUT){while(c.sent<c.out.size()){auto z=send(c.fd,c.out.data()+c.sent,c.out.size()-c.sent,MSG_NOSIGNAL);if(z>0)c.sent+=z;else if(z<0&&(errno==EAGAIN||errno==EWOULDBLOCK))break;else throw runtime_error("send");}arm(ix,false);}
   if(events[j].events&EPOLLIN){char buf[65536];for(;;){auto z=recv(c.fd,buf,sizeof buf,0);if(z>0){c.in.append(buf,z);bytes_received[tid]+=z;}else if(z<0&&(errno==EAGAIN||errno==EWOULDBLOCK))break;else throw runtime_error("recv");}
    require(c.in.size()<192*1024*1024,"reply size cap");if(!c.frame.scan(c.in))continue;require(c.frame.pos==c.in.size(),"trailing reply");uint64_t done=now();check(c);last=done;
    string metric=(phase=="load"||phase=="verify")?phase:paired()?(c.stage?"restore":"primary"):"command";stats[tid][metric].add(done-c.started);bool next=true;
    if(phase=="load"||phase=="verify"){
     c.loadpos+=c.loadcount;if(c.loadpos==N){key_operations[tid][c.key_index]++;c.loadpos=0;c.key_index+=connections;}next=c.key_index<key_count;
    }
    else if(paired()&&!c.stage)c.stage=1;
    else{key_operations[tid][c.key_index]++;stats[tid]["operation"].add(done-c.pair_start);size_t sec=(done-start)/1000000000ULL;if(sec<persecond[tid].size())persecond[tid][sec]++;
     if(op=="scan"&&c.cursor=="0"){require(c.cycle_members==uint64_t(N),"scan cycle member count");stats[tid]["scan_cycle"].add(done-c.cycle_start);c.cycle_members=0;c.cycle_start=done;}
     c.stage=0;next=done<start+uint64_t(seconds*1e9);}
    if(!next){epoll_ctl(ep,EPOLL_CTL_DEL,c.fd,nullptr);close(c.fd);c.fd=-1;active--;continue;}
    c.out=make(c,rng);c.in.clear();c.frame.reset();c.sent=0;c.started=now();if(!c.stage)c.pair_start=c.started;arm(ix,false);
   }
  }
 }
 uint64_t t=now(),old=finish;while(old<t&&!finish.compare_exchange_weak(old,t)){}
 }catch(const exception&e){failed=true;lock_guard<mutex>l(error_mutex);if(error.empty())error=e.what();}
 for(auto&c:cs)if(c.fd>=0)close(c.fd);if(ep>=0)close(ep);
}
double cpu(const rusage&r){return r.ru_utime.tv_sec+r.ru_utime.tv_usec/1e6+r.ru_stime.tv_sec+r.ru_stime.tv_usec/1e6;}
int main(int argc,char**argv){try{map<string,string>a;for(int i=1;i<argc;i+=2){require(i+1<argc,"arguments");a[argv[i]]=argv[i+1];}host=a.at("--host");key=a.at("--key");op=a.at("--op");phase=a.at("--phase");N=stoi(a.at("--members"));connections=stoi(a.at("--connections"));threads=min(connections,stoi(a.at("--threads")));seconds=stod(a.at("--seconds"));seed=stoull(a.at("--seed"));outpath=a.at("--out");require(N>=1000&&N<=100000000&&connections<=N&&threads>=1&&threads<=32,"bounds");key_count=a.count("--keys")?stoi(a.at("--keys")):1;hot_percent=a.count("--hot-percent")?stoi(a.at("--hot-percent")):0;
 require(key_count>=1&&key_count<=64&&hot_percent>=0&&hot_percent<=100,"key distribution bounds");
 if(phase=="load"||phase=="verify")require(connections<=key_count,"distinct loader key ownership");
 for(int k=0;k<key_count;k++){ostringstream name;name<<key<<":"<<hex<<setw(16)<<setfill('0')<<mix(k+20261030ULL);keys.push_back(key_count==1?key:name.str());}
 key_operations.assign(threads,vector<uint64_t>(key_count));
 stats.resize(threads);persecond.assign(threads,vector<uint64_t>(size_t(seconds)+1));bytes_received.resize(threads);rusage before{},after{};getrusage(RUSAGE_SELF,&before);vector<thread>ts;for(int t=0;t<threads;t++)ts.emplace_back(worker,t);while(ready<threads&&!failed)this_thread::sleep_for(chrono::milliseconds(1));start=now();for(auto&t:ts)t.join();getrusage(RUSAGE_SELF,&after);require(!failed,error);
 double elapsed=(finish-start)/1e9;map<string,Stats>all;uint64_t bytes=0;for(int t=0;t<threads;t++){bytes+=bytes_received[t];for(auto&[k,s]:stats[t]){auto&d=all[k];d.sum+=s.sum;d.us.insert(d.us.end(),s.us.begin(),s.us.end());}}
 ostringstream o;o<<fixed<<setprecision(6)<<"{\"op\":\""<<op<<"\",\"phase\":\""<<phase<<"\",\"members\":"<<N<<",\"connections\":"<<connections<<",\"threads\":"<<threads<<",\"seed\":"<<seed<<",\"elapsed_s\":"<<elapsed<<",\"client_cpu_cores\":"<<(cpu(after)-cpu(before))/elapsed<<",\"client_peak_rss_kib\":"<<after.ru_maxrss<<",\"response_bytes\":"<<bytes<<",\"response_mib_s\":"<<bytes/elapsed/1048576<<",\"errors\":0,\"metrics\":{";
 bool first=true;for(auto&[k,s]:all){if(!first)o<<',';first=false;sort(s.us.begin(),s.us.end());require(!s.us.empty(),"empty metric");auto p=[&](double q){return s.us[min(s.us.size()-1,size_t(ceil(q*s.us.size()))-1)]/1000.;};o<<'"'<<k<<"\":{\"count\":"<<s.us.size()<<",\"qps\":"<<s.us.size()/elapsed<<",\"avg_ms\":"<<s.sum/double(s.us.size())/1000<<",\"p50_ms\":"<<p(.5)<<",\"p99_ms\":"<<p(.99)<<",\"p999_ms\":"<<p(.999)<<",\"max_ms\":"<<s.us.back()/1000.<<'}';}
 o<<"},\"per_second\":[";for(size_t sec=0;sec<size_t(seconds);sec++){if(sec)o<<',';uint64_t count=0;for(auto&v:persecond)count+=v[sec];o<<count;}o<<"],\"key_count\":"<<key_count<<",\"hot_percent\":"<<hot_percent<<",\"per_key_operations\":[";for(int k=0;k<key_count;k++){if(k)o<<',';uint64_t n=0;for(auto&v:key_operations)n+=v[k];o<<n;}o<<"]}\n";ofstream f(outpath);f<<o.str();require(bool(f),"write result");cout<<o.str();return 0;
 }catch(const exception&e){cerr<<"ERROR: "<<e.what()<<'\n';return 1;}}
