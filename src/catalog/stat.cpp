#include "catalog/stat.hpp"

#include "common/murmurhash.hpp"

namespace wing {

void CountMinSketch::AddCount(std::string_view key, double value) {
  for (int i=0;i<funcs_;i++) data_[i*buckets_+utils::Hash(key.data(),key.size(),(i+1)*233)%buckets_]+=value;
}

double CountMinSketch::GetFreqCount(std::string_view key) const {
  double ans=data_[utils::Hash(key.data(),key.size(),233)%buckets_];
    for (int i=1;i<funcs_;i++) ans=std::min(ans,data_[i*buckets_+utils::Hash(key.data(),key.size(),(i+1)*233)%buckets_]);
    return ans;
}

void HyperLL::Add(std::string_view key)
{
    size_t res=utils::Hash(key.data(),key.size(),233);
    int now=0;
    for (uint8_t i=0;i<10;i++) if ((res>>i)&1) now|=(1<<i);
    double hh=1;
    for (uint8_t i=1;i<=data_[now];i++) hh/=2;
    sum-=hh;
    uint8_t id=0;
    for (uint8_t i=10;;i++) if ((res>>i)&1) { id=i-9;break; }
    data_[now]=std::max(data_[now],id);
    hh=1;
    for (uint8_t i=1;i<=data_[now];i++) hh/=2;
    sum+=(double)hh;
}

double HyperLL::GetDistinctCounts() const { return 1.0/sum*N_*N_*0.7213/(1+1.079/N_); }

}  // namespace wing