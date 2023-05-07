#ifndef BPLUS_TREE_H_
#define BPLUS_TREE_H_

#include "common/logging.hpp"

#include <cassert>
#include <filesystem>
#include <optional>
#include <stack>
#include <vector>
#include <iostream>

#include "page-manager.hpp"

namespace wing {

/* Level 0: Leaves
 * Level 1: Inners
 * Level 2: Inners
 * ...
 * Level N: Root
 *
 * Initially the root is a leaf.
 *-----------------------------------------------------------------------------
 * Meta page:
 * Offset(B)  Length(B) Description
 * 0          1         Level num of root
 * 4          4         Root page ID
 * 8          8         Number of tuples (i.e., KV pairs)
 *-----------------------------------------------------------------------------
 * Inner page:
 * next_0 key_0 next_1 key_1 next_2 ... next_{n-1} key_{n-1} next_n
 * ^^^^^^^^^^^^ ^^^^^^^^^^^^            ^^^^^^^^^^^^^^^^^^^^ ^^^^^^
 *    Slot_0       Slot_1                    Slot_{n-1}      Special
 * Note that the lengths of keys are omitted in slots because they can be
 * deduced with the lengths of slots.
 *-----------------------------------------------------------------------------
 * Leaf page:
 * len(key_0) key_0 value_0 len(key_1) key_1 value_1 ...
 * ^^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^
 *        Slot_0                   Slot_1
 *
 * len(key_{n-1}) key_{n-1} value_{n-1} prev_leaf next_leaf
 * ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^
 *            Slot_{n-1}                      Special
 * The type of len(key) is pgoff_t. Note that the lengths of values are omitted
 * in slots because they can be deduced with the lengths of slots:
 *     len(value_i) = len(Slot_i) - sizeof(pgoff_t) - len(key_i)
 */

// Parsed inner slot.
struct InnerSlot {
  // The child in this slot. See the layout of inner page above.
  pgid_t next;
  // The strict upper bound of the keys in the corresponding subtree of child.
  // i.e., all keys in the corresponding subtree of child < "strict_upper_bound"
  std::string_view strict_upper_bound;
};
// Parse the content of on-disk inner slot
InnerSlot InnerSlotParse(std::string_view slot);
// The size of the inner slot in on-disk format. In other words, the size of
// serialized inner slot using InnerSlotSerialize below.
static inline size_t InnerSlotSize(InnerSlot slot) {
  return sizeof(pgid_t) + slot.strict_upper_bound.size();
}
// Convert/Serialize the parsed inner slot to on-disk format and write it to
// the memory area starting with "addr".
void InnerSlotSerialize(char *addr, InnerSlot slot);

inline std::string_view gao1(InnerSlot slot)
{
  int len=InnerSlotSize(slot);
  char *str=new char[len];
  InnerSlotSerialize(str,slot);
  return std::string_view(str,len);
}

// Parsed leaf slot
struct LeafSlot {
  std::string_view key;
  std::string_view value;
};
// Parse the content of on-disk leaf slot
LeafSlot LeafSlotParse(std::string_view data);
// The size of the leaf slot in on-disk format. In other words, the size of
// serialized leaf slot using LeafSlotSerialize below.
static inline size_t LeafSlotSize(LeafSlot slot) {
  return sizeof(pgoff_t) + slot.key.size() + slot.value.size();
}
// Convert/Serialize the parsed leaf slot to on-disk format and write it to
// the memory area starting with "addr".
void LeafSlotSerialize(char *addr, LeafSlot slot);

inline std::string_view gao2(LeafSlot slot)
{
  int len=LeafSlotSize(slot);
  char *str=new char[len];
  LeafSlotSerialize(str,slot);
  return std::string_view(str,len);
}

template <typename Compare>
class BPlusTree {
   friend class Iter;
 private:
  using Self = BPlusTree<Compare>;
  class LeafSlotKeyCompare;
  class LeafSlotCompare;
  using LeafPage = SortedPage<LeafSlotKeyCompare, LeafSlotCompare>;
 public:
  class Iter {
   public:
    Iter(const Iter&) = delete;
    Iter& operator=(const Iter&) = delete;
    Iter(Iter&& iter) {
      //DB_ERR("Not implemented!");
      pgid_=iter.pgid_;mxid_=iter.mxid_;
      now=iter.now;now_mx=iter.now_mx;
      is_empty=iter.is_empty;
      hhh=iter.hhh;hh=iter.hh;
      pg_st=iter.pg_st;now_st=iter.now_st;
      iter.hhh=nullptr;iter.hh=nullptr;
      iter.pg_st=nullptr;iter.now_st=nullptr;
    }
    Iter() { is_empty=true; }
    Iter& operator=(Iter&& iter) {
      //DB_ERR("Not implemented!");
      pgid_=iter.pgid_;mxid_=iter.mxid_;
      now=iter.now;now_mx=iter.now_mx;
      is_empty=iter.is_empty;
      hhh=iter.hhh;hh=iter.hh;
      pg_st=iter.pg_st;now_st=iter.now_st;
      iter.hhh=nullptr;iter.hh=nullptr;
      iter.pg_st=nullptr;iter.now_st=nullptr;
      return *this;
    }
    inline LeafPage GetLeafPage(pgid_t pgid) { return (*hhh).get().GetSortedPage(pgid,LeafSlotKeyCompare((*hh)),LeafSlotCompare((*hh))); }
    inline pgid_t GetLeafNext(LeafPage& leaf) { return *(pgid_t*)leaf.ReadSpecial(sizeof(pgid_t),sizeof(pgid_t)).data(); }
    // Returns the current key-value pair that this iterator currently points
    // to. If this iterator does not point to any key-value pair, then return
    // std::nullopt. The first std::string_view is the key and the second
    // std::string_view is the value.
    std::optional<std::pair<std::string_view,std::string_view>> Cur() {
      if (is_empty) return std::nullopt;
      std::string_view str(pg_st+*(now_st+now),*(now_st+now-1)-*(now_st+now));
      LeafSlot slot=LeafSlotParse(str);
      return std::pair<std::string_view,std::string_view>(slot.key,slot.value);
    }
    void Next() {
      if (now<now_mx-1) now++;
      else
      {
        if (pgid_==mxid_) is_empty=true;
        else
        {
          pgid_=*((const pgid_t *)(pg_st+*(now_st-1))+1);
          auto leaf=GetLeafPage(pgid_);now=0;
          now_mx=leaf.SlotNum();pg_st=leaf.as_ptr();
          now_st=(const pgoff_t *)((const slotid_t *)pg_st+1)+1;
        }
      }
    }
    pgid_t pgid_,mxid_;slotid_t now,now_mx;
    const char *pg_st;
    const pgoff_t *now_st;
    bool is_empty;
    std::reference_wrapper<PageManager> *hhh;
    Compare *hh;
   private:
  };
  BPlusTree(const Self&)=delete;
  Self& operator=(const Self&)=delete;
  BPlusTree(Self&& rhs):pgm_(rhs.pgm_),meta_pgid_(rhs.meta_pgid_),comp_(rhs.comp_) {}
  Self& operator=(Self&& rhs) {
    pgm_=std::move(rhs.pgm_);
    meta_pgid_=rhs.meta_pgid_;
    comp_=rhs.comp_;
    return *this;
  }
  ~BPlusTree() {}
  static Self Create(std::reference_wrapper<PageManager> pgm) {
    Self ret(pgm,pgm.get().Allocate(),Compare());
    ret.UpdateTupleNum(0);
    return ret;
  }
  static Self Open(std::reference_wrapper<PageManager> pgm,pgid_t meta_pgid) { return Self(pgm,meta_pgid,Compare()); }
  inline pgid_t MetaPageID() const { return meta_pgid_; }
  void dfs(pgid_t x,uint8_t y)
  {
    if (!y)
    {
      auto now=GetLeafPage(x);
      FreePage(std::move(now));
      return;
    }
    auto now=GetInnerPage(x);
    for (slotid_t i=0;i<now.SlotNum();i++) dfs(InnerSlotParse(now.Slot(i)).next,y-1);
    dfs(GetInnerSpecial(now),y-1);
    FreePage(std::move(now));
  }
  void Destroy() {
    auto meta=GetMetaPage();
    if (!IsEmpty()) dfs(Root(),LevelNum());
    FreePage(std::move(meta));
  }
  inline bool IsEmpty() { return !TupleNum(); }
  bool work1(std::string_view key,std::string_view value,bool bo)
  {
    LeafSlot hh;hh.key=key;hh.value=value;
    std::string_view hhh=gao2(hh);
    if (IsEmpty())
    {
      if (bo) return false;
      auto now=AllocLeafPage();
      UpdateRoot(now.ID());UpdateLevelNum(0);
      now.AppendSlotUnchecked(hhh);
      IncreaseTupleNum(1);
      return true;
    }
    uint8_t level=LevelNum();
    pgid_t fa[level+1];fa[level]=Root();
    slotid_t id[level+1];
    for (uint8_t i=level;i;i--)
    {
      auto now=GetInnerPage(fa[i]);id[i]=now.UpperBound(key);
      if (id[i]<now.SlotNum()) fa[i-1]=InnerSlotParse(now.Slot(id[i])).next;
      else fa[i-1]=InnerLastPage(now);
    }
    auto now=GetLeafPage(fa[0]);
    id[0]=(!bo)?now.Find1(key):now.Find(key);
    if (!bo)
    {
      if (id[0]>now.SlotNum()) return false;
      IncreaseTupleNum(1);
      if (now.IsInsertable(hhh)) { now.InsertBeforeSlot(id[0],hhh);return true; }
    }
    else
    {
      if (id[0]==now.SlotNum()) return false;
      if (now.IsReplacable(id[0],hhh)) { now.ReplaceSlot(id[0],hhh);return true; }
    }
    auto right=AllocLeafPage();
    if (!bo) now.SplitInsert(right,id[0],hhh);
    else now.SplitReplace(right,id[0],hhh);
    std::string_view y=LeafSmallestKey(right);
    pgid_t Right=right.ID();
    bool flag=true;
    if (level!=0&&now.ID()!=LargestLeaf(GetInnerPage(Root()),level))
    {
      pgid_t nxt=GetLeafNext(now);auto NXT=GetLeafPage(nxt);
      SetLeafNext(right,nxt);SetLeafPrev(NXT,Right);
    }
    SetLeafNext(now,Right);SetLeafPrev(right,now.ID());
    for (uint8_t i=1;i<=level;i++)
    {
      auto now=GetInnerPage(fa[i]),right=AllocInnerPage();
      if (id[i]==now.SlotNum()) SetInnerSpecial(now,Right);
      else
      {
        InnerSlot h2=InnerSlotParse(now.Slot(id[i]));h2.next=Right;
        now.Replace(id[i],gao1(h2));
      }
      InnerSlot h1;h1.next=fa[i-1];h1.strict_upper_bound=y;
      now.SplitInsert(right,id[i],gao1(h1));
      if (right.SlotNum()==0) { flag=false;FreePage(std::move(right));break; }
      SetInnerSpecial(right,GetInnerSpecial(now));
      Right=InnerSlotParse(right.Slot(0)).next;
      SetInnerSpecial(now,Right);right.DeleteSlot(0);
      y=InnerSmallestKey(right,i);Right=right.ID();
    }
    if (flag)
    {
      UpdateLevelNum(level+1);
      auto Root=AllocInnerPage();
      InnerSlot h1;h1.next=fa[level];h1.strict_upper_bound=y;
      Root.AppendSlotUnchecked(gao1(h1));
      SetInnerSpecial(Root,Right);
      UpdateRoot(Root.ID());
    }
    return true;
  }
  inline bool Insert(std::string_view key,std::string_view value) { return work1(key,value,0); }
  inline bool Update(std::string_view key,std::string_view value) { return work1(key,value,1); }
  std::optional<std::string> MaxKey() {
    if (IsEmpty()) return std::nullopt;
    pgid_t Now=Root();
    if (LevelNum()) Now=LargestLeaf(GetInnerPage(Now),LevelNum());
    std::string_view str=LeafLargestKey(GetLeafPage(Now));
    return std::basic_string(str.data(),str.size());
  }
  std::optional<std::string> Get(std::string_view key) {
    if (IsEmpty()) return std::nullopt;
    pgid_t Now=Root();
    for (uint8_t i=LevelNum();i;i--)
    {
      auto now=GetInnerPage(Now);
      slotid_t id=now.UpperBound(key);
      if (id<now.SlotNum()) Now=InnerSlotParse(now.Slot(id)).next;
      else Now=InnerLastPage(now);
    }
    auto now=GetLeafPage(Now);
    slotid_t id=now.Find(key);
    if (id==now.SlotNum()) return std::nullopt;
    std::string_view str=LeafSlotParse(now.Slot(id)).value;
    return std::basic_string(str.data(),str.size());
  }
  #define pii std::pair<bool,std::optional<std::string> >
  pii work2(std::string_view key) {
    if (IsEmpty()) return pii(false,std::nullopt);
    uint8_t level=LevelNum();
    pgid_t fa[level+1],now=Root();
    slotid_t ID[level+1];
    for (uint8_t i=level;i;i--)
    {
      fa[i]=now;
      auto Now=GetInnerPage(now);
      slotid_t id=Now.UpperBound(key);ID[i]=id;
      if (id<Now.SlotNum()) now=InnerSlotParse(Now.Slot(id)).next;
      else now=InnerLastPage(Now);
    }
    fa[0]=now;
    auto Now=GetLeafPage(now);
    slotid_t id=Now.Find(key);
    if (id==Now.SlotNum()) return pii(false,std::nullopt);
    std::string_view str=LeafSlotParse(Now.Slot(id)).value;
    pii res(true,std::basic_string(str.data(),str.size()));
    Now.DeleteSlot(id);IncreaseTupleNum(-1);
    if (id>0) return res;
    std::string_view y;pgid_t Right=0;
    int flag=0;
    if (Now.SlotNum()==0) 
    {
      flag=1;
      if (level!=0&&now!=SmallestLeaf(GetInnerPage(Root()),level)&&now!=LargestLeaf(GetInnerPage(Root()),level))
      {
        pgid_t hh1=GetLeafPrev(Now),hh2=GetLeafNext(Now);
        auto h1=GetLeafPage(hh1),h2=GetLeafPage(hh2);
        SetLeafNext(h1,hh2);SetLeafPrev(h2,hh1);
      }
      FreePage(std::move(Now));
    }
    else y=LeafSmallestKey(Now);
    for (uint8_t i=1;i<=level;i++)
    {
      auto Now=GetInnerPage(fa[i]);slotid_t id=ID[i];
      if (flag==0)
      {
        if (id>0)
        {
          auto right=AllocInnerPage();
          InnerSlot h1=InnerSlotParse(Now.Slot(id-1));h1.strict_upper_bound=y;
          Now.SplitReplace(right,id-1,gao1(h1));
          if (right.SlotNum()==0) { FreePage(std::move(right));break; }
          else
          {
            flag=2;
            SetInnerSpecial(right,GetInnerSpecial(Now));
            Right=InnerSlotParse(right.Slot(0)).next;
            SetInnerSpecial(Now,Right);
            right.DeleteSlot(0);
            y=InnerSmallestKey(right,i);
            Right=right.ID();
          }
        }
      }
      else if (flag==1)
      {
        if (id<Now.SlotNum())
        {
          flag=0;
          if (id==0) Now.DeleteSlot(id);
          else
          {
            InnerSlot hh=InnerSlotParse(Now.Slot(id-1));
            hh.strict_upper_bound=InnerSlotParse(Now.Slot(id)).strict_upper_bound;
            std::string_view hhh=gao1(hh);
            Now.DeleteSlot(id);Now.DeleteSlot(id-1);
            Now.InsertBeforeSlot(id-1,hhh);
            break;
          }
          y=InnerSmallestKey(Now,i);
        }
        else
        {
          if (Now.SlotNum()==0) FreePage(std::move(Now));
          else
          {
            flag=0;
            slotid_t n=Now.SlotNum();
            pgid_t nxt=InnerSlotParse(Now.Slot(n-1)).next;
            Now.DeleteSlot(n-1);
            SetInnerSpecial(Now,nxt);
            y=InnerSmallestKey(Now,i);
          }
        }
      }
      else
      {
        auto right=AllocInnerPage();
        if (id==Now.SlotNum()) SetInnerSpecial(Now,Right);
        else
        {
          InnerSlot h2=InnerSlotParse(Now.Slot(id));h2.next=Right;
          Now.Replace(id,gao1(h2));
        }
        InnerSlot h1;
        h1.next=fa[i-1];h1.strict_upper_bound=y;
        Now.SplitInsert(right,id,gao1(h1));
        if (right.SlotNum()==0) { flag=0;FreePage(std::move(right));break; }
        SetInnerSpecial(right,GetInnerSpecial(Now));
        Right=InnerSlotParse(right.Slot(0)).next;
        SetInnerSpecial(Now,Right);
        right.DeleteSlot(0);
        y=InnerSmallestKey(right,i);
        Right=right.ID();
      }
    }
    if (flag==2)
    {
      UpdateLevelNum(level+1);
      auto Root=AllocInnerPage();
      InnerSlot h1;h1.next=fa[level];h1.strict_upper_bound=y;
      Root.AppendSlotUnchecked(gao1(h1));
      SetInnerSpecial(Root,Right);
      UpdateRoot(Root.ID());
    }
    return res;
  }
  inline bool Delete(std::string_view key) { return work2(key).first; }
  inline std::optional<std::string> Take(std::string_view key) { return work2(key).second; }
  Iter Begin() {
    Iter res;res.hhh=&pgm_;res.hh=&comp_;res.is_empty=true;
    if (IsEmpty()) return res;
    if (LevelNum()==0) res.mxid_=Root();
    else res.mxid_=LargestLeaf(GetInnerPage(Root()),LevelNum());
    res.is_empty=false;
    if (LevelNum()==0) res.pgid_=Root();
    else res.pgid_=SmallestLeaf(GetInnerPage(Root()),LevelNum());
    auto leaf=GetLeafPage(res.pgid_);
    res.now=0;res.now_mx=leaf.SlotNum();res.pg_st=leaf.as_ptr();
    res.now_st=(const pgoff_t*)((const slotid_t *)res.pg_st+1)+1;
    return res;
  }
  Iter LowerBound(std::string_view key) {
    Iter res;res.hhh=&pgm_;res.hh=&comp_;res.is_empty=true;
    if (IsEmpty()) return res;
    if (LevelNum()==0) res.mxid_=Root();
    else res.mxid_=LargestLeaf(GetInnerPage(Root()),LevelNum());
    pgid_t now=Root();
    for (uint8_t i=LevelNum();i;i--)
    {
      auto Now=GetInnerPage(now);
      slotid_t id=Now.UpperBound(key);
      if (id<Now.SlotNum()) now=InnerSlotParse(Now.Slot(id)).next;
      else now=InnerLastPage(Now);
    }
    auto Now=GetLeafPage(now);
    slotid_t id=Now.LowerBound(key);
    if (id==Now.SlotNum())
    {
      if (LevelNum()==0||now==LargestLeaf(GetInnerPage(Root()),LevelNum())) return res;
      res.is_empty=false;
      now=GetLeafNext(Now);Now=GetLeafPage(now);
      res.pgid_=now;res.now=0;
    }
    else
    {
      res.is_empty=false;
      res.pgid_=now;res.now=id;
    }
    res.now_mx=Now.SlotNum();res.pg_st=Now.as_ptr();
    res.now_st=(const pgoff_t*)((const slotid_t *)res.pg_st+1)+1;
    return res;
  }
  Iter UpperBound(std::string_view key) {
    Iter res;res.hhh=&pgm_;res.hh=&comp_;res.is_empty=true;
    if (IsEmpty()) return res;
    if (LevelNum()==0) res.mxid_=Root();
    else res.mxid_=LargestLeaf(GetInnerPage(Root()),LevelNum());
    pgid_t now=Root();
    for (uint8_t i=LevelNum();i;i--)
    {
      auto Now=GetInnerPage(now);
      slotid_t id=Now.UpperBound(key);
      if (id<Now.SlotNum()) now=InnerSlotParse(Now.Slot(id)).next;
      else now=InnerLastPage(Now);
    }
    auto Now=GetLeafPage(now);
    slotid_t id=Now.UpperBound(key);
    if (id==Now.SlotNum())
    {
      if (LevelNum()==0||now==LargestLeaf(GetInnerPage(Root()),LevelNum())) return res;
      res.is_empty=false;
      now=GetLeafNext(Now);Now=GetLeafPage(now);
      res.pgid_=now;res.now=0;
    }
    else
    {
      res.is_empty=false;
      res.pgid_=now;res.now=id;
    }
    res.now_mx=Now.SlotNum();res.pg_st=Now.as_ptr();
    res.now_st=(const pgoff_t*)((const slotid_t *)res.pg_st+1)+1;
    return res;
  }
  size_t TupleNum() { return *(pgid_t *)GetMetaPage().Read(8,sizeof(size_t)).data(); }
 private:
  // Here we provide some helper classes/functions that you may use.

  class InnerSlotKeyCompare {
  public:
    // slot: the content of the to-be-compared inner slot.
    std::weak_ordering operator()(
      std::string_view slot, std::string_view key
    ) const {
      return comp_(InnerSlotParse(slot).strict_upper_bound, key);
    }
  private:
    InnerSlotKeyCompare(const Compare& comp) : comp_(comp) {}
    Compare comp_;
    friend class BPlusTree;
  };
  class InnerSlotCompare {
  public:
    // a, b: the content of the two inner slots to be compared.
    std::weak_ordering operator()(
      std::string_view a, std::string_view b
    ) const {
      std::string_view a_key = InnerSlotParse(a).strict_upper_bound;
      std::string_view b_key = InnerSlotParse(b).strict_upper_bound;
      return comp_(a_key, b_key);
    }
  private:
    InnerSlotCompare(const Compare& comp) : comp_(comp) {}
    Compare comp_;
    friend class BPlusTree;
  };
  typedef SortedPage<InnerSlotKeyCompare, InnerSlotCompare> InnerPage;

  class LeafSlotKeyCompare {
  public:
    // slot: the content of the to-be-compared leaf slot.
    std::weak_ordering operator()(
      std::string_view slot, std::string_view key
    ) const {
      return comp_(LeafSlotParse(slot).key, key);
    }
  private:
    LeafSlotKeyCompare(const Compare& comp) : comp_(comp) {}
    Compare comp_;
    friend class BPlusTree;
  };
  class LeafSlotCompare {
  public:
    // a, b: the content of two to-be-compared leaf slots.
    std::weak_ordering operator()(
      std::string_view a, std::string_view b
    ) const {
      return comp_(LeafSlotParse(a).key, LeafSlotParse(b).key);
    }
  private:
    LeafSlotCompare(const Compare& comp) : comp_(comp) {}
    Compare comp_;
    friend class BPlusTree;
  };

  BPlusTree(std::reference_wrapper<PageManager> pgm, pgid_t meta_pgid,
      const Compare& comp)
    : pgm_(pgm), meta_pgid_(meta_pgid), comp_(comp) {}

  // Reference the inner page and return a handle for it.
  inline InnerPage GetInnerPage(pgid_t pgid) {
    return pgm_.get().GetSortedPage(pgid, InnerSlotKeyCompare(comp_),
        InnerSlotCompare(comp_));
  }
  // Reference the leaf page and return a handle for it.
  inline LeafPage GetLeafPage(pgid_t pgid) {
    return pgm_.get().GetSortedPage(pgid, LeafSlotKeyCompare(comp_),
        LeafSlotCompare(comp_));
  }
  // Reference the meta page and return a handle for it.
  inline PlainPage GetMetaPage() {
    return pgm_.get().GetPlainPage(meta_pgid_);
  }

  /* PageManager::Free requires that the page is not being referenced.
   * So we have to first explicitly drop the page handle which should be the
   * only reference to the page before calling PageManager::Free to free the
   * page.
   *
   * For example, if you have a page handle "inner1" that is the only reference
   * to the underlying page, and you want to free this page on disk:
   *
   * InnerPage inner1 = GetInnerPage(pgid);
   * // This is wrong! "inner1" is still referencing this page!
   * pgm_.get().Free(inner1.ID());
   * // This is correct, because "FreePage" will drop the page handle "inner1"
   * // and thus drop the only reference to the underlying page, so that the
   * // underlying page can be safely freed with PageManager::Free.
   * FreePage(std::move(inner1));
   */
  inline void FreePage(Page&& page) {
    pgid_t id = page.ID();
    page.Drop();
    pgm_.get().Free(id);
  }

  // Allocate an inner page and return a handle that references it.
  inline InnerPage AllocInnerPage() {
    auto inner = pgm_.get().AllocSortedPage(InnerSlotKeyCompare(comp_),
        InnerSlotCompare(comp_));
    inner.Init(sizeof(pgid_t));
    return inner;
  }
  // Allocate a leaf page and return a handle that references it.
  inline LeafPage AllocLeafPage() {
    auto leaf = pgm_.get().AllocSortedPage(LeafSlotKeyCompare(comp_),
        LeafSlotCompare(comp_));
    leaf.Init(sizeof(pgid_t) * 2);
    return leaf;
  }

  // Get the right-most child
  inline pgid_t GetInnerSpecial(const InnerPage& inner) {
    return *(pgid_t *)inner.ReadSpecial(0, sizeof(pgid_t)).data();
  }
  // Set the right-most child
  inline void SetInnerSpecial(InnerPage& inner, pgid_t page) {
    inner.WriteSpecial(0, std::string_view((char *)&page, sizeof(page)));
  }
  inline pgid_t GetLeafPrev(LeafPage& leaf) {
    return *(pgid_t *)leaf.ReadSpecial(0, sizeof(pgid_t)).data();
  }
  inline void SetLeafPrev(LeafPage& leaf, pgid_t pgid) {
    leaf.WriteSpecial(0, std::string_view((char *)&pgid, sizeof(pgid)));
  }
  inline pgid_t GetLeafNext(LeafPage& leaf) {
    return *(pgid_t *)leaf.ReadSpecial(sizeof(pgid_t), sizeof(pgid_t)).data();
  }
  inline void SetLeafNext(LeafPage& leaf, pgid_t pgid) {
    std::string_view data((char *)&pgid, sizeof(pgid));
    leaf.WriteSpecial(sizeof(pgid_t), data);
  }

  inline uint8_t LevelNum() {
    return GetMetaPage().Read(0, 1)[0];
  }
  inline void UpdateLevelNum(uint8_t level_num) {
    GetMetaPage().Write(0,
      std::string_view((char *)&level_num, sizeof(level_num)));
  }
  inline pgid_t Root() {
    return *(pgid_t *)GetMetaPage().Read(4, sizeof(pgid_t)).data();
  }
  inline void UpdateRoot(pgid_t root) {
    GetMetaPage().Write(4, std::string_view((char *)&root, sizeof(root)));
  }
  inline void UpdateTupleNum(size_t num) {
    static_assert(sizeof(size_t) == 8);
    GetMetaPage().Write(8, std::string_view((char *)&num, sizeof(num)));
  }
  inline void IncreaseTupleNum(ssize_t delta) {
    size_t tuple_num = TupleNum();
    if (delta < 0)
      assert(tuple_num >= (size_t)(-delta));
    UpdateTupleNum(tuple_num + delta);
  }

  inline std::string_view LeafSmallestKey(const LeafPage& leaf) {
    assert(leaf.SlotNum() > 0);
    LeafSlot slot = LeafSlotParse(leaf.Slot(0));
    return slot.key;
  }
  inline std::string_view LeafLargestKey(const LeafPage& leaf) {
    assert(leaf.SlotNum() > 0);
    LeafSlot slot = LeafSlotParse(leaf.Slot(leaf.SlotNum() - 1));
    return slot.key;
  }

  pgid_t InnerFirstPage(const InnerPage& inner) {
    if (inner.IsEmpty())
      return GetInnerSpecial(inner);
    InnerSlot slot = InnerSlotParse(inner.Slot(0));
    return slot.next;
  }
  pgid_t InnerLastPage(const InnerPage& inner) {
     return GetInnerSpecial(inner);
  }

  pgid_t SmallestLeaf(const InnerPage& inner, uint8_t level) {
    assert(level > 0);
    pgid_t cur = InnerFirstPage(inner);
    while (--level)
      cur = InnerFirstPage(GetInnerPage(cur));
    return cur;
  }
  pgid_t LargestLeaf(const InnerPage& inner, uint8_t level) {
    assert(level > 0);
    pgid_t cur = InnerLastPage(inner);
    while (--level)
      cur = InnerLastPage(GetInnerPage(cur));
    return cur;
  }

  std::string_view InnerSmallestKey(const InnerPage& inner, uint8_t level) {
    return LeafSmallestKey(GetLeafPage(SmallestLeaf(inner, level)));
  }
  std::string_view InnerLargestKey(const InnerPage& inner, uint8_t level) {
    assert(level > 0);
    pgid_t cur = GetInnerSpecial(inner);
    level -= 1;
    while (level > 0) {
      InnerPage inner = GetInnerPage(cur);
      cur = GetInnerSpecial(inner);
      level -= 1;
    }
    return LeafLargestKey(GetLeafPage(cur));
  }

  // For Debugging
  void LeafPrint(std::ostream& out, const LeafPage& leaf,
      size_t (*key_printer)(std::ostream& out, std::string_view),
      size_t (*val_printer)(std::ostream& out, std::string_view)) {
    for (slotid_t i = 0; i < leaf.SlotNum(); ++i) {
      LeafSlot slot = LeafSlotParse(leaf.Slot(i));
      out << '(';
      key_printer(out, slot.key);
      out << ',';
      val_printer(out, slot.value);
      out << ')';
    }
  }
  void InnerPrint(std::ostream& out, InnerPage& inner, uint8_t level,
      size_t (*key_printer)(std::ostream& out, std::string_view)) {
    out << "{smallest:" << key_formatter(InnerSmallestKey(inner, level)) << ","
      "separators:[";
    for (slotid_t i = 0; i < inner.SlotNum(); ++i) {
      InnerSlot slot = InnerSlotParse(inner.Slot(i));
      key_printer(out, slot.strict_upper_bound);
      out << ',';
    }
    out << "],largest:" << key_formatter(InnerLargestKey(inner, level)) << '}';
  }
  void PrintSubtree(std::ostream& out, std::string& prefix, pgid_t pgid,
      uint8_t level, size_t (*key_printer)(std::ostream& out, std::string_view)) {
    if (level == 0) {
      LeafPage leaf = GetLeafPage(pgid);
      if (leaf.IsEmpty()) {
        out << "{Empty}";
      } else {
        out << "{smallest:";
        key_printer(out, LeafSmallestKey(leaf));
        out << ",largest:";
        key_printer(out, LeafLargestKey(leaf));
        out << "}" << std::endl;
      }
      return;
    }
    InnerPage inner = GetInnerPage(pgid);
    size_t len = 0; // Suppress maybe unitialized warning
    slotid_t slot_num = inner.SlotNum();
    for (slotid_t i = 0; i < slot_num; ++i) {
      InnerSlot slot = InnerSlotParse(inner.Slot(i));
      if (i > 0)
        out << prefix;
      len = key_printer(out, slot.strict_upper_bound);
      out << '-';
      prefix.push_back('|');
      prefix.append(len, ' ');
      PrintSubtree(out, prefix, slot.next, level - 1, key_printer);
      prefix.resize(prefix.size() - len - 1);
    }
    out << prefix << '|';
    for (size_t i = 0; i < len; ++i)
      out << '-';
    prefix.append(len + 1, ' ');
    PrintSubtree(out, prefix, GetInnerSpecial(inner), level - 1, key_printer);
    prefix.resize(prefix.size() - len - 1);
  }
  /* Print the tree structure.
   * out: the target stream that will be printed to.
   * key_printer: print the key to the given stream, return the number of
   *  printed characters.
   */
  void Print(std::ostream& out,
      size_t (*key_printer)(std::ostream& out, std::string_view)) {
    std::string prefix;
    PrintSubtree(out, prefix, Root(), LevelNum(), key_printer);
  }
  // Some predefined key/value printers
  static size_t printer_str(std::ostream& out, std::string_view s) {
    out << s;
    return s.size();
  }
  static char to_oct(uint8_t c) {
    return c + '0';
  }
  static size_t printer_oct(std::ostream& out, std::string_view s) {
    for (uint8_t c : s)
      out << '\\' << to_oct(c >> 6) << to_oct((c >> 3) & 7) << to_oct(c & 7);
    return s.size() * 4;
  }
  static char to_hex(uint8_t c) {
    assert(0 <= c && c <= 15);
    if (0 <= c && c <= 9) {
      return c + '0';
    } else {
      return c - 10 + 'a';
    }
  }
  static size_t printer_hex(std::ostream& out, std::string_view s) {
    std::string str = fmt::format("({})", s.size());
    size_t printed = str.size();
    out << str;
    for (uint8_t c : s)
      out << to_hex(c >> 4) << to_hex(c & 15);
    return printed + s.size() * 2;
  }
  static size_t printer_mock(std::ostream&, std::string_view) { return 0; }

  std::reference_wrapper<PageManager> pgm_;
  pgid_t meta_pgid_;
  Compare comp_;
};

}

#endif	//BPLUS_TREE_H_