#ifndef PAGE_MANAGER_H_
#define PAGE_MANAGER_H_

#include "common/error.hpp"
#include "common/logging.hpp"

#include<iostream>
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <list>
#include <mutex>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

namespace wing {

class PageManager;

typedef uint32_t pgid_t;
typedef uint16_t pgoff_t;
typedef int16_t signed_pgoff_t;
typedef uint16_t slotid_t;
// A page handle that references a page buffer. This is not expected to be used
// directly by the user. The user should use PlainPage or SortedPage instead,
// which are derived classes of this class.
class Page {
public:
  static constexpr std::size_t SIZE = 4096;
  Page(const Page&) = delete;
  Page& operator=(const Page&) = delete;
  Page(Page&& page) : Page(page.id_, page.page_, page.pgm_, page.dirty_) {
    page.id_ = 0;
    page.page_ = nullptr;
  }
  Page& operator=(Page&& page) {
    __Drop();
    id_ = page.id_;
    page_ = page.page_;
    pgm_ = page.pgm_;
    dirty_ = page.dirty_;
    page.id_ = 0;
    return *this;
  }
  ~Page();
  inline pgid_t ID() const { return id_; }
  inline const char *as_ptr() const { return page_; }
  inline void MarkDirty() { dirty_=true; }
  inline void Drop();
protected:
  Page(pgid_t id,char *page,std::reference_wrapper<PageManager> pgm,bool dirty):id_(id),page_(page),pgm_(pgm),dirty_(dirty) {}
  inline pgoff_t Offset(void *addr) { return (pgoff_t)((char *)addr-page_); }
  inline void __Drop();
  pgid_t id_;
  char *page_;
  std::reference_wrapper<PageManager> pgm_;
  bool dirty_;
  friend class PageManager;
};

class PlainPage : public Page {
public:
  PlainPage(const PlainPage&) = delete;
  PlainPage& operator=(const PlainPage&) = delete;
  PlainPage(Page&& page) : Page(std::move(page)) {}
  PlainPage(PlainPage&& page) : Page(std::move(page)) {}
  PlainPage& operator=(PlainPage&& page) {
    Page::operator=(std::move(page));
    return *this;
  }
  inline std::string_view Read(pgoff_t start, pgoff_t len) const {
    return std::string_view(page_ + start, len);
  }
  inline void Read(void *buf, pgoff_t start, pgoff_t len) const {
    memcpy(buf, page_ + start, len);
  }
  inline void Write(pgoff_t start, std::string_view data) {
    MarkDirty();
    memcpy(page_ + start, data.data(), data.size());
  }
private:
  friend class PageManager;
};

/* The handle that references a page buffer whose format is SortedPage.
 * All tuples are sorted in SortedPage. Layout:
 * +--------+-----------------------------------------------------+
 * | N (2B) | special (2B)   start_0 (2B)  start_1 (2B)     ...   |
 * +--------+---------+-------------------------+-----------------+
 * | ... start_{N-1}  |       Free space        | tuple_{N-1} ... |
 * +----------+-------+---+----------------+----+-----------------+
 * | ... | tuple_1        | tuple_0        | "special space"      |
 * +----------------------+----------------+----------------------+
 *       ^ start_1        ^ start_0        ^ special
 * 
 * For example, suppose the page size is 4096, the special size is 4,
 * the format of a tuple is (key, value), each tuple is of size 8,
 * and keys are compared as integers.
 * If I want to store (2, 3), (4, 5), (114, 514) in this page,
 * then the page layout is:
 * 
 * | 3 | 4092 | 4084 | 4076 | 4068 | (free space) | (114, 514) | (4, 5) | (2, 3) | special space |
 *
 * The comparison between a slot and a key is performed by SlotKeyCompare,
 * and the comparison between two slots is performed by SlotCompare.
 * The parse of slots is performed in SlotKeyCompare and SlotCompare,
 * so we don't have to care about the content of slots in this class.
 */
template <typename SlotKeyCompare,typename SlotCompare>
class SortedPage:public Page
{
public:
  SortedPage(Page&& page, const SlotKeyCompare& slot_key_comp, const SlotCompare& slot_comp):Page(std::move(page)),slot_key_comp_(slot_key_comp),slot_comp_(slot_comp) {}
  SortedPage(const SortedPage&)=delete;
  SortedPage& operator=(const SortedPage&)=delete;
  SortedPage(SortedPage&& page): Page(std::move(page)),slot_key_comp_(page.slot_key_comp_),slot_comp_(page.slot_comp_) {}
  SortedPage& operator=(SortedPage&& page) {
    Page::operator=(std::move(page));
    return *this;
  }
  inline void Init(std::size_t special_size) {
    MarkDirty();
    slotid_t *num=(slotid_t *)page_;*num=0;
    pgoff_t *special=(pgoff_t *)(num+1);*special=SIZE-special_size;
  }
  inline slotid_t SlotNum() const { return *(const slotid_t *)page_; }
  inline slotid_t& SlotNumMut() {
    MarkDirty();
    return *(slotid_t *)page_;
  }
  inline bool IsEmpty() const { return (SlotNum()==0); }
  inline const char *SlotRaw(slotid_t slot) const {
    assert(slot<SlotNum());
    return page_+Starts()[slot];
  }
  inline char *SlotRawMut(slotid_t slot) {
    MarkDirty();
    assert(slot<SlotNum());
    return page_+StartsMut()[slot];
  }
  inline std::string_view Slot(slotid_t slot) const {
    assert(slot<SlotNum());
    return std::string_view(SlotRaw(slot),SlotSize(slot));
  }
  inline std::string_view ReadSpecial(pgoff_t start,pgoff_t len) const { return std::string_view(Special()+start,len); }
  inline void WriteSpecial(pgoff_t start,std::string_view data) {
    MarkDirty();
    memcpy(SpecialMut()+start,data.data(),data.size());
  }
  inline bool IsInsertable(std::string_view slot) const { return (FreeSpace()>=slot.size()+sizeof(pgoff_t)); }
  inline bool IsReplacable(slotid_t slotid,std::string_view slot) const { return (FreeSpace()+SlotSize(slotid)>=slot.size()); }
  slotid_t gao1(slotid_t l, slotid_t r,std::string_view d) const {
    while (l!=r)
    {
      slotid_t mid=(l+r)/2;
      if (slot_key_comp_(Slot(mid),d)==std::weak_ordering::less) l=mid+1;
      else r=mid;
    }
    return l;
  }
  slotid_t gao2(slotid_t l,slotid_t r,std::string_view d) const {
    while (l!=r)
    {
      slotid_t mid=(l+r)/2;
      if (slot_key_comp_(Slot(mid),d)==std::weak_ordering::greater) r=mid;
      else l=mid+1;
    }
    return l;
  }
  slotid_t LowerBound(std::string_view key) const { return gao1(0,SlotNum(),key); }
  slotid_t UpperBound(std::string_view key) const { return gao2(0,SlotNum(),key); }
  slotid_t Find(std::string_view key) const {
    slotid_t n=SlotNum(),h1=gao1(0,n,key);
    if (h1<n&&slot_key_comp_(Slot(h1),key)==std::weak_ordering::equivalent) return h1;
    return n;
  }
  slotid_t Find1(std::string_view key) const {
    slotid_t n=SlotNum(),h1=gao1(0,n,key);
    if (h1<n&&slot_key_comp_(Slot(h1),key)==std::weak_ordering::equivalent) return n+1;
    return h1;
  }
  std::optional<std::string_view> FindSlot(std::string_view key) const {
    slotid_t n=SlotNum();
    slotid_t h1=gao1(0,n,key);
    if (h1<n&&slot_key_comp_(Slot(h1),key)==std::weak_ordering::equivalent) return Slot(h1);
    return std::nullopt;
  }
  void AppendSlotUnchecked(std::string_view slot) {
    MarkDirty();
    pgoff_t tail_offset=Ends()[SlotNum()];
    pgoff_t start=tail_offset-slot.size();
    memcpy(page_+start,slot.data(),slot.size());
    StartsMut()[SlotNum()]=start;
    SlotNumMut()+=1;
  }
  inline bool InsertBeforeSlot(slotid_t slotid,std::string_view slot)
  {
    if (!IsInsertable(slot)) return false;
    MarkDirty();
    slotid_t n=SlotNum();
    pgoff_t h1=Starts()[n-1],h2=Ends()[slotid];
    memmove(page_+h1-slot.size(),page_+h1,h2-h1);
    for (slotid_t j=1;j<=n-slotid;j++)
    {
      slotid_t i=n-j;
      StartsMut()[i+1]=Starts()[i]-slot.size();
    }
    h1=h2-slot.size();
    memcpy(page_+h1,slot.data(),h2-h1);
    StartsMut()[slotid]=h1;SlotNumMut()+=1;
    return true;
  }
  bool SplitInsert(SortedPage<SlotKeyCompare,SlotCompare>& right,slotid_t slotid,std::string_view slot) {
    MarkDirty();
    if (IsInsertable(slot)) return InsertBeforeSlot(slotid,slot);
    slotid_t n=SlotNum();
    if (Ends()[slotid]-sizeof(slotid_t)-sizeof(pgoff_t)-sizeof(pgoff_t)*slotid>=sizeof(pgoff_t)+slot.size())
    {
      for (slotid_t i=slotid;i<n;i++) right.AppendSlotUnchecked(Slot(i));
      SlotNumMut()=slotid+1;
      StartsMut()[slotid]=Ends()[slotid]-slot.size();
      memcpy(page_+Starts()[slotid],slot.data(),slot.size());
    }
    else
    {
      right.AppendSlotUnchecked(slot);
      for (slotid_t i=slotid;i<n;i++) right.AppendSlotUnchecked(Slot(i));
      SlotNumMut()=slotid;
    }
    return true;
  }
  bool Replace(slotid_t slotid,std::string_view slot)
  {
    MarkDirty();
    memcpy(page_+Starts()[slotid],slot.data(),slot.size());
    return true;
  }
  bool ReplaceSlot(slotid_t slotid,std::string_view slot)
  {
    MarkDirty();
    slotid_t n=SlotNum();
    pgoff_t h1=Starts()[n-1],h2=Starts()[slotid];
    signed_pgoff_t hh=(Ends()[slotid]-h2)-slot.size();
    memmove(page_+h1+hh,page_+h1,h2-h1);
    for (slotid_t i=slotid;i<n;i++) StartsMut()[i]+=hh;
    memcpy(page_+Starts()[slotid],slot.data(),slot.size());
    return true;
  }
  bool SplitReplace(SortedPage<SlotKeyCompare,SlotCompare>& right,slotid_t slotid,std::string_view slot) {
    MarkDirty();
    if (IsReplacable(slotid,slot)) return ReplaceSlot(slotid,slot);
    slotid_t n=SlotNum();
    if (Ends()[slotid]-sizeof(slotid_t)-sizeof(pgoff_t)-sizeof(pgoff_t)*slotid>=sizeof(pgoff_t)+slot.size())
    {
      for (slotid_t i=slotid+1;i<n;i++) right.AppendSlotUnchecked(Slot(i));
      SlotNumMut()=slotid+1;
      StartsMut()[slotid]=Ends()[slotid]-slot.size();
      memcpy(page_+Starts()[slotid],slot.data(),slot.size());
    }
    else
    {
      right.AppendSlotUnchecked(slot);
      for (slotid_t i=slotid+1;i<n;i++) right.AppendSlotUnchecked(Slot(i));
      SlotNumMut()=slotid;
    }
    return true;
  }
  void DeleteSlot(slotid_t slotid) {
    MarkDirty();
    slotid_t n=SlotNum();
    pgoff_t h1=Starts()[n-1],h2=Starts()[slotid],hh=Ends()[slotid]-h2;
    memmove(page_+h1+hh,page_+h1,h2-h1);
    for (slotid_t i=slotid;i<n-1;i++) StartsMut()[i]=Starts()[i+1]+hh;
    SlotNumMut()-=1;
  }

private:
  class ComparePageOffKey {
  public:
    std::weak_ordering operator()(const pgoff_t *start,std::string_view key) const {
      size_t len=*(start-1)-*start;
      std::string_view slot(page_+*start,len);
      return slot_key_comp_(slot,key);
    }
  private:
    ComparePageOffKey(const char *page,SlotKeyCompare slot_key_comp):page_(page),slot_key_comp_(slot_key_comp) {}
    const char *page_;
    SlotKeyCompare slot_key_comp_;
    friend class SortedPage;
  };
  class ComparePageOffSlot {
  public:
    std::weak_ordering operator()(const pgoff_t *start,std::string_view slot) const {
      size_t len=*(start-1)-*start;
      std::string_view cur_slot(page_+*start,len);
      return slot_comp_(cur_slot,slot);
    }
  private:
    ComparePageOffSlot(const char *page, SlotCompare slot_comp)
      :	page_(page), slot_comp_(slot_comp) {}
    const char *page_;
    SlotCompare slot_comp_;
    friend class SortedPage;
  };
  inline const char *Special() const { return page_+Ends()[0]; }
  inline char *SpecialMut() {
    MarkDirty();
    return page_ + EndsMut()[0];
  }
  inline const pgoff_t *Ends() const {
    const pgoff_t *special=(const pgoff_t*)((const slotid_t *)page_+1);
    return special;
  }
  inline pgoff_t *EndsMut() {
    MarkDirty();
    slotid_t *num = (slotid_t *)page_;
    pgoff_t *special = (pgoff_t *)(num + 1);
    return special;
  }
  inline const pgoff_t *Starts() const { return Ends()+1; }
  inline pgoff_t *StartsMut() {
    MarkDirty();
    return EndsMut()+1;
  }
  inline pgoff_t SlotSize(slotid_t slot) const {
    const pgoff_t *ends=Ends();
    const pgoff_t *starts=ends+1;
    return ends[slot]-starts[slot];
  }
  inline pgoff_t SlotSpace(slotid_t slot) const { return SlotSize(slot)+sizeof(pgoff_t); }
  inline pgoff_t SlotsSize(slotid_t start,slotid_t end) const { return Ends()[start]-Ends()[end]; }
  inline pgoff_t SlotsSpace(slotid_t start,slotid_t end) const { return SlotsSize(start,end)+(end-start)*sizeof(pgoff_t); }
  // Return the size of free space in this page.
  inline pgoff_t FreeSpace() const {
    slotid_t num=SlotNum();
    const pgoff_t *ends=Ends();
    return ends[num]-sizeof(slotid_t)-sizeof(pgoff_t)-sizeof(pgoff_t)*num;
  }

  SlotKeyCompare slot_key_comp_;
  SlotCompare slot_comp_;
  friend class PageManager;
};

// Evict the page that has been unpinned for the longest time.
class EvictionPolicy {
public:
  pgid_t Evict() {
    if (evictable_.empty())
      DB_ERR("Buffer size for PageManager is too small!");
    pgid_t ret = evictable_.front();
    evictable_.pop_front();
    size_t erased = its_.erase(ret);
    (void)erased;
    assert(erased == 1);
    return ret;
  }
  void Pin(pgid_t pgid) {
    auto it = its_.find(pgid);
    assert(it != its_.end());
    evictable_.erase(it->second);
    its_.erase(it);
  }
  void Unpin(pgid_t pgid) {
    evictable_.push_back(pgid);
    auto it = evictable_.end();
    --it;
    auto ret = its_.emplace(pgid, it);
    (void)ret;
    assert(ret.second);
  }
  void Remove(pgid_t pgid) {
    auto it = its_.find(pgid);
    if (it == its_.end())
      return;
    evictable_.erase(it->second);
    its_.erase(it);
  }
private:
  std::unordered_map<pgid_t, std::list<pgid_t>::iterator> its_;
  std::list<pgid_t> evictable_;
};

/* Page 0: The meta page of PageManager.
 * Page 1: The pre-allocated super page for user. BPlusTreeStorage stores
 *  metadata (e.g., the meta page of B+tree) here.
 *
 * Similar to pages in operating systems, the buffer of pages in wing are also
 * allocated lazily. When the user calls PageManager::Allocate() to allocate a
 * page ID, there is no page buffer allocated for it. When the user try to get a
 * SortedPage or PlainPage handle for it, the page manager will check whether
 * there is a page buffer for it. If no found, then a page buffer will be
 * allocated and assigned to the page with its reference count set to 1.
 * If found, then the reference count of the page buffer will be increased by 1. 
 * The returned handle will reference the page buffer assigned to the page ID.
 * When the returned handle destructs or being dropped, the reference count of
 * the page buffer will be decreased by 1.
 * 
 * If the reference count of a page bufer > 0, then it will be pinned in the
 * buffer pool. If the reference count the a page buffer is decreased to 0,
 * then it is unpinned and becomes evictable. However, when it will acutally be
 * evicted depends on the eviction policy. When a page is evicted from the
 * buffer pool, if it is marked dirty with Page::MarkDirty(), it will be flushed
 * to disk.
 */
class PageManager {
public:
  PageManager(const PageManager&) = delete;
  PageManager& operator=(const PageManager&) = delete;
  PageManager(PageManager&& pgm) = delete;
  PageManager& operator=(PageManager&&) = delete;
  ~PageManager();
  static auto Create(
    std::filesystem::path path, size_t max_buf_pages
  ) -> std::unique_ptr<PageManager>;
  static auto Open(
    std::filesystem::path path, size_t max_buf_pages
  ) -> Result<std::unique_ptr<PageManager>, io::Error>;
  /* Allocate a page ID. You may use GetSortedPage or GetPlainPage later on
   * this page ID to get a handle for this page. Note that SortedPage should be
   * initialized with SortedPage::Init before using it for the first time.
   */
  pgid_t Allocate();
  // Free the page ID. You have to make sure that there is no SortedPage or
  // PlainPage handle that is still referencing this page.
  void Free(pgid_t pgid);
  // Return the ID of the pre-allocated super page. This is intended to be used
  // by BPlusTreeStorage to store metadata.
  pgid_t SuperPageID() { return 1; }
  // Regard the page as PlainPage and return a handle that references its
  // buffer.
  PlainPage GetPlainPage(pgid_t pgid) {
    std::lock_guard l(latch_);
    return PlainPage(GetPage(pgid));
  }
  // Regard the page as SortedPage and return a handle that references its
  // buffer.
  template <typename SlotKeyCompare, typename SlotCompare>
  auto GetSortedPage(pgid_t pgid, const SlotKeyCompare& slot_key_comp,
    const SlotCompare& slot_comp
  ) -> SortedPage<SlotKeyCompare, SlotCompare> {
    std::lock_guard l(latch_);
    return SortedPage<SlotKeyCompare, SlotCompare>(
      GetPage(pgid), slot_key_comp, slot_comp);
  }

  // Allocate a page ID, allocate a page buffer for it, and return a
  // PlainPage handle that references the buffer.
  [[maybe_unused]]
  PlainPage AllocPlainPage() { return GetPlainPage(Allocate()); }
  /* Allocate a page ID, allocate a page buffer for it, and return the
   * SortedPage handle. The user should call SortedPage::Init before using
   * it for the first time.
   */
  template <typename SlotKeyCompare, typename SlotCompare>
  auto AllocSortedPage(
    const SlotKeyCompare& slot_key_comp, const SlotCompare& slot_comp
  ) -> SortedPage<SlotKeyCompare, SlotCompare> {
    auto page = GetSortedPage(Allocate(), slot_key_comp, slot_comp);
    return page;
  }

  // Made public for test
  inline pgid_t& PageNum() {
    return *(pgid_t *)(buf_[0].addr_mut() + PAGE_NUM_OFF);
  }
  // For test
  void ShrinkToFit();
private:
  struct PageBufInfo {
    const char *addr() const {
      return reinterpret_cast<const char *>(buf.get());
    }
    char *addr_mut() { return reinterpret_cast<char *>(buf.get()); }
    std::unique_ptr<char[]> buf;
    size_t refcount;
    bool dirty;
  };
  PageManager(std::filesystem::path path, std::fstream&& file,
      size_t max_buf_pages)
    : path_(path),
      file_(std::move(file)),
      max_buf_pages_(max_buf_pages),
      free_list_buf_(free_list_bufs_[0]),
      free_list_buf_used_(0),
      free_list_buf_standby_(free_list_bufs_[1]),
      free_list_buf_standby_full_(false) {
    // One buffer page is for pinned meta page.
    assert(max_buf_pages_ >= 2);
  }
  static constexpr pgoff_t PGID_PER_PAGE = Page::SIZE / sizeof(pgid_t) - 1;
  static constexpr pgoff_t FREE_LIST_HEAD_OFF = 0;
  static constexpr pgoff_t FREE_PAGES_IN_HEAD = FREE_LIST_HEAD_OFF + sizeof(pgid_t);
  static constexpr pgoff_t PAGE_NUM_OFF = FREE_PAGES_IN_HEAD + sizeof(pgid_t);
  inline pgid_t& FreeListHead() {
    return *(pgid_t *)(buf_[0].addr_mut() + FREE_LIST_HEAD_OFF);
  }
  inline pgid_t& FreePagesInHead() {
    return *(pgid_t *)(buf_[0].addr_mut() + FREE_PAGES_IN_HEAD);
  }

  pgid_t __Allocate();

  void AllocMeta();
  void Init();
  std::optional<io::Error> Load();
  Page GetPage(pgid_t pgid);
  void DropPage(pgid_t pgid, bool dirty);
  void FlushFreeListStandby(pgid_t pgid);

  std::filesystem::path path_;
  std::fstream file_;
  size_t max_buf_pages_;
  pgid_t *free_list_buf_;
  size_t free_list_buf_used_;
  // The standby buffer is either full or empty.
  pgid_t *free_list_buf_standby_;
  bool free_list_buf_standby_full_;
  std::unordered_map<pgid_t, PageBufInfo> buf_;
  EvictionPolicy eviction_policy_;
  pgid_t free_list_bufs_[2][PGID_PER_PAGE];

  // For debugging
  std::vector<bool> is_free_;
  
  std::mutex latch_;

  friend class Page;
};

inline Page::~Page() {
  __Drop();
}

inline void Page::__Drop() {
  if (id_ == 0)
    return;
  pgm_.get().DropPage(id_, dirty_);
}

inline void Page::Drop() {
  __Drop();
  id_ = 0;
}

}

#endif	//PAGE-MANAGER_H_