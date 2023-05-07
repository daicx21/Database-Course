#include "bplus-tree.hpp"

namespace wing {

InnerSlot InnerSlotParse(std::string_view data) {
	InnerSlot res;
	pgoff_t keylen=data.size()-sizeof(pgid_t);
	res.next=*(pgid_t*)data.data();
	res.strict_upper_bound=std::string_view(data.data()+sizeof(pgid_t),keylen);
	return res;
}
void InnerSlotSerialize(char *s, InnerSlot slot) {
	*(pgid_t*)s=slot.next;
	memcpy(s+sizeof(pgid_t),slot.strict_upper_bound.data(),slot.strict_upper_bound.size());
}

LeafSlot LeafSlotParse(std::string_view data) {
	LeafSlot res;
	pgoff_t keylen=*(pgoff_t*)data.data(),valuelen=data.size()-sizeof(pgoff_t)-keylen;
	res.key=std::string_view(data.data()+sizeof(pgoff_t),keylen);
	res.value=std::string_view(data.data()+sizeof(pgoff_t)+keylen,valuelen);
	return res;
}
void LeafSlotSerialize(char *s, LeafSlot slot) {
	pgoff_t keylen=slot.key.size();
	*(pgoff_t*)s=keylen;
	memcpy(s+sizeof(pgoff_t),slot.key.data(),slot.key.size());
	memcpy(s+sizeof(pgoff_t)+keylen,slot.value.data(),slot.value.size());
}

}
