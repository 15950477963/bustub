//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <list>
#include <unordered_map>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  avlbPages = 0;
}

LRUReplacer::~LRUReplacer() = default;

// 返回一个不经常访问的元素的frame_id，代表可以被移出内存
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(avlbPages <= 0 || lruList.back().isPin){
    return false;
  }
  *frame_id = lruList.back().frame_id;
  lruList.pop_back();
  lruMap.erase(*frame_id);
  // 移除了一个可被替换的frame，因此要减小
  avlbPages--;
  return true;
}

// Pin住的元素代表有人使用，不能被替换，avlbPages--,且要移动到队头
void LRUReplacer::Pin(frame_id_t frame_id) {
  // 先判断下是否存在了，存在才能pin
  if(lruMap.find(frame_id) != lruMap.end()){
    frame_id_t temp = lruList.back().frame_id;
    lruList.pop_back();
    lruList.push_front(frame_element{frame_id, true});
    lruMap[temp] = lruList.begin();
    avlbPages--;
  }
}
// Unpin后元素才可以被替换，可以将其加入Lru中了
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // 先判断下是否存在了，不存在才加
  if(lruMap.find(frame_id) == lruMap.end()){
    lruList.push_front(frame_element{frame_id, false});
    lruMap.emplace(frame_id, lruList.begin());
    avlbPages++;
  }else if(lruMap[frame_id]->isPin){
    // 若存在，则判断下isPin
    lruMap[frame_id]->isPin = false;
    avlbPages++;
  }
}

// 返回可替换的frame数量
size_t LRUReplacer::Size() {
  return avlbPages;
}

}  // namespace bustub
