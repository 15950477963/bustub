//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//  printf("new page: %d\n", *page_id);
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  auto page_iter = page_table_.find(page_id);
  // 直接在page_table里找到了
  if(page_iter != page_table_.end()) {
    pages_[page_iter->second].pin_count_++;
    return &pages_[page_iter->second];
  }

  // page_table没有，需要从free_list和replacer找一个空白或替换的frame_id
  frame_id_t fetch_frame_id;
  page_id_t replace_page_id;
  if (!free_list_.empty()){
    fetch_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&fetch_frame_id)) {
      return nullptr;
    }
    // test出错点：用这个frame_id前先看下是否is_dirty，是的话记得先写回硬盘
    if (pages_[fetch_frame_id].is_dirty_) {
      replace_page_id = pages_[fetch_frame_id].GetPageId();
      FlushPageImpl(replace_page_id);
    }
    page_table_.erase(replace_page_id);
  }

  // 更新page_table和pages
  page_table_.emplace(page_id, fetch_frame_id);
  pages_[fetch_frame_id].ResetMemory();
  pages_[fetch_frame_id].is_dirty_ = false;
  pages_[fetch_frame_id].pin_count_ = 1;
  pages_[fetch_frame_id].page_id_ = page_id;

  disk_manager_->ReadPage(page_id, pages_[fetch_frame_id].data_);

  return &pages_[fetch_frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  auto page_iter = page_table_.find(page_id);
  // 没有这个page，直接返回false
  if (page_iter == page_table_.end()) {
    return false;
  }
  frame_id_t unpin_frame_id = page_iter->second;
  // pin_count小于0，不需要unpin，返回false
  if (pages_[unpin_frame_id].pin_count_ <= 0) {
    return false;
  }
  // 正常unpin操作
  // 用或操作，因为传入的is_dirty和自己的is_dirty有一个为true，那就代表被修改
  pages_[unpin_frame_id].is_dirty_ = pages_[unpin_frame_id].is_dirty_ || is_dirty;
  pages_[unpin_frame_id].pin_count_ -= 1;

  // test出错点：bpm进行unpin后需要判断replacer是否需要unpin
  if (pages_[unpin_frame_id].pin_count_ == 0) {
    replacer_->Unpin(unpin_frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // 这里没有判断是否pin住，交给调用他的函数去做
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto page_iter = page_table_.find(page_id);
  // 没有这个page，直接返回false
  if (page_iter == page_table_.end()) {
    return false;
  }
  // page写回disk，is_dirty修改为false
  frame_id_t write_frame_id = page_iter->second;
  disk_manager_->WritePage(page_id, pages_[write_frame_id].data_);
  pages_[write_frame_id].is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // 从free_list或者replacer中取得可以使用的frame_id
  frame_id_t free_frame_id;
  page_id_t replace_page_id;
  if (!free_list_.empty()){
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&free_frame_id)) {
      return nullptr;
    }
    // test出错点：用这个frame_id前先看下是否is_dirty，是的话记得先写回硬盘
    if (pages_[free_frame_id].is_dirty_) {
      replace_page_id = pages_[free_frame_id].GetPageId();
      FlushPageImpl(replace_page_id);
    }
    page_table_.erase(replace_page_id);
  }

  // 从硬盘读取新的page，将其写入bpm
  *page_id = disk_manager_->AllocatePage();
  pages_[free_frame_id].ResetMemory();
  pages_[free_frame_id].page_id_ = *page_id;
  pages_[free_frame_id].is_dirty_ = false;

  // test出错点：注意New的pin_count_应该是1，不然test过不了
  pages_[free_frame_id].pin_count_ = 1;
  page_table_.emplace(*page_id, free_frame_id);
  return &pages_[free_frame_id];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  auto page_iter = page_table_.find(page_id);
  frame_id_t del_frame_id;
  if(page_iter == page_table_.end()) {
    return true;
  }
  del_frame_id = page_iter->second;
  if (pages_[del_frame_id].pin_count_ != 0){
    return false;
  }
  disk_manager_->DeallocatePage(page_id);
  pages_[del_frame_id].ResetMemory();
  free_list_.push_back(del_frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for(size_t i = 0; i < pool_size_; i++) {
    FlushPageImpl(pages_[i].GetPageId());
  }
}

}  // namespace bustub
