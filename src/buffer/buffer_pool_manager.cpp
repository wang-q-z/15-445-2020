//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
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
    std::lock_guard<std::mutex> lock(latch_);

    if(page_table_.find(page_id) != page_table_.end()){
        frame_id_t frame_id = page_table_[page_id];
        Page *re_page = &pages_[frame_id];
        replacer_->Pin(frame_id);
        re_page->pin_count_++;
        return re_page;
    }
    // free list
    frame_id_t frame_id_;
    if(!free_list_.empty()){
        frame_id_ = free_list_.front();
        free_list_.pop_front();
    }else{
        if(!replacer_->Victim(&frame_id_)) {
            return nullptr;
        }
    }
    Page *re_page_ = &pages_[frame_id_];

    if(re_page_->IsDirty()){
        disk_manager_->WritePage(re_page_->page_id_, re_page_->data_);
        re_page_->is_dirty_ = false;
    }
    page_table_.erase(re_page_->page_id_);
    page_table_[re_page_->page_id_] = frame_id_;

    re_page_->ResetMemory();
    disk_manager_->ReadPage(re_page_->page_id_,re_page_->data_);
    return re_page_;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(latch_);
    if (page_table_.find(page_id) == page_table_.end()){
        return false;
    }
    frame_id_t frame_id = page_table_[page_id];
    Page *u_page = &pages_[frame_id];

    if (u_page->pin_count_ == 0){
        return false;
    }

    u_page->pin_count_--;
    if(u_page->pin_count_ == 0){
        replacer_->Unpin(frame_id);
    }

    if (is_dirty){
        u_page->is_dirty_ = true;
    }
    return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_table_.find(page_id) == page_table_.end()){
      return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page* f_page = &pages_[frame_id];

  disk_manager_->DiskManager::WritePage(page_id,f_page->data_);
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
    std::lock_guard<std::mutex> lock(latch_);
    frame_id_t frame_id_;
    if(!free_list_.empty()){
        frame_id_ = free_list_.front();
        free_list_.pop_front();
    }else{
        if(!replacer_->Victim(&frame_id_)) {
            return nullptr;
        }
    }

    *page_id = disk_manager_->AllocatePage();
    Page* re_page_ = &pages_[frame_id_];
    re_page_->page_id_ = *page_id;
    if(re_page_->IsDirty()){
        disk_manager_->WritePage(re_page_->page_id_, re_page_->data_);
        re_page_->is_dirty_ = false;
    }
    page_table_.erase(re_page_->page_id_);
    page_table_[re_page_->page_id_] = frame_id_;

    re_page_->ResetMemory();
    disk_manager_->ReadPage(re_page_->page_id_,re_page_->data_);

    return re_page_;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    std::lock_guard<std::mutex> lock(latch_);
    if (page_table_.find(page_id) == page_table_.end()){
        return true;
    }
    frame_id_t frame_id = page_table_[page_id];
    Page* d_page = &pages_[frame_id];
    if(d_page->pin_count_ != 0){
        return false;
    };

    d_page->ResetMemory();
    page_table_.erase(page_id);
    disk_manager_->DiskManager::DeallocatePage(page_id);
    free_list_.push_back(page_id);
    return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
    std::lock_guard<std::mutex> lock(latch_);
    for (size_t i = 0; i < pool_size_; i++) {
        // FlushPageImpl(i); // 这样写有问题，因为FlushPageImpl传入的参数是page id，其值可以>=pool size
        Page *page = &pages_[i];
        if (page->page_id_ != INVALID_PAGE_ID && page->IsDirty()) {
            disk_manager_->WritePage(page->page_id_, page->data_);
            page->is_dirty_ = false;
        }
    }
}

}  // namespace bustub
