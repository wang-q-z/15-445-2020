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

#include <iostream>
#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) :capacity(num_pages){}

LRUReplacer::~LRUReplacer() = default;

/*
 * Remove the object that was accessed the least recently compared to all the elements being tracked by the Replacer,
 * store its contents in the output parameter and return True. If the Replacer is empty return False
 */

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    latch.lock();
    if(lru_list.empty()){
        return false;
    }
    frame_id_t frame_temp = lru_list.back();
    *frame_id = frame_temp;
    lru_list.pop_back();
    lruMap.erase(frame_temp);
    latch.unlock();
    return true;
}
/*This method should be called after a page is pinned to a frame in the BufferPoolManager.
 * It should remove the frame containing the pinned page from the LRUReplacer
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
    latch.lock();
    if(lruMap.count(frame_id) != 0){
        lru_list.remove(frame_id);
        lruMap.erase(frame_id);
    }
    latch.unlock();
}

/*
 * This method should be called when the pin_count of a page becomes 0.
 * This method should add the frame containing the unpinned page to the LRUReplacer
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
    latch.lock();
    if(lruMap.find(frame_id) == lruMap.end()){
        if(lru_list.size() >= capacity){
            frame_id_t last = lru_list.back();
            lru_list.pop_back();
            lruMap.erase(last);
        }
        lru_list.push_front(frame_id);
        lruMap[frame_id] = lru_list.begin();
    }
    latch.unlock();
}

size_t LRUReplacer::Size() { return lru_list.size(); }

}  // namespace bustub
