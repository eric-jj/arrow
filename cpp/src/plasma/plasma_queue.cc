// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// PLASMA CLIENT: Client library for using the plasma store and manager

#include "plasma/plasma_queue.h"

#ifdef _WIN32
#include <Win32_Interop/win32_types.h>
#endif

#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/util/thread-pool.h"

#include "plasma/common.h"
#include "plasma/fling.h"
#include "plasma/io.h"
#include "plasma/malloc.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"

namespace plasma {

PlasmaQueueWriter::PlasmaQueueWriter(uint8_t* buffer, uint64_t buffer_size) :
  buffer_(buffer),
  buffer_size_(buffer_size),
  queue_header_(reinterpret_cast<QueueHeader*>(buffer_)),
  seq_id_(0),
  next_index_in_block_(0),
  last_block_offset_(INVALID_OFFSET) {

  ARROW_CHECK(buffer_ != nullptr);

  // Initialize the queue header properly.
  *queue_header_ = QueueHeader(); 

  // first_block_header_ = nullptr;
  // curr_block_header_ = nullptr;
  next_index_in_block_ = 0;
}

bool PlasmaQueueWriter::FindStartOffset(uint32_t data_size, uint64_t& new_start_offset) {
  if (queue_header_->first_block_offset == INVALID_OFFSET) {
    // Queue is empty. Start immediately after QueueHeader.
    new_start_offset = sizeof(QueueHeader);
    return true;
  }

  bool should_create_block = false;
  if (next_index_in_block_ >= QUEUE_BLOCK_SIZE) {
    // Already reaches the end of current block. Create a new one.
    should_create_block = true;
    // Don't return yet, calculate the start offset for next block below.
    next_index_in_block_ = QUEUE_BLOCK_SIZE;
    // Account for the space needed for block header.
    data_size += sizeof(QueueBlockHeader);
  }

  QueueBlockHeader* curr_block_header = reinterpret_cast<QueueBlockHeader*>(buffer_ + last_block_offset_);
  auto curr_block_end = last_block_offset_ + curr_block_header->item_offsets[next_index_in_block_];
  if (curr_block_end + data_size > buffer_size_) {
    // It would reach the end of buffer if we append the new data to the current block.
    // So we need to start from the begining, which is right after QueueHeader.
    should_create_block = true;
    new_start_offset = sizeof(QueueHeader);
  } else {
    // Still ok to append to the current end.
    new_start_offset = curr_block_end;
  }
  
  return should_create_block;
}

bool PlasmaQueueWriter::Allocate(uint64_t& start_offset, uint32_t data_size) {
  if (queue_header_->first_block_offset == INVALID_OFFSET ||
     start_offset > queue_header_->first_block_offset) {
    // Check if there are enough space at the end of the buffer.
    return start_offset + data_size <= buffer_size_;
  }

  if (start_offset + data_size <= queue_header_->first_block_offset) {
    // Enough space before first_block, no need for eviction.
    return true;
  }

  // Try to evict some blocks.
  // TODO: solve race between reader & writer on first_block_offset.
  while (queue_header_->first_block_offset != INVALID_OFFSET) {
    auto curr_block = reinterpret_cast<QueueBlockHeader*>(buffer_ + queue_header_->first_block_offset);
    if (curr_block->ref_count > 0) {
      // current block is in-use, cannot evict.
      return false;
    }

    // XXX: Remove the current block for more space.
    queue_header_->first_block_offset = curr_block->next_block_offset;
    
    auto next_block_offset = curr_block->next_block_offset;
    if (next_block_offset == sizeof(QueueHeader) || next_block_offset == INVALID_OFFSET) {
      // This indicates the next block is round back at the start of the buffer, or
      // this is the last block.
      next_block_offset = buffer_size_;
    }

    if (start_offset + data_size <= next_block_offset) {
      // OK, we could satisfy the space requirement by removing current block.
      break;
    }
  }

  if (queue_header_->first_block_offset == INVALID_OFFSET) {
    last_block_offset_ = INVALID_OFFSET;
    // This indicates that all the existing blocks have been evicted. In this case
    // check if the whole buffer can contain the new item.

    return data_size <= buffer_size_;
  }

  return true;
}

int PlasmaQueueWriter::Append(uint8_t* data, uint32_t data_size) {
  uint64_t start_offset = sizeof(QueueHeader);
  bool should_create_block = FindStartOffset(data_size, start_offset);

  uint32_t required_size = data_size;
  if (should_create_block) {
     required_size += sizeof(QueueBlockHeader);
  }
  
  // We have checked there are enough space for the new item, potentially by 
  // evicting some existing blocks. Try to allocate the space for new item.
  bool succeed = Allocate(start_offset, required_size);
  if (!succeed) {
    return PlasmaError_OutOfMemory;
  }

  seq_id_++;

  if (should_create_block) {
    // 1. initialize new block.
    auto new_block_header = reinterpret_cast<QueueBlockHeader*>(buffer_ + start_offset);
    *new_block_header = QueueBlockHeader();
    new_block_header->start_seq_id = seq_id_;
    new_block_header->end_seq_id = seq_id_;

    next_index_in_block_ = 0;
    // append new data.
    memcpy(reinterpret_cast<uint8_t*>(new_block_header) + new_block_header->item_offsets[next_index_in_block_],
      data, data_size);
    new_block_header->item_offsets[next_index_in_block_ + 1] = new_block_header->item_offsets[next_index_in_block_] + data_size;
    next_index_in_block_++;

    // hook up previous block with new one.
    if (queue_header_->first_block_offset == INVALID_OFFSET) {
      queue_header_->first_block_offset = start_offset;
      last_block_offset_ = start_offset;
    } else {
      QueueBlockHeader* last_block_header = reinterpret_cast<QueueBlockHeader*>(buffer_ + last_block_offset_);
      last_block_header->next_block_offset = start_offset;
      last_block_offset_ = start_offset;
    }
  } else {
    // ASSERT(last_block_offset_ + curr_block_header->item_offsets[next_index_in_block_] == start_offset);
    QueueBlockHeader* last_block_header = reinterpret_cast<QueueBlockHeader*>(buffer_ + last_block_offset_);
    last_block_header->end_seq_id = seq_id_;

    memcpy(reinterpret_cast<uint8_t*>(last_block_header) + last_block_header->item_offsets[next_index_in_block_], 
      data, data_size);

    last_block_header->item_offsets[next_index_in_block_ + 1] = last_block_header->item_offsets[next_index_in_block_] + data_size;
    next_index_in_block_++;    
  }


  // Does it need to create a new block?
  // If this queue is empty. Create a new block.
  // If current block is full, or there's no enough room by appendig to current block
  // (it's approaching the end of the ring buffer, so cannot grow the current block).

  // Does it need to evict existing blocks?

  // allocate enough memory (potentially evicting existing blocks)

  // If new block needed, create & initialize new block.

  // Append the entry to the end of this block.
  return PlasmaError_OK;
}


PlasmaQueueReader::PlasmaQueueReader(uint8_t* buffer, uint64_t buffer_size) :
  buffer_(buffer),
  buffer_size_(buffer_size),
  queue_header_(reinterpret_cast<QueueHeader*>(buffer_)),
  curr_seq_id_(0),
  curr_block_header_(nullptr) {

  ARROW_CHECK(buffer_ != nullptr);
}

int PlasmaQueueReader::SetStartSeqId(uint64_t seq_id) {
  // if seq is current seq + 1
  // then look at current block.

  // Else if (seq < current seq )  search from start.
  // Else search from current block.
  
  // 0 is a magic number, means start from the first block in the queue.
  // TODO: in some cases we could search from current block instead of from start.
  
  if (queue_header_->first_block_offset == INVALID_OFFSET) {
     curr_seq_id_ = 0;
     return PlasmaError_ObjectNonexistent; 
  }

  if (curr_block_header_ != nullptr) {
    if (seq_id >= curr_block_header_->start_seq_id &&
      seq_id <= curr_block_header_->end_seq_id) {
        curr_seq_id_ = seq_id;
        // already ref-cnted, no need to add refcnt.
        return PlasmaError_OK;
    }
  }

  auto curr_block_offset = queue_header_->first_block_offset;
  while (curr_block_offset != INVALID_OFFSET) {
    auto queue_block_header = reinterpret_cast<QueueBlockHeader*>(buffer_ + curr_block_offset);
    queue_block_header->ref_count++;

    if (seq_id < queue_block_header->start_seq_id) {
      // This seq id doesn't exist. 
      queue_block_header->ref_count--;
      return PlasmaError_ObjectNonexistent;
    }

    // seq_id >= queue_block_header->start_seq_id
    if (seq_id <= queue_block_header->end_seq_id) {
       // Set current block, and curr seq_id.
       // Still hold reference count for current block.
       curr_block_header_ = queue_block_header;
       curr_seq_id_ = seq_id;

       return PlasmaError_OK;      
    }

    // Examine next block. 
    curr_block_offset = queue_block_header->next_block_offset;
    queue_block_header->ref_count--;
  }

  return PlasmaError_ObjectNonexistent; 
}

int PlasmaQueueReader::GetNext(uint8_t*& data, uint32_t& data_size, uint64_t& seq_id) {
  
  // Check if still in current block. 
  // if not, move to next block. Add refcnt to next block, and release refcnt to current block.
  if (curr_block_header_ == nullptr) {
    auto first_block_offset = queue_header_->first_block_offset;
    // If this is called for the first time, initialize it (if queue does have items).
    if (first_block_offset == INVALID_OFFSET) {
      // queue is empty.
      return PlasmaError_ObjectNonexistent;
    }
    
    // set curr_block_header_.
    curr_block_header_ = reinterpret_cast<QueueBlockHeader*>(buffer_ + first_block_offset);
    curr_block_header_->ref_count++;

    auto index = 0;
    data = reinterpret_cast<uint8_t*>(curr_block_header_) + curr_block_header_->item_offsets[index];
    data_size = curr_block_header_->item_offsets[index + 1] - curr_block_header_->item_offsets[index];
    curr_seq_id_ = curr_block_header_->start_seq_id;
    seq_id = curr_seq_id_++;
    curr_block_header_->ref_count++; // will be released when client release the item.
    outstanding_seq_ids_[seq_id] = curr_block_header_;

    return PlasmaError_OK;

  }

  // curr_seq_id >= curr_block_header_->seq_id, but it's possible it reaches the end of this block
  // and arrives at the first of next block (if it exists).
  if (curr_seq_id_ > curr_block_header_->end_seq_id) {
    auto next_block_offset = curr_block_header_->next_block_offset;
    if (next_block_offset == INVALID_OFFSET) {
      // next block is empty. return failure.
      // XXX in this case, should we update curr seqid and curr block?
      return PlasmaError_ObjectNonexistent;
    }
 
    auto last_block_header = curr_block_header_;
    curr_block_header_ = reinterpret_cast<QueueBlockHeader*>(buffer_ + next_block_offset);
    curr_block_header_->ref_count++;
    // Deference previous block.
    last_block_header->ref_count--;
  }

  auto index = curr_seq_id_ - curr_block_header_->start_seq_id;

  data = reinterpret_cast<uint8_t*>(curr_block_header_) + curr_block_header_->item_offsets[index];
  data_size = curr_block_header_->item_offsets[index + 1] - curr_block_header_->item_offsets[index];
  seq_id = curr_seq_id_++;
  curr_block_header_->ref_count++;  // will be released when client release the item.
  outstanding_seq_ids_[seq_id] = curr_block_header_;

  return PlasmaError_OK;
}

int PlasmaQueueReader::Release(uint64_t seq_id) {
  auto it = outstanding_seq_ids_.find(seq_id);
  if (it == outstanding_seq_ids_.end()) {
    return PlasmaError_ObjectNonexistent;
  }

  it->second->ref_count--;
  outstanding_seq_ids_.erase(it);
  return PlasmaError_OK;
}
/*
QueueBlockHeader* PlasmaClient::Impl::create_new_block(uint8_t* pointer,
  QueueHeader* queue_header,
  QueueBlockHeader* block_header,
  int32_t offset) {
  auto p = reinterpret_cast<char*>(block_header) + block_header->item_offsets[QUEUE_BLOCK_SIZE];

  auto new_block_header = reinterpret_cast<QueueBlockHeader*>(p);
  if (new_block_header == nullptr) {
    auto first_block = queue_header->firset_block_header;
    if (firse_block.next_block_offset == 0) {
      /// It means the queue can't contains a block, retun nullptr.
      return nullptr;
    }
    queue_header->first_block = 
      reinterpret_cast<QueueBlockHeader*>(first_block->next_block_offset + pointer);
    if (first_block > block_header) {
      /// Merge the next block to current block.
      block_header->cur_boundary = first_block->cur_boundary;
      return block_header;
    } else {
      new_block_header = first_block;
    }
  } else {
    new_block_header->next_block_offset = queue_header->next_block_offset;
  }
  new_block_header->start_seq_id = new_block_header->cur_seq_id;
  memset(new_block_header->item_offsets, 0, sizeof(new_block_header->item_offsets));
  new_block_header->item_offsets[0] = sizeof(QueueBlockHeader);
  return new_block_header;
}

Status PlasmaStore::PushQueue(const ObjectID& object_id, uint8_t* data, int64_t data_size) {
  ARROW_LOG(DEBUG) << "push queue " << object_id.hex();
  
  auto entry = objects_in_use_.find(object_id);
  if (entry == objects_in_use_.end()) {
    return Status::PlasmaObjectNonexistent("object does not exist in the plasma store");
  };
  
  ARROW_CHECK(entry != NULL);
  ARROW_CHECK(entry->state == PLASMA_QUEUE);
  
  QueueHeader* queue_header = reinterpret_cast<QueueHeader*>(entry->pointer);
  queue_header->cur_seq_id++;
  QueueBlockHeader* block_header = queue_header->cur_block_header;
  auto offset = queue_header->cur_seq_id - block_header->start_seq_id;

  bool new_block = false;
  if (offset >= QUEUE_BLOCK_SIZE) {
    new_block = true;
  }

  /// Get the first item start pointer;
  auto p = reinterpret_cast<char*>(queue_header + 1);
  auto new_end = p + block_header->item_offsets[offset] + data_size;
  if (new_end > queue_header->cur_boundary) {
    new_block = true;
  }

  if (new_block) {
    /// Create new block
    block_header = create_new_block(entry->pointer, queue_header, block_header, offset);
    if (block_header == nullptr) {
      return PlasmaError_OutOfMemory;
    }
  }
  memcpy(p + block_header->item_offsets[offset], data, data_size);
  block_header->item_offsets[offset + 1] = block_header->item_offsets[offset] + data_size;

  // Inform all subscribers that a new object has been sealed.
  // push_notification(&entry->info);

  // Update all get requests that involve this object.
  // update_object_get_requests(object_id);
  return Status::OK();
}
*/
/*
Status PlasmaClient::Impl::PushQueue(const ObjectID& object_id, uint8_t* data, int64_t data_size) {
  RETURN_NOT_OK(
      SendPushQueueItemRequest(store_conn_, object_id, data_size));
  std::vector<uint8_t> buffer;
  RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType_PlasmaPushQueueItemReply, &buffer));
  ObjectID id;
  uint64_t data_offset;
  uint64_t returned_data_size;
  uint64_t seq_id;
  RETURN_NOT_OK(
      ReadPushQueueItemReply(buffer.data(), buffer.size(), &id, &data_offset, &returned_data_size, &seq_id));
  // if (device_num == 0) { // TODO should we handle GPU case?
  ARROW_CHECK(returned_data_size == data_size);

  // TODO: this may need to be revisited.
  auto entry = objects_in_use_.find(object_id);
  if (entry == objects_in_use_.end()) {
    return Status::PlasmaObjectNonexistent("object does not exist in the plasma store");
  }

  auto pointer = lookup_mmapped_file(entry->second->object.store_fd);
  if (pointer == nullptr) {
    return Status::PlasmaObjectNonexistent("object does not exist in the plasma store");
  }

  // Get the pointer in PlasmaStore, write the data.  
  memcpy(pointer + entry->second->object.data_offset + data_offset, data, data_size);

  // TODO: Seal this seq_id so that PlasmaStore knows it's done and can update its seq and notify clients,
  // otherwise clients can read incomplete data. Another option is to put data into plasma requests but
  // then it involves serialization/deserialization of data.

  return Status::OK();
}*/

/*
Status PlasmaClient::Impl::GetQueue(const ObjectID& object_id, uint8_t* data, int64_t data_size) {
  ARROW_LOG(DEBUG) << "called plasma_create on conn " << store_conn_ << " with size "
                   << data_size << " and metadata size " << metadata_size;
  RETURN_NOT_OK(
      SendPushQueueItemRequest(store_conn_, object_id, data_size));
  std::vector<uint8_t> buffer;
  RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType_PlasmaPushQueueItemReply, &buffer));
  ObjectID id;
  uint64_t data_offset;
  uint64_t returned_data_size;
  uint64_t seq_id;
  RETURN_NOT_OK(
      ReadPushQueueItemReply(buffer.data(), buffer.size(), &id, &data_offset, &returned_data_size, &seq_id));
  // if (device_num == 0) { // TODO should we handle GPU case?
  ARROW_CHECK(returned_data_size == data_size);

  // TODO: this may need to be revisited.
  auto entry = objects_in_use_.find(object_id);
  if (entry == objects_in_use_.end()) {
    return Status::PlasmaObjectNonexistent("object does not exist in the plasma store");
  }

  // Get the pointer in PlasmaStore, write the data.  
  memcpy(entry->pointer + data_offset, data, data_size);

  // TODO: Seal this seq_id so that PlasmaStore knows it's done and can update its seq and notify clients,
  // otherwise clients can read incomplete data. Another option is to put data into plasma requests but
  // then it involves serialization/deserialization of data.

  return Status::OK();
}
*/
}  // namespace plasma
