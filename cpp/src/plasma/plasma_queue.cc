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

using flatbuf::PlasmaError;

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

Status PlasmaQueueWriter::Append(uint8_t* data, uint32_t data_size, uint64_t& offset, uint64_t& seq_id) {
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
    return Status::OutOfMemory("could not allocate space in queue buffer");
  }

  seq_id_++;
  seq_id = seq_id_;

  if (should_create_block) {
    // 1. initialize new block.
    auto new_block_header = reinterpret_cast<QueueBlockHeader*>(buffer_ + start_offset);
    *new_block_header = QueueBlockHeader();
    new_block_header->start_seq_id = seq_id_;
    new_block_header->end_seq_id = seq_id_;

    next_index_in_block_ = 0;
    // append new data.
    uint8_t* dest_position = reinterpret_cast<uint8_t*>(new_block_header) + new_block_header->item_offsets[next_index_in_block_];
    offset = dest_position - buffer_;
    if (data != nullptr) {
      memcpy(dest_position, data, data_size);
    }
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

    uint8_t* dest_position = reinterpret_cast<uint8_t*>(last_block_header) + last_block_header->item_offsets[next_index_in_block_];
    offset = dest_position - buffer_;
    if (data != nullptr) {
      memcpy(dest_position, data, data_size);
    }

    last_block_header->item_offsets[next_index_in_block_ + 1] = last_block_header->item_offsets[next_index_in_block_] + data_size;
    next_index_in_block_++;    
  }

  return Status::OK();
}

}  // namespace plasma
