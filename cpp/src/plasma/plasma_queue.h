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

#ifndef PLASMA_QUEUE_H
#define PLASMA_QUEUE_H

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"
#include "plasma/common.h"

using arrow::Buffer;
using arrow::Status;

namespace plasma {
  static const uint64_t INVALID_OFFSET = -1;
  static const uint32_t QUEUE_BLOCK_SIZE = 1000; 

  struct QueueHeader {
    QueueHeader()
      : first_block_offset(INVALID_OFFSET)
      //, last_block_offset_(-1)
      , ref_count(0) {
    }
    // Current seq ID in the queue. This should only be used by the writer.
    //uint64_t cur_seq_id;
    // Sealed seq ID, this is the largest valid ID for client to read. 0 for invalid.
    //uint64_t last_seq_id;
    // Offset of the first block relative to the end of QueueHeader. -1 for invalid.
    uint64_t first_block_offset;
    // Offset of the last block relative to the end of QueueHeader. -1 for invalid.
    // uint64_t last_block_offset_;
    // Reference count to protect the first_block_offset field from being changed.
    uint32_t ref_count;
  };

  struct QueueBlockHeader {
    
    QueueBlockHeader() 
      : start_seq_id(0)
      , end_seq_id(0)
      , next_block_offset(-1)
      , ref_count(0) {

      // Account for the space occupied by QueueBlockHeader. The item offsets 
      // are relative to the start of this block.
      auto start_offset_in_block = sizeof(QueueBlockHeader);
      std::fill_n(item_offsets, sizeof(item_offsets)/sizeof(uint32_t), start_offset_in_block);
    }
    
    // Start seq ID of this block.
    uint64_t start_seq_id;
    // End seq ID of this block.
    uint64_t end_seq_id;

    // Offset of next block which is relative to end of QueueHeader. -1 indicates invalid.
    uint64_t next_block_offset; 
    // Number of elements contained in this block. Note that it's possible that a block
    // contains less than QUEUE_BLOCK_SIZE items, e.g. if it reaches the end of Queue buffer.
    uint32_t ref_count;
    // Offset of items relative to the start of this block. This array has 
    // QUEUE_BLOCK_SIZE + 1 elements as the last one accounts for the offset
    // for the end of this block (if this block has QUEUE_BLOCK_SIZE items).
    uint32_t item_offsets[QUEUE_BLOCK_SIZE + 1];
  };

 

  class PlasmaQueueWriter {
  public:
    PlasmaQueueWriter(uint8_t* buffer, uint64_t buffer_size);

    PlasmaError Append(uint8_t* data, uint32_t data_size, uint64_t& offset, uint64_t& seq_id);

    uint8_t* GetBuffer() { return buffer_; }

  private:
    bool FindStartOffset(uint32_t data_size, uint64_t& new_start_offset);

    bool Allocate(uint64_t& start_offset, uint32_t data_size);
    
    // Points to start of the ring buffer.
    uint8_t* buffer_;
    // Size of the ring buffer.
    uint64_t buffer_size_;

    QueueHeader* queue_header_;

    uint64_t seq_id_;

    uint32_t next_index_in_block_;

    uint64_t last_block_offset_;

  };

}  // namespace plasma

#endif  // PLASMA_QUEUE_H
