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

#include "plasma/client.h"

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

#ifdef PLASMA_GPU
#include "arrow/gpu/cuda_api.h"

using arrow::gpu::CudaBuffer;
using arrow::gpu::CudaBufferWriter;
using arrow::gpu::CudaContext;
using arrow::gpu::CudaDeviceManager;
#endif

#define XXH_STATIC_LINKING_ONLY
#include "thirdparty/xxhash.h"

#define XXH64_DEFAULT_SEED 0

namespace plasma {

using arrow::MutableBuffer;

typedef struct XXH64_state_s XXH64_state_t;

// Number of threads used for hash computations.
constexpr int64_t kHashingConcurrency = 8;
constexpr int64_t kBytesInMB = 1 << 20;

// Use 100MB as an overestimate of the L3 cache size.
constexpr int64_t kL3CacheSizeBytes = 100000000;

// ----------------------------------------------------------------------
// GPU support

#ifdef PLASMA_GPU
struct GpuProcessHandle {
  /// Pointer to CUDA buffer that is backing this GPU object.
  std::shared_ptr<CudaBuffer> ptr;
  /// Number of client using this GPU object.
  int client_count;
};

// This is necessary as IPC handles can only be mapped once per process.
// Thus if multiple clients in the same process get the same gpu object,
// they need to access the same mapped CudaBuffer.
static std::unordered_map<ObjectID, GpuProcessHandle*> gpu_object_map;
static std::mutex gpu_mutex;
#endif

// ----------------------------------------------------------------------
// PlasmaBuffer

/// A Buffer class that automatically releases the backing plasma object
/// when it goes out of scope.
class ARROW_NO_EXPORT PlasmaBuffer : public Buffer {
 public:
  ~PlasmaBuffer();

  PlasmaBuffer(std::shared_ptr<PlasmaClient::Impl> client, const ObjectID& object_id,
               const std::shared_ptr<Buffer>& buffer)
      : Buffer(buffer, 0, buffer->size()), client_(client), object_id_(object_id) {
    if (buffer->is_mutable()) {
      is_mutable_ = true;
    }
  }

 private:
  std::shared_ptr<PlasmaClient::Impl> client_;
  ObjectID object_id_;
};

// ----------------------------------------------------------------------
// PlasmaClient::Impl

struct ObjectInUseEntry {
  /// A count of the number of times this client has called PlasmaClient::Create
  /// or
  /// PlasmaClient::Get on this object ID minus the number of calls to
  /// PlasmaClient::Release.
  /// When this count reaches zero, we remove the entry from the ObjectsInUse
  /// and decrement a count in the relevant ClientMmapTableEntry.
  int count;
  /// Cached information to read the object.
  PlasmaObject object;
  /// A flag representing whether the object has been sealed.
  bool is_sealed;
};

/// Configuration options for the plasma client.
struct PlasmaClientConfig {
  /// Number of release calls we wait until the object is actually released.
  /// This allows us to avoid invalidating the cpu cache on workers if objects
  /// are reused accross tasks.
  size_t release_delay;
};

struct ClientMmapTableEntry {
  /// The result of mmap for this file descriptor.
  uint8_t* pointer;
  /// The length of the memory-mapped file.
  size_t length;
  /// The number of objects in this memory-mapped file that are currently being
  /// used by the client. When this count reaches zeros, we unmap the file.
  int count;
};

class PlasmaClient::Impl : public std::enable_shared_from_this<PlasmaClient::Impl> {
 public:
  Impl();
  ~Impl();

  // PlasmaClient method implementations

  Status Connect(const std::string& store_socket_name,
                 const std::string& manager_socket_name, int release_delay,
                 int num_retries = -1);

  Status Create(const ObjectID& object_id, int64_t data_size, const uint8_t* metadata,
                int64_t metadata_size, std::shared_ptr<Buffer>* data, int device_num = 0, ObjectType type = ObjectType_Default);

  Status Get(const std::vector<ObjectID>& object_ids, int64_t timeout_ms,
             std::vector<ObjectBuffer>* object_buffers);

  Status Get(const ObjectID* object_ids, int64_t num_objects, int64_t timeout_ms,
             ObjectBuffer* object_buffers);

  Status Release(const ObjectID& object_id);

  Status Contains(const ObjectID& object_id, bool* has_object);

  Status Abort(const ObjectID& object_id);

  Status Seal(const ObjectID& object_id);

  Status Delete(const std::vector<ObjectID>& object_ids);

  Status Evict(int64_t num_bytes, int64_t& num_bytes_evicted);

  Status Hash(const ObjectID& object_id, uint8_t* digest);

  Status Subscribe(int* fd);

  Status GetNotification(int fd, ObjectID* object_id, int64_t* data_size,
                         int64_t* metadata_size);

  Status Disconnect();

  Status Fetch(int num_object_ids, const ObjectID* object_ids);

  Status Wait(int64_t num_object_requests, ObjectRequest* object_requests,
              int num_ready_objects, int64_t timeout_ms, int* num_objects_ready);

  Status Transfer(const char* addr, int port, const ObjectID& object_id);

  Status Info(const ObjectID& object_id, int* object_status);

  int get_manager_fd() const;

  Status FlushReleaseHistory();

  bool IsInUse(const ObjectID& object_id);

  Status PushQueue(const ObjectID& object_id, uint8_t* data, int64_t data_size);

 private:
  /// This is a helper method for unmapping objects for which all references have
  /// gone out of scope, either by calling Release or Abort.
  ///
  /// @param object_id The object ID whose data we should unmap.
  Status UnmapObject(const ObjectID& object_id);

  Status PerformRelease(const ObjectID& object_id);

  /// Common helper for Get() variants
  Status GetBuffers(const ObjectID* object_ids, int64_t num_objects, int64_t timeout_ms,
                    const std::function<std::shared_ptr<Buffer>(
                        const ObjectID&, const std::shared_ptr<Buffer>&)>& wrap_buffer,
                    ObjectBuffer* object_buffers);

  uint8_t* lookup_or_mmap(int fd, int store_fd_val, int64_t map_size);

  uint8_t* lookup_mmapped_file(int store_fd_val);

  void increment_object_count(const ObjectID& object_id, PlasmaObject* object,
                              bool is_sealed);

  bool compute_object_hash_parallel(XXH64_state_t* hash_state, const unsigned char* data,
                                    int64_t nbytes);

  uint64_t compute_object_hash(const ObjectBuffer& obj_buffer);

  /// File descriptor of the Unix domain socket that connects to the store.
  int store_conn_;
  /// File descriptor of the Unix domain socket that connects to the manager.
  int manager_conn_;
  /// Table of dlmalloc buffer files that have been memory mapped so far. This
  /// is a hash table mapping a file descriptor to a struct containing the
  /// address of the corresponding memory-mapped file.
  std::unordered_map<int, ClientMmapTableEntry> mmap_table_;
  /// A hash table of the object IDs that are currently being used by this
  /// client.
  std::unordered_map<ObjectID, std::unique_ptr<ObjectInUseEntry>> objects_in_use_;
  /// Object IDs of the last few release calls. This is a deque and
  /// is used to delay releasing objects to see if they can be reused by
  /// subsequent tasks so we do not unneccessarily invalidate cpu caches.
  /// TODO(pcm): replace this with a proper lru cache using the size of the L3
  /// cache.
  std::deque<ObjectID> release_history_;
  /// The number of bytes in the combined objects that are held in the release
  /// history doubly-linked list. If this is too large then the client starts
  /// releasing objects.
  int64_t in_use_object_bytes_;
  /// Configuration options for the plasma client.
  PlasmaClientConfig config_;
  /// The amount of memory available to the Plasma store. The client needs this
  /// information to make sure that it does not delay in releasing so much
  /// memory that the store is unable to evict enough objects to free up space.
  int64_t store_capacity_;
  
  // std::unordered_map<ObjectID, QueueHeader> queue_info_;
#ifdef PLASMA_GPU
  /// Cuda Device Manager.
  arrow::gpu::CudaDeviceManager* manager_;
#endif
};

PlasmaBuffer::~PlasmaBuffer() { ARROW_UNUSED(client_->Release(object_id_)); }

PlasmaClient::Impl::Impl() {
#ifdef PLASMA_GPU
  CudaDeviceManager::GetInstance(&manager_);
#endif
}

PlasmaClient::Impl::~Impl() {}

// If the file descriptor fd has been mmapped in this client process before,
// return the pointer that was returned by mmap, otherwise mmap it and store the
// pointer in a hash table.
uint8_t* PlasmaClient::Impl::lookup_or_mmap(int fd, int store_fd_val, int64_t map_size) {
  auto entry = mmap_table_.find(store_fd_val);
  if (entry != mmap_table_.end()) {
    close(fd);
    return entry->second.pointer;
  } else {
    // We subtract kMmapRegionsGap from the length that was added
    // in fake_mmap in malloc.h, to make map_size page-aligned again.
    uint8_t* result = reinterpret_cast<uint8_t*>(mmap(
        NULL, map_size - kMmapRegionsGap, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
    // TODO(pcm): Don't fail here, instead return a Status.
    if (result == MAP_FAILED) {
      ARROW_LOG(FATAL) << "mmap failed";
    }
    close(fd);  // Closing this fd has an effect on performance.

    ClientMmapTableEntry& entry = mmap_table_[store_fd_val];
    entry.pointer = result;
    entry.length = map_size;
    entry.count = 0;
    return result;
  }
}

// Get a pointer to a file that we know has been memory mapped in this client
// process before.
uint8_t* PlasmaClient::Impl::lookup_mmapped_file(int store_fd_val) {
  auto entry = mmap_table_.find(store_fd_val);
  ARROW_CHECK(entry != mmap_table_.end());
  return entry->second.pointer;
}

bool PlasmaClient::Impl::IsInUse(const ObjectID& object_id) {
  const auto elem = objects_in_use_.find(object_id);
  return (elem != objects_in_use_.end());
}

void PlasmaClient::Impl::increment_object_count(const ObjectID& object_id,
                                                PlasmaObject* object, bool is_sealed) {
  // Increment the count of the object to track the fact that it is being used.
  // The corresponding decrement should happen in PlasmaClient::Release.
  auto elem = objects_in_use_.find(object_id);
  ObjectInUseEntry* object_entry;
  if (elem == objects_in_use_.end()) {
    // Add this object ID to the hash table of object IDs in use. The
    // corresponding call to free happens in PlasmaClient::Release.
    objects_in_use_[object_id] =
        std::unique_ptr<ObjectInUseEntry>(new ObjectInUseEntry());
    objects_in_use_[object_id]->object = *object;
    objects_in_use_[object_id]->count = 0;
    objects_in_use_[object_id]->is_sealed = is_sealed;
    object_entry = objects_in_use_[object_id].get();
    if (object->device_num == 0) {
      // Increment the count of the number of objects in the memory-mapped file
      // that are being used. The corresponding decrement should happen in
      // PlasmaClient::Release.
      auto entry = mmap_table_.find(object->store_fd);
      ARROW_CHECK(entry != mmap_table_.end());
      ARROW_CHECK(entry->second.count >= 0);
      // Update the in_use_object_bytes_.
      in_use_object_bytes_ +=
          (object_entry->object.data_size + object_entry->object.metadata_size);
      entry->second.count += 1;
    }
  } else {
    object_entry = elem->second.get();
    ARROW_CHECK(object_entry->count > 0);
  }
  // Increment the count of the number of instances of this object that are
  // being used by this client. The corresponding decrement should happen in
  // PlasmaClient::Release.
  object_entry->count += 1;
}

Status PlasmaClient::Impl::Create(const ObjectID& object_id, int64_t data_size,
                                  const uint8_t* metadata, int64_t metadata_size,
                                  std::shared_ptr<Buffer>* data, int device_num, ObjectType object_type) {
  ARROW_LOG(DEBUG) << "called plasma_create on conn " << store_conn_ << " with size "
                   << data_size << " and metadata size " << metadata_size;
  RETURN_NOT_OK(
      SendCreateRequest(store_conn_, object_id, data_size, metadata_size, device_num, object_type));
  std::vector<uint8_t> buffer;
  RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaCreateReply, &buffer));
  ObjectID id;
  PlasmaObject object;
  int store_fd;
  int64_t mmap_size;
  RETURN_NOT_OK(
      ReadCreateReply(buffer.data(), buffer.size(), &id, &object, &store_fd, &mmap_size));
  // If the CreateReply included an error, then the store will not send a file
  // descriptor.
  if (device_num == 0) {
    int fd = recv_fd(store_conn_);
    ARROW_CHECK(fd >= 0) << "recv not successful";
    ARROW_CHECK(object.data_size == data_size);
    ARROW_CHECK(object.metadata_size == metadata_size);
    // The metadata should come right after the data.
    ARROW_CHECK(object.metadata_offset == object.data_offset + data_size);
    *data = std::make_shared<MutableBuffer>(
        lookup_or_mmap(fd, store_fd, mmap_size) + object.data_offset, data_size);
    // If plasma_create is being called from a transfer, then we will not copy the
    // metadata here. The metadata will be written along with the data streamed
    // from the transfer.
    if (metadata != NULL) {
      // Copy the metadata to the buffer.
      memcpy((*data)->mutable_data() + object.data_size, metadata, metadata_size);
    }
  } else {
#ifdef PLASMA_GPU
    std::lock_guard<std::mutex> lock(gpu_mutex);
    std::shared_ptr<CudaContext> context;
    RETURN_NOT_OK(manager_->GetContext(device_num - 1, &context));
    GpuProcessHandle* handle = new GpuProcessHandle();
    RETURN_NOT_OK(context->OpenIpcBuffer(*object.ipc_handle, &handle->ptr));
    gpu_object_map[object_id] = handle;
    *data = handle->ptr;
    if (metadata != NULL) {
      // Copy the metadata to the buffer.
      CudaBufferWriter writer(std::dynamic_pointer_cast<CudaBuffer>(*data));
      writer.WriteAt(object.data_size, metadata, metadata_size);
    }
#else
    ARROW_LOG(FATAL) << "Arrow GPU library is not enabled.";
#endif
  }

  // Increment the count of the number of instances of this object that this
  // client is using. A call to PlasmaClient::Release is required to decrement
  // this
  // count. Cache the reference to the object.
  increment_object_count(object_id, &object, false);
  // We increment the count a second time (and the corresponding decrement will
  // happen in a PlasmaClient::Release call in plasma_seal) so even if the
  // buffer
  // returned by PlasmaClient::Dreate goes out of scope, the object does not get
  // released before the call to PlasmaClient::Seal happens.
  increment_object_count(object_id, &object, false);
  return Status::OK();
}

Status PlasmaClient::Impl::GetBuffers(
    const ObjectID* object_ids, int64_t num_objects, int64_t timeout_ms,
    const std::function<std::shared_ptr<Buffer>(
        const ObjectID&, const std::shared_ptr<Buffer>&)>& wrap_buffer,
    ObjectBuffer* object_buffers) {
  // Fill out the info for the objects that are already in use locally.
  bool all_present = true;
  for (int64_t i = 0; i < num_objects; ++i) {
    auto object_entry = objects_in_use_.find(object_ids[i]);
    if (object_entry == objects_in_use_.end()) {
      // This object is not currently in use by this client, so we need to send
      // a request to the store.
      all_present = false;
    } else {
      // NOTE: If the object is still unsealed, we will deadlock, since we must
      // have been the one who created it.
      ARROW_CHECK(object_entry->second->is_sealed)
          << "Plasma client called get on an unsealed object that it created";
      PlasmaObject* object = &object_entry->second->object;
      std::shared_ptr<Buffer> physical_buf;

      if (object->device_num == 0) {
        uint8_t* data = lookup_mmapped_file(object->store_fd);
        physical_buf = std::make_shared<Buffer>(
            data + object->data_offset, object->data_size + object->metadata_size);
      } else {
#ifdef PLASMA_GPU
        physical_buf = gpu_object_map.find(object_ids[i])->second->ptr;
#else
        ARROW_LOG(FATAL) << "Arrow GPU library is not enabled.";
#endif
      }
      physical_buf = wrap_buffer(object_ids[i], physical_buf);
      object_buffers[i].data = SliceBuffer(physical_buf, 0, object->data_size);
      object_buffers[i].metadata =
          SliceBuffer(physical_buf, object->data_size, object->metadata_size);
      object_buffers[i].device_num = object->device_num;
      // Increment the count of the number of instances of this object that this
      // client is using. Cache the reference to the object.
      increment_object_count(object_ids[i], object, true);
    }
  }

  if (all_present) {
    return Status::OK();
  }

  // If we get here, then the objects aren't all currently in use by this
  // client, so we need to send a request to the plasma store.
  RETURN_NOT_OK(SendGetRequest(store_conn_, &object_ids[0], num_objects, timeout_ms));
  std::vector<uint8_t> buffer;
  RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaGetReply, &buffer));
  std::vector<ObjectID> received_object_ids(num_objects);
  std::vector<PlasmaObject> object_data(num_objects);
  PlasmaObject* object;
  std::vector<int> store_fds;
  std::vector<int64_t> mmap_sizes;
  RETURN_NOT_OK(ReadGetReply(buffer.data(), buffer.size(), received_object_ids.data(),
                             object_data.data(), num_objects, store_fds, mmap_sizes));

  // We mmap all of the file descriptors here so that we can avoid look them up
  // in the subsequent loop based on just the store file descriptor and without
  // having to know the relevant file descriptor received from recv_fd.
  for (size_t i = 0; i < store_fds.size(); i++) {
    int fd = recv_fd(store_conn_);
    ARROW_CHECK(fd >= 0);
    lookup_or_mmap(fd, store_fds[i], mmap_sizes[i]);
  }

  for (int64_t i = 0; i < num_objects; ++i) {
    DCHECK(received_object_ids[i] == object_ids[i]);
    object = &object_data[i];
    if (object_buffers[i].data) {
      // If the object was already in use by the client, then the store should
      // have returned it.
      DCHECK_NE(object->data_size, -1);
      // We've already filled out the information for this object, so we can
      // just continue.
      continue;
    }
    // If we are here, the object was not currently in use, so we need to
    // process the reply from the object store.
    if (object->data_size != -1) {
      std::shared_ptr<Buffer> physical_buf;
      if (object->device_num == 0) {
        uint8_t* data = lookup_mmapped_file(object->store_fd);
        physical_buf = std::make_shared<Buffer>(
            data + object->data_offset, object->data_size + object->metadata_size);
      } else {
#ifdef PLASMA_GPU
        std::lock_guard<std::mutex> lock(gpu_mutex);
        auto handle = gpu_object_map.find(object_ids[i]);
        if (handle == gpu_object_map.end()) {
          std::shared_ptr<CudaContext> context;
          RETURN_NOT_OK(manager_->GetContext(object->device_num - 1, &context));
          GpuProcessHandle* obj_handle = new GpuProcessHandle();
          RETURN_NOT_OK(context->OpenIpcBuffer(*object->ipc_handle, &obj_handle->ptr));
          gpu_object_map[object_ids[i]] = obj_handle;
          physical_buf = obj_handle->ptr;
        } else {
          handle->second->client_count += 1;
          physical_buf = handle->second->ptr;
        }
#else
        ARROW_LOG(FATAL) << "Arrow GPU library is not enabled.";
#endif
      }
      // Finish filling out the return values.
      physical_buf = wrap_buffer(object_ids[i], physical_buf);
      object_buffers[i].data = SliceBuffer(physical_buf, 0, object->data_size);
      object_buffers[i].metadata =
          SliceBuffer(physical_buf, object->data_size, object->metadata_size);
      object_buffers[i].device_num = object->device_num;
      // Increment the count of the number of instances of this object that this
      // client is using. Cache the reference to the object.
      increment_object_count(received_object_ids[i], object, true);
    } else {
      // The object was not retrieved.  The caller can detect this condition
      // by checking the boolean value of the metadata/data buffers.
      DCHECK(!object_buffers[i].metadata);
      DCHECK(!object_buffers[i].data);
    }
  }
  return Status::OK();
}

Status PlasmaClient::Impl::Get(const std::vector<ObjectID>& object_ids,
                               int64_t timeout_ms, std::vector<ObjectBuffer>* out) {
  const auto wrap_buffer = [=](const ObjectID& object_id,
                               const std::shared_ptr<Buffer>& buffer) {
    return std::make_shared<PlasmaBuffer>(shared_from_this(), object_id, buffer);
  };
  const size_t num_objects = object_ids.size();
  *out = std::vector<ObjectBuffer>(num_objects);
  return GetBuffers(&object_ids[0], num_objects, timeout_ms, wrap_buffer, &(*out)[0]);
}

Status PlasmaClient::Impl::Get(const ObjectID* object_ids, int64_t num_objects,
                               int64_t timeout_ms, ObjectBuffer* out) {
  const auto wrap_buffer = [](const ObjectID& object_id,
                              const std::shared_ptr<Buffer>& buffer) { return buffer; };
  return GetBuffers(object_ids, num_objects, timeout_ms, wrap_buffer, out);
}

Status PlasmaClient::Impl::UnmapObject(const ObjectID& object_id) {
  auto object_entry = objects_in_use_.find(object_id);
  ARROW_CHECK(object_entry != objects_in_use_.end());
  ARROW_CHECK(object_entry->second->count == 0);

  // Decrement the count of the number of objects in this memory-mapped file
  // that the client is using. The corresponding increment should have
  // happened in plasma_get.
  int fd = object_entry->second->object.store_fd;
  auto entry = mmap_table_.find(fd);
  ARROW_CHECK(entry != mmap_table_.end());
  ARROW_CHECK(entry->second.count >= 1);
  if (entry->second.count == 1) {
    // If no other objects are being used, then unmap the file.
    // We subtract kMmapRegionsGap from the length that was added
    // in fake_mmap in malloc.h, to make the size page-aligned again.
    int err = munmap(entry->second.pointer, entry->second.length - kMmapRegionsGap);
    if (err == -1) {
      return Status::IOError("Error during munmap");
    }
    // Remove the corresponding entry from the hash table.
    mmap_table_.erase(fd);
  } else {
    // If there are other objects being used, decrement the reference count.
    entry->second.count -= 1;
  }
  // Update the in_use_object_bytes_.
  in_use_object_bytes_ -= (object_entry->second->object.data_size +
                           object_entry->second->object.metadata_size);
  DCHECK_GE(in_use_object_bytes_, 0);
  // Remove the entry from the hash table of objects currently in use.
  objects_in_use_.erase(object_id);
  return Status::OK();
}

/// This is a helper method for implementing plasma_release. We maintain a
/// buffer
/// of release calls and only perform them once the buffer becomes full (as
/// judged by the aggregate sizes of the objects). There may be multiple release
/// calls for the same object ID in the buffer. In this case, the first release
/// calls will not do anything. The client will only send a message to the store
/// releasing the object when the client is truly done with the object.
///
/// @param object_id The object ID to attempt to release.
Status PlasmaClient::Impl::PerformRelease(const ObjectID& object_id) {
  // Decrement the count of the number of instances of this object that are
  // being used by this client. The corresponding increment should have happened
  // in PlasmaClient::Get.
  auto object_entry = objects_in_use_.find(object_id);
  ARROW_CHECK(object_entry != objects_in_use_.end());
  object_entry->second->count -= 1;
  ARROW_CHECK(object_entry->second->count >= 0);
  // Check if the client is no longer using this object.
  if (object_entry->second->count == 0) {
    // Tell the store that the client no longer needs the object.
    RETURN_NOT_OK(UnmapObject(object_id));
    RETURN_NOT_OK(SendReleaseRequest(store_conn_, object_id));
  }
  return Status::OK();
}

Status PlasmaClient::Impl::Release(const ObjectID& object_id) {
  // If the client is already disconnected, ignore release requests.
  if (store_conn_ < 0) {
    return Status::OK();
  }
  // Add the new object to the release history.
  release_history_.push_front(object_id);
  // If there are too many bytes in use by the client or if there are too many
  // pending release calls, and there are at least some pending release calls in
  // the release_history list, then release some objects.

  // TODO(wap) Evicition policy only works on host memory, and thus objects
  //           on the GPU cannot be released currently.
  while ((in_use_object_bytes_ > std::min(kL3CacheSizeBytes, store_capacity_ / 100) ||
          release_history_.size() > config_.release_delay) &&
         release_history_.size() > 0) {
    // Perform a release for the object ID for the first pending release.
    RETURN_NOT_OK(PerformRelease(release_history_.back()));
    // Remove the last entry from the release history.
    release_history_.pop_back();
  }
  return Status::OK();
}

Status PlasmaClient::Impl::FlushReleaseHistory() {
  // If the client is already disconnected, ignore the flush.
  if (store_conn_ < 0) {
    return Status::OK();
  }
  while (release_history_.size() > 0) {
    // Perform a release for the object ID for the first pending release.
    RETURN_NOT_OK(PerformRelease(release_history_.back()));
    // Remove the last entry from the release history.
    release_history_.pop_back();
  }
  return Status::OK();
}

// This method is used to query whether the plasma store contains an object.
Status PlasmaClient::Impl::Contains(const ObjectID& object_id, bool* has_object) {
  // Check if we already have a reference to the object.
  if (objects_in_use_.count(object_id) > 0) {
    *has_object = 1;
  } else {
    // If we don't already have a reference to the object, check with the store
    // to see if we have the object.
    RETURN_NOT_OK(SendContainsRequest(store_conn_, object_id));
    std::vector<uint8_t> buffer;
    RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaContainsReply, &buffer));
    ObjectID object_id2;
    DCHECK_GT(buffer.size(), 0);
    RETURN_NOT_OK(
        ReadContainsReply(buffer.data(), buffer.size(), &object_id2, has_object));
  }
  return Status::OK();
}

static void ComputeBlockHash(const unsigned char* data, int64_t nbytes, uint64_t* hash) {
  XXH64_state_t hash_state;
  XXH64_reset(&hash_state, XXH64_DEFAULT_SEED);
  XXH64_update(&hash_state, data, nbytes);
  *hash = XXH64_digest(&hash_state);
}

bool PlasmaClient::Impl::compute_object_hash_parallel(XXH64_state_t* hash_state,
                                                      const unsigned char* data,
                                                      int64_t nbytes) {
  // Note that this function will likely be faster if the address of data is
  // aligned on a 64-byte boundary.
  auto pool = arrow::internal::GetCpuThreadPool();

  const int num_threads = kHashingConcurrency;
  uint64_t threadhash[num_threads + 1];
  const uint64_t data_address = reinterpret_cast<uint64_t>(data);
  const uint64_t num_blocks = nbytes / kBlockSize;
  const uint64_t chunk_size = (num_blocks / num_threads) * kBlockSize;
  const uint64_t right_address = data_address + chunk_size * num_threads;
  const uint64_t suffix = (data_address + nbytes) - right_address;
  // Now the data layout is | k * num_threads * block_size | suffix | ==
  // | num_threads * chunk_size | suffix |, where chunk_size = k * block_size.
  // Each thread gets a "chunk" of k blocks, except the suffix thread.

  std::vector<std::future<void>> futures;
  for (int i = 0; i < num_threads; i++) {
    futures.push_back(pool->Submit(
        ComputeBlockHash, reinterpret_cast<uint8_t*>(data_address) + i * chunk_size,
        chunk_size, &threadhash[i]));
  }
  ComputeBlockHash(reinterpret_cast<uint8_t*>(right_address), suffix,
                   &threadhash[num_threads]);

  for (auto& fut : futures) {
    fut.get();
  }

  XXH64_update(hash_state, reinterpret_cast<unsigned char*>(threadhash),
               sizeof(threadhash));
  return true;
}

uint64_t PlasmaClient::Impl::compute_object_hash(const ObjectBuffer& obj_buffer) {
  DCHECK(obj_buffer.metadata);
  DCHECK(obj_buffer.data);
  XXH64_state_t hash_state;
  if (obj_buffer.device_num != 0) {
    // TODO(wap): Create cuda program to hash data on gpu.
    return 0;
  }
  XXH64_reset(&hash_state, XXH64_DEFAULT_SEED);
  if (obj_buffer.data->size() >= kBytesInMB) {
    compute_object_hash_parallel(
        &hash_state, reinterpret_cast<const unsigned char*>(obj_buffer.data->data()),
        obj_buffer.data->size());
  } else {
    XXH64_update(&hash_state,
                 reinterpret_cast<const unsigned char*>(obj_buffer.data->data()),
                 obj_buffer.data->size());
  }
  XXH64_update(&hash_state,
               reinterpret_cast<const unsigned char*>(obj_buffer.metadata->data()),
               obj_buffer.metadata->size());
  return XXH64_digest(&hash_state);
}

Status PlasmaClient::Impl::Seal(const ObjectID& object_id) {
  // Make sure this client has a reference to the object before sending the
  // request to Plasma.
  auto object_entry = objects_in_use_.find(object_id);

  if (object_entry == objects_in_use_.end()) {
    return Status::PlasmaObjectNonexistent(
        "Seal() called on an object without a reference to it");
  }
  if (object_entry->second->is_sealed) {
    return Status::PlasmaObjectAlreadySealed("Seal() called on an already sealed object");
  }

  object_entry->second->is_sealed = true;
  /// Send the seal request to Plasma.
  static unsigned char digest[kDigestSize];
  RETURN_NOT_OK(Hash(object_id, &digest[0]));
  RETURN_NOT_OK(SendSealRequest(store_conn_, object_id, &digest[0]));
  // We call PlasmaClient::Release to decrement the number of instances of this
  // object
  // that are currently being used by this client. The corresponding increment
  // happened in plasma_create and was used to ensure that the object was not
  // released before the call to PlasmaClient::Seal.
  return Release(object_id);
}

Status PlasmaClient::Impl::Abort(const ObjectID& object_id) {
  auto object_entry = objects_in_use_.find(object_id);
  ARROW_CHECK(object_entry != objects_in_use_.end())
      << "Plasma client called abort on an object without a reference to it";
  ARROW_CHECK(!object_entry->second->is_sealed)
      << "Plasma client called abort on a sealed object";

  // Flush the release history.
  RETURN_NOT_OK(FlushReleaseHistory());
  // Make sure that the Plasma client only has one reference to the object. If
  // it has more, then the client needs to release the buffer before calling
  // abort.
  if (object_entry->second->count > 1) {
    return Status::Invalid("Plasma client cannot have a reference to the buffer.");
  }

  // Send the abort request.
  RETURN_NOT_OK(SendAbortRequest(store_conn_, object_id));
  // Decrease the reference count to zero, then remove the object.
  object_entry->second->count--;
  RETURN_NOT_OK(UnmapObject(object_id));

  std::vector<uint8_t> buffer;
  ObjectID id;
  MessageType type;
  RETURN_NOT_OK(ReadMessage(store_conn_, &type, &buffer));
  return ReadAbortReply(buffer.data(), buffer.size(), &id);
}

Status PlasmaClient::Impl::Delete(const std::vector<ObjectID>& object_ids) {
  RETURN_NOT_OK(FlushReleaseHistory());
  std::vector<ObjectID> not_in_use_ids;
  for (auto& object_id : object_ids) {
    // If the object is in used, skip it.
    if (objects_in_use_.count(object_id) == 0) {
      not_in_use_ids.push_back(object_id);
    }
  }
  if (not_in_use_ids.size() > 0) {
    RETURN_NOT_OK(SendDeleteRequest(store_conn_, not_in_use_ids));
    std::vector<uint8_t> buffer;
    RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaDeleteReply, &buffer));
    DCHECK_GT(buffer.size(), 0);
    std::vector<PlasmaError> error_codes;
    not_in_use_ids.clear();
    RETURN_NOT_OK(
        ReadDeleteReply(buffer.data(), buffer.size(), &not_in_use_ids, &error_codes));
  }
  return Status::OK();
}

Status PlasmaClient::Impl::Evict(int64_t num_bytes, int64_t& num_bytes_evicted) {
  // Send a request to the store to evict objects.
  RETURN_NOT_OK(SendEvictRequest(store_conn_, num_bytes));
  // Wait for a response with the number of bytes actually evicted.
  std::vector<uint8_t> buffer;
  MessageType type;
  RETURN_NOT_OK(ReadMessage(store_conn_, &type, &buffer));
  return ReadEvictReply(buffer.data(), buffer.size(), num_bytes_evicted);
}

Status PlasmaClient::Impl::Hash(const ObjectID& object_id, uint8_t* digest) {
  // Get the plasma object data. We pass in a timeout of 0 to indicate that
  // the operation should timeout immediately.
  std::vector<ObjectBuffer> object_buffers;
  RETURN_NOT_OK(Get({object_id}, 0, &object_buffers));
  // If the object was not retrieved, return false.
  if (!object_buffers[0].data) {
    return Status::PlasmaObjectNonexistent("Object not found");
  }
  // Compute the hash.
  uint64_t hash = compute_object_hash(object_buffers[0]);
  memcpy(digest, &hash, sizeof(hash));
  return Status::OK();
}

Status PlasmaClient::Impl::Subscribe(int* fd) {
  int sock[2];
  // Create a non-blocking socket pair. This will only be used to send
  // notifications from the Plasma store to the client.
  socketpair(AF_UNIX, SOCK_STREAM, 0, sock);
  // Make the socket non-blocking.
  int flags = fcntl(sock[1], F_GETFL, 0);
  ARROW_CHECK(fcntl(sock[1], F_SETFL, flags | O_NONBLOCK) == 0);
  // Tell the Plasma store about the subscription.
  RETURN_NOT_OK(SendSubscribeRequest(store_conn_));
  // Send the file descriptor that the Plasma store should use to push
  // notifications about sealed objects to this client.
  ARROW_CHECK(send_fd(store_conn_, sock[1]) >= 0);
  close(sock[1]);
  // Return the file descriptor that the client should use to read notifications
  // about sealed objects.
  *fd = sock[0];
  return Status::OK();
}

Status PlasmaClient::Impl::GetNotification(int fd, ObjectID* object_id,
                                           int64_t* data_size, int64_t* metadata_size) {
  auto notification = read_message_async(fd);
  if (notification == NULL) {
    return Status::IOError("Failed to read object notification from Plasma socket");
  }
  auto object_info = flatbuffers::GetRoot<ObjectInfo>(notification.get());
  ARROW_CHECK(object_info->object_id()->size() == sizeof(ObjectID));
  memcpy(object_id, object_info->object_id()->data(), sizeof(ObjectID));
  if (object_info->is_deletion()) {
    *data_size = -1;
    *metadata_size = -1;
  } else {
    *data_size = object_info->data_size();
    *metadata_size = object_info->metadata_size();
  }
  return Status::OK();
}

Status PlasmaClient::Impl::Connect(const std::string& store_socket_name,
                                   const std::string& manager_socket_name,
                                   int release_delay, int num_retries) {
  RETURN_NOT_OK(ConnectIpcSocketRetry(store_socket_name, num_retries, -1, &store_conn_));
  if (manager_socket_name != "") {
    RETURN_NOT_OK(
        ConnectIpcSocketRetry(manager_socket_name, num_retries, -1, &manager_conn_));
  } else {
    manager_conn_ = -1;
  }
  config_.release_delay = release_delay;
  in_use_object_bytes_ = 0;
  // Send a ConnectRequest to the store to get its memory capacity.
  RETURN_NOT_OK(SendConnectRequest(store_conn_));
  std::vector<uint8_t> buffer;
  RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaConnectReply, &buffer));
  RETURN_NOT_OK(ReadConnectReply(buffer.data(), buffer.size(), &store_capacity_));
  return Status::OK();
}

Status PlasmaClient::Impl::Disconnect() {
  // NOTE: We purposefully do not finish sending release calls for objects in
  // use, so that we don't duplicate PlasmaClient::Release calls (when handling
  // a SIGTERM, for example).

  // Close the connections to Plasma. The Plasma store will release the objects
  // that were in use by us when handling the SIGPIPE.
  close(store_conn_);
  store_conn_ = -1;
  if (manager_conn_ >= 0) {
    close(manager_conn_);
    manager_conn_ = -1;
  }
  return Status::OK();
}

Status PlasmaClient::Impl::Transfer(const char* address, int port,
                                    const ObjectID& object_id) {
  return SendDataRequest(manager_conn_, object_id, address, port);
}

Status PlasmaClient::Impl::Fetch(int num_object_ids, const ObjectID* object_ids) {
  ARROW_CHECK(manager_conn_ >= 0);
  return SendFetchRequest(manager_conn_, object_ids, num_object_ids);
}

int PlasmaClient::Impl::get_manager_fd() const { return manager_conn_; }

Status PlasmaClient::Impl::Info(const ObjectID& object_id, int* object_status) {
  ARROW_CHECK(manager_conn_ >= 0);

  RETURN_NOT_OK(SendStatusRequest(manager_conn_, &object_id, 1));
  std::vector<uint8_t> buffer;
  RETURN_NOT_OK(PlasmaReceive(manager_conn_, MessageType::PlasmaStatusReply, &buffer));
  ObjectID id;
  RETURN_NOT_OK(ReadStatusReply(buffer.data(), buffer.size(), &id, object_status, 1));
  ARROW_CHECK(object_id == id);
  return Status::OK();
}

Status PlasmaClient::Impl::Wait(int64_t num_object_requests,
                                ObjectRequest* object_requests, int num_ready_objects,
                                int64_t timeout_ms, int* num_objects_ready) {
  ARROW_CHECK(manager_conn_ >= 0);
  ARROW_CHECK(num_object_requests > 0);
  ARROW_CHECK(num_ready_objects > 0);
  ARROW_CHECK(num_ready_objects <= num_object_requests);

  for (int i = 0; i < num_object_requests; ++i) {
    ARROW_CHECK(object_requests[i].type == ObjectRequestType::PLASMA_QUERY_LOCAL ||
                object_requests[i].type == ObjectRequestType::PLASMA_QUERY_ANYWHERE);
  }

  RETURN_NOT_OK(SendWaitRequest(manager_conn_, object_requests, num_object_requests,
                                num_ready_objects, timeout_ms));
  std::vector<uint8_t> buffer;
  RETURN_NOT_OK(PlasmaReceive(manager_conn_, MessageType::PlasmaWaitReply, &buffer));
  RETURN_NOT_OK(
      ReadWaitReply(buffer.data(), buffer.size(), object_requests, &num_ready_objects));

  *num_objects_ready = 0;
  for (int i = 0; i < num_object_requests; ++i) {
    ObjectRequestType type = object_requests[i].type;
    ObjectStatus status = object_requests[i].status;
    switch (type) {
      case ObjectRequestType::PLASMA_QUERY_LOCAL:
        if (status == ObjectStatus::Local) {
          *num_objects_ready += 1;
        }
        break;
      case ObjectRequestType::PLASMA_QUERY_ANYWHERE:
        if (status == ObjectStatus::Local || status == ObjectStatus::Remote) {
          *num_objects_ready += 1;
        } else {
          ARROW_CHECK(status == ObjectStatus::Nonexistent);
        }
        break;
      default:
        ARROW_LOG(FATAL) << "This code should be unreachable.";
    }
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// PlasmaClient

PlasmaClient::PlasmaClient() : impl_(std::make_shared<PlasmaClient::Impl>()) {}

PlasmaClient::~PlasmaClient() {}

Status PlasmaClient::Connect(const std::string& store_socket_name,
                             const std::string& manager_socket_name, int release_delay,
                             int num_retries) {
  return impl_->Connect(store_socket_name, manager_socket_name, release_delay,
                        num_retries);
}

Status PlasmaClient::Create(const ObjectID& object_id, int64_t data_size,
                            const uint8_t* metadata, int64_t metadata_size,
                            std::shared_ptr<Buffer>* data, int device_num) {
  return impl_->Create(object_id, data_size, metadata, metadata_size, data, device_num);
}

Status PlasmaClient::Get(const std::vector<ObjectID>& object_ids, int64_t timeout_ms,
                         std::vector<ObjectBuffer>* object_buffers) {
  return impl_->Get(object_ids, timeout_ms, object_buffers);
}

Status PlasmaClient::Get(const ObjectID* object_ids, int64_t num_objects,
                         int64_t timeout_ms, ObjectBuffer* object_buffers) {
  return impl_->Get(object_ids, num_objects, timeout_ms, object_buffers);
}

Status PlasmaClient::Release(const ObjectID& object_id) {
  return impl_->Release(object_id);
}

Status PlasmaClient::Contains(const ObjectID& object_id, bool* has_object) {
  return impl_->Contains(object_id, has_object);
}

Status PlasmaClient::Abort(const ObjectID& object_id) { return impl_->Abort(object_id); }

Status PlasmaClient::Seal(const ObjectID& object_id) { return impl_->Seal(object_id); }

Status PlasmaClient::Delete(const ObjectID& object_id) {
  return impl_->Delete(std::vector<ObjectID>{object_id});
}

Status PlasmaClient::Delete(const std::vector<ObjectID>& object_ids) {
  return impl_->Delete(object_ids);
}

Status PlasmaClient::Evict(int64_t num_bytes, int64_t& num_bytes_evicted) {
  return impl_->Evict(num_bytes, num_bytes_evicted);
}

Status PlasmaClient::Hash(const ObjectID& object_id, uint8_t* digest) {
  return impl_->Hash(object_id, digest);
}

Status PlasmaClient::Subscribe(int* fd) { return impl_->Subscribe(fd); }

Status PlasmaClient::GetNotification(int fd, ObjectID* object_id, int64_t* data_size,
                                     int64_t* metadata_size) {
  return impl_->GetNotification(fd, object_id, data_size, metadata_size);
}

Status PlasmaClient::Disconnect() { return impl_->Disconnect(); }

Status PlasmaClient::Fetch(int num_object_ids, const ObjectID* object_ids) {
  return impl_->Fetch(num_object_ids, object_ids);
}

Status PlasmaClient::Wait(int64_t num_object_requests, ObjectRequest* object_requests,
                          int num_ready_objects, int64_t timeout_ms,
                          int* num_objects_ready) {
  return impl_->Wait(num_object_requests, object_requests, num_ready_objects, timeout_ms,
                     num_objects_ready);
}

Status PlasmaClient::Transfer(const char* addr, int port, const ObjectID& object_id) {
  return impl_->Transfer(addr, port, object_id);
}

Status PlasmaClient::Info(const ObjectID& object_id, int* object_status) {
  return impl_->Info(object_id, object_status);
}

int PlasmaClient::get_manager_fd() const { return impl_->get_manager_fd(); }

Status PlasmaClient::FlushReleaseHistory() { return impl_->FlushReleaseHistory(); }

bool PlasmaClient::IsInUse(const ObjectID& object_id) {
  return impl_->IsInUse(object_id);
}

Status PlasmaClient::CreateQueue(const ObjectID& object_id, int64_t data_size,
                            const uint8_t* metadata, int64_t metadata_size,
                            std::shared_ptr<Buffer>* data, int device_num) {
  return impl_->Create(object_id, data_size, metadata, metadata_size, data, device_num, ObjectType_Queue);
}

}  // namespace plasma
