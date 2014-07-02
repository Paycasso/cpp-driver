/*
  Copyright 2014 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include "execute_request.hpp"

namespace cass {

int32_t ExecuteRequest::encode(int version, BufferValueVec* bufs) const {
  assert(version == 2);

  uint8_t flags = 0;
  int32_t length = 0;

    // <id> [short bytes] + <consistency> [short] + <flags> [byte]
  int32_t query_buf_size = sizeof(uint16_t) + prepared_id().size() +
                           sizeof(uint16_t) + sizeof(uint8_t);
  int32_t paging_buf_size = 0;

  if (values_count() > 0) { // <values> = <n><value_1>...<value_n>
    query_buf_size += sizeof(uint16_t); // <n> [short]
    flags |= CASS_QUERY_FLAG_VALUES;
  }

  if (page_size() >= 0) {
    paging_buf_size += sizeof(int32_t); // [int]
    flags |= CASS_QUERY_FLAG_PAGE_SIZE;
  }

  if (!paging_state().empty()) {
    paging_buf_size += sizeof(int32_t) + paging_state().size(); // [bytes]
    flags |= CASS_QUERY_FLAG_PAGING_STATE;
  }

  if (serial_consistency() != 0) {
    paging_buf_size += sizeof(uint16_t); // [short]
    flags |= CASS_QUERY_FLAG_SERIAL_CONSISTENCY;
  }

  {
    bufs->push_back(BufferValue(query_buf_size));
    length += query_buf_size;

    BufferValue& buf = bufs->back();
    size_t pos = buf.encode_string(0,
                                 prepared_id().data(),
                                 prepared_id().size());
    pos = buf.encode_uint16(pos, consistency());
    pos = buf.encode_byte(pos, flags);

    if (values_count() > 0) {
      buf.encode_uint16(pos, values_count());
      length += encode_values(version, bufs);
    }
  }

  if (paging_buf_size > 0) {
    bufs->push_back(BufferValue(paging_buf_size));
    length += paging_buf_size;

    BufferValue& buf = bufs->back();
    size_t pos = 0;

    if (page_size() >= 0) {
      pos = buf.encode_int32(pos, page_size());
    }

    if (!paging_state().empty()) {
      pos = buf.encode_bytes(pos, paging_state().data(), paging_state().size());
    }

    if (serial_consistency() != 0) {
      pos = buf.encode_uint16(pos, serial_consistency());
    }
  }

  return length;
}

} // namespace cass