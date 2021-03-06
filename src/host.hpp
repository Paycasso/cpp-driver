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

#ifndef __CASS_HOST_HPP_INCLUDED__
#define __CASS_HOST_HPP_INCLUDED__

#include "address.hpp"

namespace cass {

struct Host {
  Address address;

  Host() {}

  Host(const Address& address)
      : address(address) {}
};

inline bool operator<(const Host& a, const Host& b) {
  return a.address < b.address;
}

inline bool operator==(const Host& a, const Host& b) {
  return a.address == b.address;
}

} // namespace cass

namespace std {

template <>
struct hash<cass::Host> {
  typedef cass::Host argument_type;
  typedef size_t result_type;
  size_t operator()(const cass::Host& h) const {
    std::hash<cass::Address> hash_func;
    return hash_func(h.address);
  }
};

} // namespace std

#endif
