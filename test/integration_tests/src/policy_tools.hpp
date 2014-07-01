#pragma once
#include "test_utils.hpp"

struct policy_tool {

  std::map<CassInet, int> coordinators;

  void show_coordinators();	// show what queries went to what node IP.
  void reset_coordinators();
  
  void
  init(
       CassSession* session,
       int n,
       CassConsistency cl,
       bool batch = false);

  CassError
  init_return_error(
                    CassSession* session,
                    int n,
                    CassConsistency cl,
                    bool batch = false);
    
  void
  create_schema(
                CassSession* session,
                int replicationFactor);

  void
  add_coordinator(CassInet coord_addr);

  void
  assertQueried(
                CassInet coord_addr,
                int n);

  void
  assertQueriedAtLeast(
                       CassInet coord_addr,
                       int n);

  void
  query(
        CassSession* session,
        int n,
        CassConsistency cl);

  CassError
  query_return_error(
                     CassSession* session,
                     int n,
                     CassConsistency cl);
};