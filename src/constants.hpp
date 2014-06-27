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

#ifndef __CASS_CONSTANTS_HPP_INCLUDED__
#define __CASS_CONSTANTS_HPP_INCLUDED__

#define CQL_OPCODE_ERROR 0x00
#define CQL_OPCODE_STARTUP 0x01
#define CQL_OPCODE_READY 0x02
#define CQL_OPCODE_AUTHENTICATE 0x03
#define CQL_OPCODE_CREDENTIALS 0x04
#define CQL_OPCODE_OPTIONS 0x05
#define CQL_OPCODE_SUPPORTED 0x06
#define CQL_OPCODE_QUERY 0x07
#define CQL_OPCODE_RESULT 0x08
#define CQL_OPCODE_PREPARE 0x09
#define CQL_OPCODE_EXECUTE 0x0A
#define CQL_OPCODE_REGISTER 0x0B
#define CQL_OPCODE_EVENT 0x0C
#define CQL_OPCODE_BATCH 0x0D

// TODO(mpenick): We need to expose these (not as "CQL")
#define CQL_ERROR_SERVER_ERROR 0x0000
#define CQL_ERROR_PROTOCOL_ERROR 0x000A
#define CQL_ERROR_BAD_CREDENTIALS 0x0100
#define CQL_ERROR_UNAVAILABLE 0x1000
#define CQL_ERROR_OVERLOADED 0x1001
#define CQL_ERROR_IS_BOOTSTRAPPING 0x1002
#define CQL_ERROR_TRUNCATE_ERROR 0x1003
#define CQL_ERROR_WRITE_TIMEOUT 0x1100
#define CQL_ERROR_READ_TIMEOUT 0x1200
#define CQL_ERROR_SYNTAX_ERROR 0x2000
#define CQL_ERROR_UNAUTHORIZED 0x2100
#define CQL_ERROR_INVALID_QUERY 0x2200
#define CQL_ERROR_CONFIG_ERROR 0x2300
#define CQL_ERROR_ALREADY_EXISTS 0x2400
#define CQL_ERROR_UNPREPARED 0x2500

#define CASS_RESULT_KIND_VOID 1
#define CASS_RESULT_KIND_ROWS 2
#define CASS_RESULT_KIND_SET_KEYSPACE 3
#define CASS_RESULT_KIND_PREPARED 4
#define CASS_RESULT_KIND_SCHEMA_CHANGE 5

#define CASS_RESULT_FLAG_GLOBAL_TABLESPEC 1
#define CASS_RESULT_FLAG_HAS_MORE_PAGES 2
#define CASS_RESULT_FLAG_NO_METADATA 4

enum RetryType { RETRY_WITH_CURRENT_HOST, RETRY_WITH_NEXT_HOST };

#endif