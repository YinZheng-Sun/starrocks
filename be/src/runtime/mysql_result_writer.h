// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mysql_result_writer.h

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

#pragma once

#include "common/statusor.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class ExprContext;
class MysqlRowBuffer;
class BufferControlBlock;
class RuntimeProfile;
using TFetchDataResultPtr = std::unique_ptr<TFetchDataResult>;
using TFetchDataResultPtrs = std::vector<TFetchDataResultPtr>;
// convert the row batch to mysql protocol row
class MysqlResultWriter final : public ResultWriter {
public:
    MysqlResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                      RuntimeProfile* parent_profile);

    ~MysqlResultWriter() override;

    Status init(RuntimeState* state) override;

    Status append_chunk(vectorized::Chunk* chunk) override;

    Status close() override;

    StatusOr<TFetchDataResultPtrs> process_chunk(vectorized::Chunk* chunk) override;

    StatusOr<bool> try_add_batch(TFetchDataResultPtrs& results) override;

private:
    void _init_profile();
    // this function is only used in non-pipeline engine
    StatusOr<TFetchDataResultPtr> _process_chunk(vectorized::Chunk* chunk);

    BufferControlBlock* _sinker;
    const std::vector<ExprContext*>& _output_expr_ctxs;
    MysqlRowBuffer* _row_buffer;

    RuntimeProfile* _parent_profile; // parent profile from result sink. not owned
    // total time cost on append chunk operation
    RuntimeProfile::Counter* _append_chunk_timer = nullptr;
    // tuple convert timer, child timer of _append_chunk_timer
    RuntimeProfile::Counter* _convert_tuple_timer = nullptr;
    // file write timer, child timer of _append_chunk_timer
    RuntimeProfile::Counter* _result_send_timer = nullptr;
    // number of sent rows
    RuntimeProfile::Counter* _sent_rows_counter = nullptr;

    const size_t _max_row_buffer_size = 1024 * 1024 * 1024;
};

} // namespace starrocks
