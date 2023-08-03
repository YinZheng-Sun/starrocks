// Copyright 2023-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#pragma once

#include <boost/algorithm/string.hpp>
#include <orc/OrcFile.hh>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter_bank.h"
#include "formats/orc/orc_mapping.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"

namespace starrocks {

// OrcChunkWriter is a bridge between apache/orc file and chunk, wraps orc::writer
// Write chunks into buffer. Flush on closing.

class ORCOutputStream : public orc::OutputStream {
public:
    ORCOutputStream(std::unique_ptr<starrocks::WritableFile> wfile);

    ~ORCOutputStream() override;
    
    uint64_t getLength() const override;
    
    uint64_t getNautralWriteSize() const override {}
    
    void write(const void * buf, size_t length) override;

    void close() override;

    const std::string& getName() const override;

private:
    std::unique_ptr<starrocks::WritableFile> _wfile;
    bool _is_closed = false;
}


class OrcChunkWriter {
public:
    OrcChunkWriter(const std::vector<TypeDescriptor>& type_descs);
    Status init_writer();
    
    Status write(Chunk* chunk);

    void close();


private:
    Status _make_schema();

    uint32_t _batchsize = 4096;
    std::unique_ptr<orc::Writer> _writer;               //负责将数据写入到buffer
    std::vector<TypeDescriptor> _type_descs;            //chunk中各个列的type信息
    std::vector<string> _field_names;                   //chunk中各列的column name
    std::vector<SlotDescriptor*> _slot_descriptors;
    orc::WriterOptions _writer_options;                 //用于配置写入的参数，如压缩算法，压缩等级等
    std::unique_ptr<orc::ColumnVectorBatch> _batch;  
    std::unique_ptr<orc::Type>  _schema;                //维护表的schema
    ORCOutputStream _output_stream;
    std::vector<std::unique_ptr<ORCColumnWriter>> _column_writers;
    // std::vector<FillCVBFunction> _fill_functions;    //负责将对应的Column填充到CVB中
};
} // namespace starrocks