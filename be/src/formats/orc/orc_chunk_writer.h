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
#include "fs/fs.h"

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter_bank.h"
#include "formats/orc/orc_mapping.h"
#include "formats/orc/column_writer.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "formats/orc/utils.h"

namespace starrocks {

class OrcColumnWriter;

// OrcChunkWriter is a bridge between apache/orc file and chunk, wraps orc::writer
// Write chunks into buffer. Flush on closing.
class OrcOutputStream : public orc::OutputStream {
public:
    OrcOutputStream(std::unique_ptr<starrocks::WritableFile> wfile);

    ~OrcOutputStream() override;
    
    uint64_t getLength() const override;
    
    uint64_t getNaturalWriteSize() const override;
    
    void write(const void * buf, size_t length) override;

    void close() override;

    const std::string& getName() const override;
    
private:
    std::unique_ptr<starrocks::WritableFile> _wfile;
    bool _is_closed = false;
};

class OrcBuildHelper {
public:
    static StatusOr<std::unique_ptr<orc::Type>> make_schema(
            const std::vector<std::string>& file_column_names, const std::vector<TypeDescriptor>& type_descs);

private:

};

class OrcChunkWriter {
public:
    OrcChunkWriter(std::vector<TypeDescriptor>& type_descs, OrcOutputStream *output_stream, std::unique_ptr<orc::Type> schema) : _schema(std::move(schema)), _type_descs(type_descs), _output_stream(output_stream) {};
        
    Status set_compression(const TCompressionType::type& compression_type);

    Status write(Chunk* chunk);

    void close();

    static StatusOr<std::unique_ptr<orc::Type>> make_schema(
            const std::vector<std::string>& file_column_names, const std::vector<TypeDescriptor>& type_descs);

private:

    static StatusOr<std::unique_ptr<orc::Type>> _get_orc_type(const TypeDescriptor& type_desc);

    Status _write_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type_desc);
    

    template <LogicalType Type, typename VectorBatchType>
    void _write_numbers(orc::ColumnVectorBatch & orc_column, ColumnPtr& column);

    void _write_strings(orc::ColumnVectorBatch & orc_column, ColumnPtr& column);

    template <LogicalType DecimalType, typename ConvertFunc>
    void _write_decimals(orc::ColumnVectorBatch & orc_column, ColumnPtr& column, ConvertFunc convert, int precision, int scale);

    template <LogicalType DecimalType, typename VectorBatchType, typename T>
    void _write_decimal32or64or128(orc::ColumnVectorBatch & orc_column, ColumnPtr& column, int precision, int scale);

    void _write_datetimes(orc::ColumnVectorBatch & orc_column, ColumnPtr& column);

    void _write_timestamps(orc::ColumnVectorBatch & orc_column, ColumnPtr& column);

    Status _write_array_column(orc::ColumnVectorBatch & orc_column, ColumnPtr& column, const TypeDescriptor& type);

    Status _write_struct_column(orc::ColumnVectorBatch & orc_column, ColumnPtr& column, const TypeDescriptor& type);
    
    Status _write_map_column(orc::ColumnVectorBatch & orc_column, ColumnPtr& column, const TypeDescriptor& type);
    
    
    std::unique_ptr<orc::Writer> _writer;               //负责将数据写入到buffer
    std::vector<TypeDescriptor> _type_descs;            //chunk中各个列的type信息
    std::vector<string> _field_names;                   //chunk中各列的column name
    std::vector<SlotDescriptor*> _slot_descriptors;
    orc::WriterOptions _writer_options;                 //用于配置写入的参数，如压缩算法，压缩等级等
    std::unique_ptr<orc::ColumnVectorBatch> _batch;  
    std::unique_ptr<orc::Type>  _schema;                //维护表的schema
    OrcOutputStream* _output_stream;
    // std::vector<std::unique_ptr<OrcColumnWriter>> _column_writers;
};
} // namespace starrocks