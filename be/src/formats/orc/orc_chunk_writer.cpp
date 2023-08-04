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

#include "formats/orc/orc_chunk_writer.h"

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr.h"
#include "util/defer_op.h"

namespace starrocks {

uint64_t stripeSize = 16 * 1024;      // 16K
uint64_t compressionBlockSize = 1024; // 1k


#define CREATE_ORC_PRIMITIVE_TYPE(orctype)                           \
   {                                                                 \
    std::unique_ptr<orc::Type> field_type = orc::createPrimitiveType(orctype); \
    _schema->addStructField(_field_names[i], std::move(field_type));  \
    break;                                                           \
   }
/*
**   ORCOutputStream
*/

OrcOutputStream::OrcOutputStream(std::unique_ptr<starrocks::WritableFile> wfile) : _wfile(std::move(wfile)) {}

OrcOutputStream::~OrcOutputStream() {
    
}

uint64_t OrcOutputStream::getLength() const {
    return _wfile->size();
}

uint64_t OrcOutputStream::getNaturalWriteSize() const {
    return 0;
}

const std::string& OrcOutputStream::getName() const {
    return _wfile->filename();
}

void OrcOutputStream::write(const void* buf, size_t length)
{
    if (_is_closed) {
        LOG(WARNING) << "The output stream is closed but there are still inputs";
        return;
    }
    const char* ch = reinterpret_cast<const char*>(buf);
    Status st = _wfile->append(Slice(ch, length));
    if (!st.ok()) {
        LOG(WARNING) << "write to orc output stream failed: " << st;
    }
    return;
}

void OrcOutputStream::close() {
    if (_is_closed) {
        return;
    }
    Status st = _wfile->close();
    if (!st.ok()) {
        LOG(WARNING) << "close orc output stream failed: " << st;
        return;
    }
    _is_closed = true;
    return;
}

Status OrcChunkWriter::_make_schema() {
    if (_schema) {
        _schema.reset();
    }
    _schema = orc::createStructType();
    size_t column_size = _type_descs.size();
    // static_assert(column_size == _field_names.size());
    for (size_t i = 0; i < column_size; ++i) {
        TypeDescriptor type_desc = _type_descs[i];
        switch (type_desc.type) {
        case TYPE_INT:
            CREATE_ORC_PRIMITIVE_TYPE(orc::TypeKind::INT);
        case TYPE_FLOAT:
            CREATE_ORC_PRIMITIVE_TYPE(orc::TypeKind::FLOAT);
        case TYPE_BOOLEAN:
            CREATE_ORC_PRIMITIVE_TYPE(orc:: TypeKind::BOOLEAN);
        case TYPE_CHAR:
            CREATE_ORC_PRIMITIVE_TYPE(orc::TypeKind::CHAR);
        case TYPE_BINARY:
            CREATE_ORC_PRIMITIVE_TYPE(orc::TypeKind::BINARY);
        case TYPE_DATE:
            CREATE_ORC_PRIMITIVE_TYPE(orc::TypeKind::DATE);
        case TYPE_DOUBLE:
            CREATE_ORC_PRIMITIVE_TYPE(orc::TypeKind::DOUBLE);
        case TYPE_VARCHAR:
            CREATE_ORC_PRIMITIVE_TYPE(orc::TypeKind::VARCHAR);
        default:
            break;
        }
    }
    return Status::OK();
}

Status OrcChunkWriter::init_writer() {
    // try create orc writer
    if (!_schema) {
        _make_schema();
    }
    try {
        _writer = orc::createWriter(*_schema, _output_stream, _writer_options);
    } catch (std::exception& e) {
        return Status::InternalError("Init Orc Writer failed");
    }
    return Status::OK();
}





Status OrcChunkWriter::write(Chunk* chunk) {
    if (!_writer) {
        init_writer();
    }
    size_t column_num = chunk->num_columns();
    // size_t rows_num = chunk->num_rows();

    // std::unique_ptr<orc::ColumnVectorBatch> batch = _writer->createRowBatch(getMaxColumnSize(chunk));
    // orc::StructVectorBatch & root = dynamic_cast<orc::StructVectorBatch &>(*batch);

    for (size_t i = 0; i < column_num; ++i) {
        
    }
    return Status::OK();
}

void OrcChunkWriter::close() {
    _writer->close();
}

} // namespace starrocks
