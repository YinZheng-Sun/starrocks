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


#define CREATE_ORC_PRIMITIVE_TYPE(orctype) \
    std::unique_ptr<Type> field_type = createPrimitiveType(orctype); \
    _schema->addStructField(field_names[i], std::move(field_type));  \
    break;                                                           \

/*
**
*/


Status OrcChunkWriter::_make_schema() {
    if (_schema) {
        _schema.reset();
    }
    _schema = createStructType();
    int column_size = _type_descs.size();
    static_assert(column_size == field_names.size());
    for (int i = 0; i < column_size; ++i) {
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
        }
    }
    return Status::OK();
}

Status OrcChunkWriter::init_writer(const std::vector<TypeDescriptor>& type_descs, std::unique<orc::OutputStream> outputstream) {
    _type_descs = type_descs;

    // try create orc writer
    try {
        _writer = orc::createWriter( , std::move(outputstream), _writer_options)
    } catch (std::exception& e) {
        return Status::InternalError("Init OrcReader failed. reason = {}", e.what());
    }
    return Status::OK();
}



OrcChunkWriter::OrcChunkWriter() {
    _batch = _writer->createRowBatch(_batchsize);

}

Status OrcChunkWriter::write(Chunk* chunk) {
    int column_size = _type_descs.size();
    for (int i = 0; i < column_size; ++i) {

    }
    return Status::OK();
}

void OrcChunkWriter::close() {
    _writer->close();
}


int64_t ChunkWriter::estimated_buffered_bytes() const {
    return 0;
}

} // namespace starrocks
