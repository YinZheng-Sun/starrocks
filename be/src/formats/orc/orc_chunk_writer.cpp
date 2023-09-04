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
#include "formats/orc/orc_chunk_writer.h"

namespace starrocks {

uint64_t stripeSize = 16 * 1024;      // 16K
uint64_t compressionBlockSize = 1024; // 1k


#define CREATE_ORC_PRIMITIVE_TYPE(srType, orcType)                                  \
    case srType:                                                                    \
    {                                                                               \
        std::unique_ptr<orc::Type> field_type = orc::createPrimitiveType(orcType);  \
        schema->addStructField(file_column_names[i], std::move(field_type));        \
    }                                                                               \
    break;

/*
** ORCOutputStream
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

void OrcChunkWriter::set_compression(const TCompressionType::type& compression_type) {
    switch (compression_type) {
    case TCompressionType::SNAPPY: {
        _writer_options.setCompression(orc::CompressionKind::CompressionKind_SNAPPY);
        break;
    }
    case TCompressionType::ZLIB: {
        _writer_options.setCompression(orc::CompressionKind::CompressionKind_ZLIB);
        break;
    }
    case TCompressionType::ZSTD: {
        _writer_options.setCompression(orc::CompressionKind::CompressionKind_ZSTD);
        break;
    }
    case TCompressionType::LZ4: {
        _writer_options.setCompression(orc::CompressionKind::CompressionKind_LZ4);
        break;
    }
    case TCompressionType::LZO: {
        _writer_options.setCompression(orc::CompressionKind::CompressionKind_LZO);
        break;
    }
    default:
        _writer_options.setCompression(orc::CompressionKind::CompressionKind_NONE);
    }
}


Status OrcChunkWriter::init_writer() {
    _writer = orc::createWriter(*_schema, _output_stream, _writer_options);
    return Status::OK();
}

Status OrcChunkWriter::write(Chunk* chunk) {
    if (!_writer) {
        init_writer();
    }
    size_t num_rows = chunk->num_rows();
    size_t column_size = chunk->num_columns();

    auto columns = chunk->columns();

    std::unique_ptr<orc::ColumnVectorBatch> batch = _writer->createRowBatch(config::vector_chunk_size);
    orc::StructVectorBatch & root = dynamic_cast<orc::StructVectorBatch &>(*batch);


    for (size_t i = 0; i < column_size; ++i) {
        _write_column(*root.fields[i], columns[i], _type_descs[i]);
    }

    root.numElements = num_rows;
    _writer->add(*batch);
    return Status::OK();
}

void OrcChunkWriter::_write_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, TypeDescriptor& type_desc) {
    switch (type_desc.type) {
        case TYPE_BOOLEAN:
        {
            _write_numbers<TYPE_BOOLEAN, orc::LongVectorBatch>(orc_column, column);
            break;
        }
        case TYPE_TINYINT:
        {
            _write_numbers<TYPE_TINYINT, orc::LongVectorBatch>(orc_column, column);
            break;
        }
        case TYPE_SMALLINT:
        {
            _write_numbers<TYPE_SMALLINT, orc::LongVectorBatch>(orc_column, column);
            break;
        }
        case TYPE_INT:
        {
            _write_numbers<TYPE_INT, orc::LongVectorBatch>(orc_column, column);
            break;
        }
        case TYPE_BIGINT:
        {
            _write_numbers<TYPE_BIGINT, orc::LongVectorBatch>(orc_column, column);
            break;
        }
        case TYPE_LARGEINT:
        {
            // _write_numbers<TYPE_BIGINT, orc::DoubleVectorBatch>(orc_column, column, [] (const int64 & value) { return value; });
            break;
        }
        case TYPE_FLOAT:
        {
            _write_numbers<TYPE_FLOAT, orc::DoubleVectorBatch>(orc_column, column);
        }
        case TYPE_DOUBLE:
        {
            _write_numbers<TYPE_DOUBLE, orc::DoubleVectorBatch>(orc_column, column);
        }
        case TYPE_CHAR: [[fallthrough]];
        case TYPE_VARCHAR:
        {
            _write_strings(orc_column, column);
            break;
        }
        case TYPE_DECIMAL:
        {
            _write_decimals<TYPE_DECIMAL>(orc_column, column, [] (const double & value) { return value; }, type_desc.precision, type_desc.scale);
            break;
        }
        default:
            break;
    }
}

template <LogicalType Type, typename VectorBatchType, typename ConvertFunc>
void OrcChunkWriter::_write_numbers(orc::ColumnVectorBatch & orc_column, ColumnPtr& column, ConvertFunc convert) {
    auto & number_orc_column = dynamic_cast<VectorBatchType &>(orc_column);
    number_orc_column.resize(column->size());
    number_orc_column.notNull.resize(column->size());

    if (column->is_nullable()) {
        auto c = ColumnHelper::as_raw_column<NullableColumn>(column);
        auto* nulls = c->null_column()->get_data().data();
        auto* values = ColumnHelper::cast_to_raw<Type>(c->data_column())->get_data().data();
        
        for (size_t i = 0; i < column->size(); ++i) {
            if (nulls[i]) {
                number_orc_column.notNull[i] = 0;
                continue;
            }
            number_orc_column.notNull[i] = 1;
            number_orc_column.data[i] = values[i];
        }
        orc_column.hasNulls = true;
    } else {
        auto* values = ColumnHelper::cast_to_raw<Type>(column)->get_data().data();
        for (size_t i = 0; i < column->size(); ++i) {
            number_orc_column.notNull[i] = 1;
            number_orc_column.data[i] = values[i];
        }
    }
    number_orc_column.numElements = column->size();
}

// template <LogicalType Type>
void OrcChunkWriter::_write_strings(orc::ColumnVectorBatch & orc_column, ColumnPtr& column) {
    auto & string_orc_column = dynamic_cast<orc::StringVectorBatch &>(orc_column);
    string_orc_column.resize(column->size());
    string_orc_column.notNull.resize(column->size());

    if (column->is_nullable()) {
        auto* c = ColumnHelper::as_raw_column<NullableColumn>(column);
        auto* nulls = c->null_column()->get_data().data();
        auto* values = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(c->data_column());

        for (size_t i = 0; i < column->size(); ++i) {
            if (nulls[i]) {
                string_orc_column.notNull[i] = 0;
                continue;
            }
            string_orc_column.notNull[i] = 1;
            auto slice = values->get_slice(i);
            string_orc_column.data[i] = const_cast<char *>(slice.get_data());
            string_orc_column.length[i] = slice.get_size();
        }
        orc_column.hasNulls = true;
    } else {
        auto* str_column = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(column);

        for (size_t i = 0; i < column->size(); ++i) {
            string_orc_column.notNull[i] = 1;
            auto slice = str_column->get_slice(i);

            string_orc_column.data[i] = const_cast<char *>(slice.get_data());
            string_orc_column.length[i] = string_orc_column.length[i] = slice.get_size();
        }
    }
    string_orc_column.numElements = column->size();
}

template <LogicalType DecimalType, typename ConvertFunc>
void OrcChunkWriter::_write_decimals(orc::ColumnVectorBatch & orc_column, ColumnPtr& column, ConvertFunc convert, int precision, int scale) {
    auto & decimal_orc_column = dynamic_cast<orc::Decimal64VectorBatch &>(orc_column);

    decimal_orc_column.resize(column->size());
    decimal_orc_column.notNull.resize(column->size());

    decimal_orc_column.precision = precision;
    decimal_orc_column.scale = scale;

    if (column->is_nullable()) {
        auto c = ColumnHelper::as_raw_column<NullableColumn>(column);
        auto* nulls = c->null_column()->get_data().data();
        auto* values = reinterpret_cast<int128_t*>(down_cast<DecimalColumn*>(c->data_column().get())->get_data().data());
        
        for (size_t i = 0; i < column->size(); ++i) {
            if (nulls[i]) {
                decimal_orc_column.notNull[i] = 0;
                continue;
            }
            decimal_orc_column.notNull[i] = 1;
            decimal_orc_column.values[i] = convert(values[i]);
        }
        orc_column.hasNulls = true;
    } else {
        auto* values = reinterpret_cast<int128_t*>(down_cast<DecimalColumn*>(column.get())->get_data().data());
        for (size_t i = 0; i < column->size(); ++i) {
            decimal_orc_column.notNull[i] = 1;
            decimal_orc_column.values[i] = convert(values[i]);
        }
    }
}



void OrcChunkWriter::close() {
    _writer->close();
}

std::unique_ptr<orc::Type> OrcBuildHelper::make_schema(
            const std::vector<std::string>& file_column_names, const std::vector<TypeDescriptor>& type_descs) {
    auto schema = orc::createStructType();
    for (size_t i = 0; i < type_descs.size(); ++i) {
        switch (type_descs[i].type) {
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_BOOLEAN, orc::TypeKind::BOOLEAN);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_TINYINT, orc::TypeKind::BYTE);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_UNSIGNED_TINYINT, orc::TypeKind::BYTE);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_SMALLINT, orc::TypeKind::SHORT);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_UNSIGNED_SMALLINT, orc::TypeKind::SHORT);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_INT, orc::TypeKind::INT);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_UNSIGNED_INT, orc::TypeKind::INT);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_BIGINT, orc::TypeKind::LONG);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_LARGEINT, orc::TypeKind::LONG);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_FLOAT, orc::TypeKind::FLOAT);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_DISCRETE_DOUBLE, orc::TypeKind::DOUBLE);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_DOUBLE, orc::TypeKind::DOUBLE);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_CHAR, orc::TypeKind::STRING);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_BINARY, orc::TypeKind::BINARY);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_DATE, orc::TypeKind::DATE);
            CREATE_ORC_PRIMITIVE_TYPE(TYPE_VARCHAR, orc::TypeKind::STRING);
            case TYPE_DECIMAL:
            {
                std::unique_ptr<orc::Type> field_type = orc::createDecimalType(type_descs[i].precision, type_descs[i].scale);
                schema->addStructField(file_column_names[i], std::move(field_type));
            }
            break;
            default:
            {
                // throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} is not supported for ORC output format", type->getName());
            }
        }
    }
    return schema;
}
} // namespace starrocks
