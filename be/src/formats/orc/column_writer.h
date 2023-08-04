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

#include <orc/OrcFile.hh>
#include <utility>

#include "formats/orc/orc_chunk_writer.h"
#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "common/status.h"
#include "common/statusor.h"
#include "formats/orc/orc_mapping.h"
#include "runtime/types.h"
#include "types/logical_type.h"

namespace starrocks {

class OrcChunkWriter;

class OrcColumnWriter {
public:
    OrcColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* writer)
            : _type(type), _orc_type(orc_type), _nullable(nullable), _writer(writer) {}
    virtual ~OrcColumnWriter() = default;
    virtual Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) = 0;

    static StatusOr<std::unique_ptr<OrcColumnWriter>> create(const TypeDescriptor& type, const orc::Type* orc_type,
                                                             bool nullable, const OrcMappingPtr& orc_mapping,
                                                             OrcChunkWriter* writer);
    const orc::Type* get_orc_type() { return _orc_type; }

protected:
    const TypeDescriptor& _type;
    const orc::Type* _orc_type;
    bool _nullable;
    OrcChunkWriter* _writer;
};

class PrimitiveColumnWriter : public OrcColumnWriter {
public:
    PrimitiveColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* writer)
            : OrcColumnWriter(type, orc_type, nullable, writer) {}
    ~PrimitiveColumnWriter() override = default;
};

class BooleanColumnWriter : public PrimitiveColumnWriter {
public:
    BooleanColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* writer)
            : PrimitiveColumnWriter(type, orc_type, nullable, writer) {}
    ~BooleanColumnWriter() override = default;
    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
};

template <LogicalType Type>
class IntColumnWriter : public PrimitiveColumnWriter {
public:
    IntColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* Writer)
            : PrimitiveColumnWriter(type, orc_type, nullable, Writer) {}
    ~IntColumnWriter() override = default;
    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

private:
    template <typename OrcColumnVectorBatch>
    Status _fill_cvb_from_int_column_with_null(OrcColumnVectorBatch* cvb, ColumnPtr& col);

    template <typename OrcColumnVectorBatch>
    Status _fill_cvb_from_int_column(OrcColumnVectorBatch* cvb, starrocks::ColumnPtr& col);
};

// template <LogicalType Type>
// class FloatColumnWriter : public PrimitiveColumnWriter {
// public:
//     FloatColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* Writer)
//             : PrimitiveColumnWriter(type, orc_type, nullable, Writer) {}
//     ~FloatColumnWriter() override = default;

//     Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
// };

// class DecimalColumnWriter : public PrimitiveColumnWriter {
// public:
//     DecimalColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* Writer)
//             : PrimitiveColumnWriter(type, orc_type, nullable, Writer) {}
//     ~DecimalColumnWriter() override = default;

//     Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

// private:
//     void _fill_decimal_column_from_orc_decimal64(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from,
//                                                  size_t size);

//     void _fill_decimal_column_from_orc_decimal128(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from,
//                                                   size_t size);

//     void _fill_decimal_column_with_null_from_orc_decimal64(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col,
//                                                            size_t from, size_t size);

//     void _fill_decimal_column_with_null_from_orc_decimal128(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col,
//                                                             size_t from, size_t size);
// };

// template <LogicalType DecimalType>
// class Decimal32Or64Or128ColumnWriter : public PrimitiveColumnWriter {
// public:
//     Decimal32Or64Or128ColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable,
//                                    OrcChunkWriter* Writer)
//             : PrimitiveColumnWriter(type, orc_type, nullable, Writer) {}
//     ~Decimal32Or64Or128ColumnWriter() override = default;

//     Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

// private:
//     inline void _fill_decimal_column_from_orc_decimal64_or_decimal128(orc::ColumnVectorBatch* cvb, ColumnPtr& col,
//                                                                       size_t from, size_t size);

//     template <typename T>
//     inline void _fill_decimal_column_generic(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size);
// };

// class StringColumnWriter : public PrimitiveColumnWriter {
// public:
//     StringColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* Writer)
//             : PrimitiveColumnWriter(type, orc_type, nullable, Writer) {}
//     ~StringColumnWriter() override = default;

//     Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
// };

// class VarbinaryColumnWriter : public PrimitiveColumnWriter {
// public:
//     VarbinaryColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* Writer)
//             : PrimitiveColumnWriter(type, orc_type, nullable, Writer) {}
//     ~VarbinaryColumnWriter() override = default;

//     Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
// };

// class DateColumnWriter : public PrimitiveColumnWriter {
// public:
//     DateColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* Writer)
//             : PrimitiveColumnWriter(type, orc_type, nullable, Writer) {}

//     ~DateColumnWriter() override = default;

//     Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
// };

// template <bool IsInstant>
// class TimestampColumnWriter : public PrimitiveColumnWriter {
// public:
//     TimestampColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* Writer)
//             : PrimitiveColumnWriter(type, orc_type, nullable, Writer) {}
//     ~TimestampColumnWriter() override = default;

//     Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
// };

// class ComplexColumnWriter : public ORCColumnWriter {
// public:
//     ComplexColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* Writer,
//                         std::vector<std::unique_ptr<ORCColumnWriter>> child_Writers)
//             : ORCColumnWriter(type, orc_type, nullable, Writer) {
//         _child_Writers = std::move(child_Writers);
//     }
//     ~ComplexColumnWriter() override = default;

// protected:
//     static void copy_array_offset(orc::DataBuffer<int64_t>& src, int from, int size, UInt32Column* dst) {
//         DCHECK_GT(size, 0);
//         if (from == 0 && dst->size() == 1) {
//             //           ^^^^^^^^^^^^^^^^ offset column size is 1, means the array column is empty.
//             DCHECK_EQ(0, src[0]);
//             DCHECK_EQ(0, dst->get_data()[0]);
//             dst->resize(size);
//             uint32_t* dst_data = dst->get_data().data();
//             for (int i = 1; i < size; i++) {
//                 dst_data[i] = static_cast<uint32_t>(src[i]);
//             }
//         } else {
//             DCHECK_GT(dst->size(), 1);
//             int dst_pos = dst->size();
//             dst->resize(dst_pos + size - 1);
//             uint32_t* dst_data = dst->get_data().data();

//             // Equivalent to the following code:
//             // ```
//             //  for (int i = from + 1; i < from + size; i++, dst_pos++) {
//             //      dst_data[dst_pos] = dst_data[dst_pos-1] + (src[i] - src[i-1]);
//             //  }
//             // ```
//             uint32_t prev_starrocks_offset = dst_data[dst_pos - 1];
//             int64_t prev_orc_offset = src[from];
//             for (int i = from + 1; i < from + size; i++, dst_pos++) {
//                 int64_t curr_orc_offset = src[i];
//                 int64_t diff = curr_orc_offset - prev_orc_offset;
//                 uint32_t curr_starrocks_offset = prev_starrocks_offset + diff;
//                 dst_data[dst_pos] = curr_starrocks_offset;
//                 prev_orc_offset = curr_orc_offset;
//                 prev_starrocks_offset = curr_starrocks_offset;
//             }
//         }
//     }

//     std::vector<std::unique_ptr<ORCColumnWriter>> _child_Writers;
// };

// class ArrayColumnWriter : public ComplexColumnWriter {
// public:
//     ArrayColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* Writer,
//                       std::vector<std::unique_ptr<ORCColumnWriter>> child_Writers)
//             : ComplexColumnWriter(type, orc_type, nullable, Writer, std::move(child_Writers)) {}
//     ~ArrayColumnWriter() override = default;

//     Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

// private:
//     Status _fill_array_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size);
// };

// class MapColumnWriter : public ComplexColumnWriter {
// public:
//     MapColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* Writer,
//                     std::vector<std::unique_ptr<ORCColumnWriter>> child_Writers)
//             : ComplexColumnWriter(type, orc_type, nullable, Writer, std::move(child_Writers)) {}
//     ~MapColumnWriter() override = default;

//     Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

// private:
//     Status _fill_map_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size);
// };

// class StructColumnWriter : public ComplexColumnWriter {
// public:
//     StructColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* Writer,
//                        std::vector<std::unique_ptr<ORCColumnWriter>> child_Writers)
//             : ComplexColumnWriter(type, orc_type, nullable, Writer, std::move(child_Writers)) {}
//     ~StructColumnWriter() override = default;

//     Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

// private:
//     Status _fill_struct_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size);
// };

} // namespace starrocks