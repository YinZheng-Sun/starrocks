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

#include <gtest/gtest.h>

#include <ctime>
#include <filesystem>
#include <map>
#include <vector>
#include <iostream>

#include "column/array_column.h"
#include "column/struct_column.h"
#include "common/object_pool.h"
#include "gen_cpp/Exprs_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "column/map_column.h"
#include "runtime/runtime_state.h"
#include "orc_test_util/MemoryInputStream.hh"
#include "orc_test_util/MemoryOutputStream.hh"
#include "formats/orc/orc_chunk_writer.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "testutil/assert.h"

namespace starrocks {

class OrcChunkWriterTest : public testing::Test {
public:
    void SetUp() override {};
    void TearDown() override {};

protected:

    std::vector<std::string> _make_type_names(const std::vector<TypeDescriptor>& type_descs) {
        std::vector<std::string> names;
        for (auto& desc : type_descs) {
            names.push_back(desc.debug_string());
        }
        return names;
    }

    Status _write_chunk(const ChunkPtr& chunk, std::vector<TypeDescriptor>& type_descs, std::unique_ptr<orc::Type> schema) {
        auto fs = FileSystem::Default();
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file("./tmp2.orc"));
        OrcOutputStream OutputStream(std::move(file));

        auto chunk_writer = std::make_shared<OrcChunkWriter>(type_descs, &OutputStream, std::move(schema));
        auto st = chunk_writer->write(chunk.get());

        if (!st.ok()) {
            std::cout << st.to_string() << std::endl;
            return st;
        }
        chunk_writer->set_compression(TCompressionType::SNAPPY);
        chunk_writer->close();
        return st;
    }


private:
    MemoryFileSystem _fs;
    std::string _file_path{"./tmp.orc"};
    ObjectPool _pool;
    std::shared_ptr<RuntimeState> _runtime_state;
};

TEST_F(OrcChunkWriterTest, TestSimpleWrite) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_TINYINT),
            TypeDescriptor::from_logical_type(TYPE_SMALLINT),
            // TypeDescriptor::from_logical_type(TYPE_INT),
            // TypeDescriptor::from_logical_type(TYPE_BIGINT),
    };

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_TINYINT), true);
        std::vector<int8_t> int8_nums{INT8_MIN, INT8_MAX, 0, 1};
        auto count = col0->append_numbers(int8_nums.data(), size(int8_nums) * sizeof(int8_t));
        ASSERT_EQ(4, count);
        chunk->append_column(col0, chunk->num_columns());

        auto col1 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_SMALLINT), true);
        std::vector<int16_t> int16_nums{INT16_MIN, INT16_MAX, 0, 1};
        count = col1->append_numbers(int16_nums.data(), size(int16_nums) * sizeof(int16_t));
        ASSERT_EQ(4, count);
        chunk->append_column(col1, chunk->num_columns());

        
        auto col2 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
        std::vector<int32_t> int32_nums{INT32_MIN, INT32_MAX, 0, 1};
        count = col2->append_numbers(int32_nums.data(), size(int32_nums) * sizeof(int32_t));
        ASSERT_EQ(4, count);
        chunk->append_column(col2, chunk->num_columns());

        auto col3 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
        std::vector<int64_t> int64_nums{INT64_MIN, INT64_MAX, 0, 1};
        count = col3->append_numbers(int64_nums.data(), size(int64_nums) * sizeof(int64_t));
        ASSERT_EQ(4, count);
        chunk->append_column(col3, chunk->num_columns());
    }
    auto column_names = _make_type_names(type_descs);
    ASSIGN_OR_ABORT(auto schema, OrcChunkWriter::make_schema(column_names, type_descs));

    // write chunk
    auto st = _write_chunk(chunk, type_descs, std::move(schema));
    ASSERT_OK(st);
}

TEST_F(OrcChunkWriterTest, TestWriteVarchar) {
    auto type_varchar = TypeDescriptor::from_logical_type(TYPE_VARCHAR);
    std::vector<TypeDescriptor> type_descs{type_varchar};

    auto chunk = std::make_shared<Chunk>();
    {
        auto data_column = BinaryColumn::create();
        data_column->append("hello");
        data_column->append("world");
        data_column->append("starrocks");
        data_column->append("lakehouse");

        auto null_column = UInt8Column::create();
        std::vector<uint8_t> nulls = {1, 0, 1, 0};
        null_column->append_numbers(nulls.data(), nulls.size());
        auto nullable_column = NullableColumn::create(data_column, null_column);
        chunk->append_column(nullable_column, chunk->num_columns());
    }

    auto column_names = _make_type_names(type_descs);
    ASSIGN_OR_ABORT(auto schema, OrcChunkWriter::make_schema(column_names, type_descs));
    // write chunk
    auto st = _write_chunk(chunk, type_descs, std::move(schema));
    ASSERT_OK(st);
}

TEST_F(OrcChunkWriterTest, TestWriteIntegralTypes) {

    auto _fs = FileSystem::Default();
    ASSIGN_OR_ABORT(auto file, _fs->new_writable_file("./tmp.orc"));
    
    OrcOutputStream buffer(std::move(file));

    orc::WriterOptions writerOptions;
    // force to make stripe every time.
    writerOptions.setStripeSize(1);
    writerOptions.setRowIndexStride(10);
    ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:int,c1:int>"));
    ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, writerOptions);

    int batchSize = 4096;
    int batchNum = 1;

    ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
    auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
    auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
    auto* c1 = dynamic_cast<orc::LongVectorBatch*>(root->fields[1]);

    size_t index = 0;
    for (size_t k = 0; k < batchNum; k++) {
        for (size_t i = 0; i < batchSize; i++) {
            c0->data[i] = index;
            c1->data[i] = index * 10;
            index += 1;
        }
        c0->numElements = batchSize;
        c1->numElements = batchSize;
        root->numElements = batchSize;
        writer->add(*batch);
    }
    writer->close();
}

TEST_F(OrcChunkWriterTest, TestWriteStruct) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_a = TypeDescriptor::from_logical_type(TYPE_SMALLINT);
    auto type_int_b = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_c = TypeDescriptor::from_logical_type(TYPE_BIGINT);
    auto type_int_struct = TypeDescriptor::from_logical_type(TYPE_STRUCT);
    type_int_struct.children = {type_int_a, type_int_b, type_int_c};
    type_int_struct.field_names = {"a", "b", "c"};
    type_descs.push_back(type_int_struct);

    auto chunk = std::make_shared<Chunk>();
    {
        std::vector<uint8_t> nulls{0, 0, 1, 0};

        auto data_col_a = Int16Column::create();
        std::vector<int16_t> nums_a{1, 2, -99, 3};
        data_col_a->append_numbers(nums_a.data(), sizeof(int16_t) * nums_a.size());
        auto null_col_a = UInt8Column::create();
        null_col_a->append_numbers(nulls.data(), sizeof(uint8_t) * nulls.size());
        auto nullable_col_a = NullableColumn::create(data_col_a, null_col_a);

        auto data_col_b = Int32Column::create();
        std::vector<int32_t> nums_b{1, 2, -99, 3};
        data_col_b->append_numbers(nums_b.data(), sizeof(int32_t) * nums_b.size());
        auto null_col_b = UInt8Column::create();
        null_col_b->append_numbers(nulls.data(), sizeof(uint8_t) * nulls.size());
        auto nullable_col_b = NullableColumn::create(data_col_b, null_col_b);

        auto data_col_c = Int64Column::create();
        std::vector<int64_t> nums_c{1, 2, -99, 3};
        data_col_c->append_numbers(nums_c.data(), sizeof(int64_t) * nums_c.size());
        auto null_col_c = UInt8Column::create();
        null_col_c->append_numbers(nulls.data(), sizeof(uint8_t) * nulls.size());
        auto nullable_col_c = NullableColumn::create(data_col_c, null_col_c);

        Columns fields{nullable_col_a, nullable_col_b, nullable_col_c};
        auto struct_column = StructColumn::create(fields, type_int_struct.field_names);
        auto null_column = UInt8Column::create();
        null_column->append_numbers(nulls.data(), sizeof(uint8_t) * nulls.size());
        auto nullable_col = NullableColumn::create(struct_column, null_column);

        chunk->append_column(nullable_col, chunk->num_columns());
    }

    // write chunk
    auto column_names = _make_type_names(type_descs);
    ASSIGN_OR_ABORT(auto schema, OrcChunkWriter::make_schema(column_names, type_descs));
    // write chunk
    auto st = _write_chunk(chunk, type_descs, std::move(schema));
    ASSERT_OK(st);
}

TEST_F(OrcChunkWriterTest, TestWriteMap) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_key = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_value = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_map = TypeDescriptor::from_logical_type(TYPE_MAP);
    type_int_map.children.push_back(type_int_key);
    type_int_map.children.push_back(type_int_value);
    type_descs.push_back(type_int_map);

    // [1 -> 1], NULL, [], [2 -> 2, 3 -> NULL]
    auto chunk = std::make_shared<Chunk>();
    {
        auto key_data_col = Int32Column::create();
        std::vector<int32_t> key_nums{1, 2, 3, 4};
        key_data_col->append_numbers(key_nums.data(), sizeof(int32_t) * key_nums.size());
        auto key_null_col = UInt8Column::create();
        std::vector<uint8_t> key_nulls{0, 0, 0, 0};
        key_null_col->append_numbers(key_nulls.data(), sizeof(uint8_t) * key_nulls.size());
        auto key_col = NullableColumn::create(key_data_col, key_null_col);

        auto value_data_col = Int32Column::create();
        std::vector<int32_t> value_nums{1, 2, -99, 4};
        value_data_col->append_numbers(value_nums.data(), sizeof(int32_t) * value_nums.size());
        auto value_null_col = UInt8Column::create();
        std::vector<uint8_t> value_nulls{0, 0, 1, 0};
        value_null_col->append_numbers(value_nulls.data(), sizeof(uint8_t) * value_nulls.size());
        auto value_col = NullableColumn::create(value_data_col, value_null_col);

        auto offsets_col = UInt32Column::create();
        std::vector<uint32_t> offsets{0, 1, 1, 1, 4};
        offsets_col->append_numbers(offsets.data(), sizeof(uint32_t) * offsets.size());
        auto map_col = MapColumn::create(key_col, value_col, offsets_col);

        std::vector<uint8_t> _nulls{0, 1, 0, 0};
        auto null_col = UInt8Column::create();
        null_col->append_numbers(_nulls.data(), sizeof(uint8_t) * _nulls.size());
        auto nullable_col = NullableColumn::create(map_col, null_col);

        chunk->append_column(nullable_col, chunk->num_columns());
    }

    // write chunk
    auto column_names = _make_type_names(type_descs);
    ASSIGN_OR_ABORT(auto schema, OrcChunkWriter::make_schema(column_names, type_descs));
    // write chunk  
    auto st = _write_chunk(chunk, type_descs, std::move(schema));
}

TEST_F(OrcChunkWriterTest, TestWriteNestedArray) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int = TypeDescriptor::from_logical_type(TYPE_INT);
    auto type_int_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    auto type_int_array_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array.children.push_back(type_int);
    type_int_array_array.children.push_back(type_int_array);
    type_descs.push_back(type_int_array_array);

    // [[1], NULL, [], [2, NULL, 3]], [[4, 5], [6]], NULL
    auto chunk = std::make_shared<Chunk>();
    {
        auto int_data_col = Int32Column::create();
        std::vector<int32_t> nums{1, 2, -99, 3, 4, 5, 6};
        int_data_col->append_numbers(nums.data(), sizeof(int32_t) * nums.size());
        auto int_null_col = UInt8Column::create();
        std::vector<uint8_t> nulls{0, 0, 1, 0, 0, 0, 0};
        int_null_col->append_numbers(nulls.data(), sizeof(uint8_t) * nulls.size());
        auto int_col = NullableColumn::create(int_data_col, int_null_col);

        auto offsets_col = UInt32Column::create();
        std::vector<uint32_t> offsets{0, 1, 1, 1, 4, 6, 7};
        offsets_col->append_numbers(offsets.data(), sizeof(uint32_t) * offsets.size());
        auto array_data_col = ArrayColumn::create(int_col, offsets_col);

        std::vector<uint8_t> _nulls{0, 1, 0, 0, 0, 0};
        auto array_null_col = UInt8Column::create();
        array_null_col->append_numbers(_nulls.data(), sizeof(uint8_t) * _nulls.size());
        auto array_col = NullableColumn::create(array_data_col, array_null_col);

        auto array_array_offsets_col = UInt32Column::create();
        std::vector<uint32_t> array_array_offsets{0, 4, 6, 6};
        array_array_offsets_col->append_numbers(array_array_offsets.data(),
                                                sizeof(uint32_t) * array_array_offsets.size());
        auto array_array_data_col = ArrayColumn::create(array_col, array_array_offsets_col);

        std::vector<uint8_t> outer_nulls{0, 0, 1};
        auto array_array_null_col = UInt8Column::create();
        array_array_null_col->append_numbers(outer_nulls.data(), sizeof(uint8_t) * outer_nulls.size());
        auto array_array_col = NullableColumn::create(array_array_data_col, array_array_null_col);

        chunk->append_column(array_array_col, chunk->num_columns());
    }

     // write chunk
    auto column_names = _make_type_names(type_descs);
    ASSIGN_OR_ABORT(auto schema, OrcChunkWriter::make_schema(column_names, type_descs));
    // write chunk  
    auto st = _write_chunk(chunk, type_descs, std::move(schema));
}

} // namespace starrocks