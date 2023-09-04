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

#include "column/struct_column.h"
#include "common/object_pool.h"
#include "gen_cpp/Exprs_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
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
    auto schema = OrcBuildHelper::make_schema(column_names, type_descs);
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
    auto schema = OrcBuildHelper::make_schema(column_names, type_descs);
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

} // namespace starrocks