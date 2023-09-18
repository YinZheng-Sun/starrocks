// Copyright 2021-present StarRocks, Inc. All rights reserved.
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


#include <benchmark/benchmark.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "bench.h"
#include "column/chunk.h"
#include "common/statusor.h"
#include "formats/orc/orc_chunk_writer.h"
#include "fs/fs.h"
#include "fs/fs_posix.h"
#include "runtime/descriptor_helper.h"
#include "testutil/assert.h"
#include "util/time.h"

namespace starrocks {
namespace orc {
namespace {

const int random_seed = 42;

inline std::shared_ptr<Chunk> make_chunk(int num_rows, int null_percent) {
    std::srand(random_seed);

    std::vector<int32_t> values(num_rows);
    std::vector<uint8_t> is_null(num_rows, 0);

    for (int i = 0; i < num_rows; i++) {
        if (std::rand() % 100 < null_percent) {
            is_null[i] = 1;
        } else {
            values[i] = std::rand();
        }
    }

    auto chunk = std::make_shared<Chunk>();
    auto data_column = Int32Column::create();
    data_column->append_numbers(values.data(), values.size() * sizeof(int32));
    auto null_column = UInt8Column::create();
    null_column->append_numbers(is_null.data(), is_null.size());
    auto col = NullableColumn::create(data_column, null_column);

    chunk->append_column(col, 0);
    return chunk;
}

inline std::vector<TypeDescriptor> _make_type_descs() {
    return TypeDescriptor::from_logical_type(TYPE_INT);
}

inline std::unique_ptr<orc::Type> _make_schema() {
    auto type_descs = _make_type_descs();
    std::vector<std::string> column_names{"INT"};
    ASSIGN_OR_ABORT(auto schema, OrcChunkWriter::make_schema(column_names, type_descs));
    return schema;
}

inline std::unique_ptr<OrcChunkWriter> make_starrocks_writer(std::unique_ptr<WritableFile> file) {
    auto writer_options = std::make_shared<orc::WriterOptions>();
    auto type_descs = _make_type_descs();
    auto schema = _make_schema();
    auto chunk_writer = std::make_unique<OrcChunkWriter>(std::move(file), writer_options, type_descs, std::move(schema));
    return chunk_writer;
}

static void Benchmark_OrcChunkWriterArgs(benchmark::internal::Benchmark* b) {
    std::vector<int64_t> bm_num_rows = {100000, 1000000, 10000000};
    for (auto& num_rows : bm_num_rows) {
        b->Args({num_rows});
    }
}

static void Benchmark_StarRocksOrcWriter(benchmark::State& state) {
    auto fs = new_fs_posix();
    const std::string file_path = "./be/test/exec/test_data/orc_scanner/starrocks_writer.orc";
    fs->delete_file(file_path);

    auto num_rows = state.range(0);
    auto chunk = make_chunk(num_rows);

    for (int i = 0; i < 10; i++) {
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto writer = make_starrocks_writer(std::move(file));
        writer->init();

        writer->write(chunk.get());
        auto st = writer->close();
        ASSERT_TRUE(st.ok());

        fs->delete_file(file_path);
    }

    for (auto _ : state) {
        state.PauseTiming();
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto writer = make_starrocks_writer(std::move(file));
        writer->init();

        state.ResumeTiming();
        writer->write(chunk.get());
        auto st = writer->close();
        ASSERT_TRUE(st.ok());
        state.PauseTiming();

        fs->delete_file(file_path);
    }

    {
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(file_path));
        auto writer = make_starrocks_writer(std::move(file));
        writer->init();

         writer->write(chunk.get());
        auto st = writer->close();
        ASSERT_TRUE(st.ok());

        fs->delete_file(file_path);
    }
}

BENCHMARK(Benchmark_StarRocksOrcWriter)
        ->Apply(Benchmark_OrcChunkWriterArgs)
        ->Unit(benchmark::kMillisecond)
        ->MinTime(30);

} // namespace
} // namespace orc
} // namespace starrocks

BENCHMARK_MAIN();