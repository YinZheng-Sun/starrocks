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

// #include <gen_cpp/DataSinks_types.h>

// #include <utility>

#include "column/chunk.h"
#include "column/nullable_column.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"
#include "formats/orc/orc_chunk_writer.h"

namespace starrocks::orc {

// class OrcOutputStream : public orc::OutputStream {
// public:
//     OrcOutputStream(std::unique_ptr<starrocks::WritableFile> wfile);

//     ~OrcOutputStream() override;
    
//     uint64_t getLength() const override;
    
//     uint64_t getNaturalWriteSize() const override;
    
//     void write(const void * buf, size_t length) override;

//     void close() override;

//     const std::string& getName() const override;

// private:
//     std::unique_ptr<starrocks::WritableFile> _wfile;
//     bool _is_closed = false;
// };

// struct FileColumnId {
//     int32_t field_id = -1;
//     std::vector<FileColumnId> children;
// };

// struct OrcBuilderOptions {
//     TCompressionType::type compression_type = TCompressionType::SNAPPY;
//     bool use_dict = true;
//     int64_t row_group_max_size = 128 * 1024 * 1024;
// };

class FileWriterBase {
public:
    // FileWriterBase(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::orc::WriterOptions> options,
    //                std::shared_ptr<::orc::Type> schema,
    //                const std::vector<ExprContext*>& output_expr_ctxs, int64_t _max_file_size);
    // UT
    FileWriterBase(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::orc::WriterOptions> options,
                   std::shared_ptr<::orc::Type> schema, std::vector<TypeDescriptor> type_descs);

    virtual ~FileWriterBase() = default;

    Status init();

    Status write(Chunk* chunk);

    std::size_t file_size() const;

    void set_max_row_group_size(int64_t rg_size) { _max_row_group_size = rg_size; }

    Status split_offsets(std::vector<int64_t>& splitOffsets) const;

    virtual bool closed() const = 0;

protected:
    void _generate_chunk_writer();

    virtual Status _flush_row_group() = 0;

// private:
//     bool is_last_row_group() {
//         return _max_file_size - _writer->num_row_groups() * _max_row_group_size < 2 * _max_row_group_size;
//     }

protected:
    std::shared_ptr<OrcOutputStream> _outstream;
    std::shared_ptr<::orc::WriterOptions> _options;
    std::shared_ptr<::orc::Type> _schema;
    std::unique_ptr<OrcChunkWriter> _chunk_writer;

    std::vector<TypeDescriptor> _type_descs;
    std::function<StatusOr<ColumnPtr>(Chunk*, size_t)> _eval_func;

    const static int64_t kDefaultMaxRowGroupSize = 128 * 1024 * 1024; // 128MB
    int64_t _max_row_group_size = kDefaultMaxRowGroupSize;
    int64_t _max_file_size = 512 * 1024 * 1024; // 512MB
};

class SyncFileWriter : public FileWriterBase {
public:
    SyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::orc::WriterOptions> writeroptions,
                   std::shared_ptr<::orc::Type> schema,
                   const std::vector<ExprContext*>& output_expr_ctxs, int64_t max_file_size)
            : FileWriterBase(std::move(writable_file), std::move(writeroptions), std::move(schema), output_expr_ctxs,
                             max_file_size) {}

    SyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::orc::WriterOptions> writeroptions,
                   std::shared_ptr<::orc::Type> schema, std::vector<TypeDescriptor> type_descs)
            : FileWriterBase(std::move(writable_file), std::move(writeroptions), std::move(schema),
                             std::move(type_descs)) {}

    ~SyncFileWriter() override = default;

    Status close();

    bool closed() const override { return _closed; }

private:
    Status _flush_row_group() override;

    bool _closed = false;
};

class AsyncFileWriter : public FileWriterBase {
public:
    AsyncFileWriter(std::unique_ptr<WritableFile> writable_file, std::string file_location,
                    std::string partition_location, std::shared_ptr<::orc::WriterOptions> properties,
                    std::shared_ptr<::orc::Type> schema,
                    const std::vector<ExprContext*>& output_expr_ctxs, PriorityThreadPool* executor_pool,
                    RuntimeProfile* parent_profile, int64_t max_file_size);

    ~AsyncFileWriter() override = default;

    Status close(RuntimeState* state,
                 const std::function<void(starrocks::orc::AsyncFileWriter*, RuntimeState*)>& cb = nullptr);

    bool writable() {
    }

    bool closed() const override { return _closed.load(); }

    std::string file_location() const { return _file_location; }

    std::string partition_location() const { return _partition_location; }

private:
    Status _flush_row_group() override;

    std::string _file_location;
    std::string _partition_location;
    std::atomic<bool> _closed = false;

    PriorityThreadPool* _executor_pool;

    RuntimeProfile* _parent_profile = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;

    std::condition_variable _cv;
    bool _rg_writer_closing = false;
    std::mutex _m;
};

} // namespace starrocks::parquet
