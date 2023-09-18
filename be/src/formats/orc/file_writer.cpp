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

#include "formats/orc/file_writer.h"

namespace starrocks::orc {

// /*
// ** ORCOutputStream
// */
// OrcOutputStream::OrcOutputStream(std::unique_ptr<starrocks::WritableFile> wfile) : _wfile(std::move(wfile)) {}

// OrcOutputStream::~OrcOutputStream() {
// }

// uint64_t OrcOutputStream::getLength() const {
//     return _wfile->size();
// }

// uint64_t OrcOutputStream::getNaturalWriteSize() const {
//     return 0;
// }

// const std::string& OrcOutputStream::getName() const {
//     return _wfile->filename();
// }

// void OrcOutputStream::write(const void* buf, size_t length)
// {
//     if (_is_closed) {
//         LOG(WARNING) << "The output stream is closed but there are still inputs";
//         return;
//     }
//     const char* ch = reinterpret_cast<const char*>(buf);
//     Status st = _wfile->append(Slice(ch, length));
//     if (!st.ok()) {
//         LOG(WARNING) << "write to orc output stream failed: " << st;
//     }
//     return;
// }

// void OrcOutputStream::close() {
//     if (_is_closed) {
//         return;
//     }
//     Status st = _wfile->close();
//     if (!st.ok()) {
//         LOG(WARNING) << "close orc output stream failed: " << st;
//         return;
//     }
//     _is_closed = true;
//     return;
// }

// FileWriterBase::FileWriterBase(std::unique_ptr<WritableFile> writable_file,
//                                std::shared_ptr<orc::WriterOptions> options,
//                                std::shared_ptr<orc::Type> schema,
//                                const std::vector<ExprContext*>& output_expr_ctxs, int64_t max_file_size)
//         : _options(std::move(options)), _schema(std::move(schema)), _max_file_size(max_file_size) {
//     _outstream = std::make_shared<OrcOutputStream>(std::move(writable_file));
//     _type_descs.reserve();
//     for (auto expr : output_expr_ctxs) {
//         _type_descs.push_back(expr->root()->type());
//     }
//     _eval_func = [output_expr_ctxs](Chunk *chunk, size_t col_index) {
//         return output_expr_ctxs[col_index]->evaluate(chunk);
//     };
// }

// UT
FileWriterBase::FileWriterBase(std::unique_ptr<WritableFile> writable_file,
                               std::shared_ptr<::orc::WriterOptions> options, std::shared_ptr<::orc::Type> schema,
                               std::vector<TypeDescriptor> type_descs)
        : _options(std::move(options)), _schema(std::move(schema)), _type_descs(std::move(type_descs)) {
    _outstream = std::make_shared<OrcOutputStream>(std::move(writable_file));
}

// Status FileWriterBase::init() {
//     _chunk_writer = std::make_unique<OrcChunkWriter>(_type_descs, _outstream);
// }

// // Status FileWriterBase::write(Chunk* chunk) {
// //     if(!chunk->has_rows()) {
// //         return Status::OK();
// //     }
// //     // _chunk_writer->write
// // }

} // namespace starrocks::orc