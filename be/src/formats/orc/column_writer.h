// // Copyright 2023-present StarRocks, Inc. All rights reserved.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     https://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

// #pragma once

// #include <orc/OrcFile.hh>
// #include <utility>

// #include "formats/orc/orc_chunk_writer.h"
// #include "column/array_column.h"
// #include "column/map_column.h"
// #include "column/struct_column.h"
// #include "common/status.h"
// #include "common/statusor.h"
// #include "formats/orc/orc_mapping.h"
// #include "runtime/types.h"
// #include "types/logical_type.h"

// namespace starrocks {

// class OrcChunkWriter;

// class OrcColumnWriter {
// public:
//     OrcColumnWriter(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkWriter* writer)
//             : _type(type), _orc_type(orc_type), _nullable(nullable), _writer(writer) {}
//     virtual ~OrcColumnWriter() = default;
//     virtual Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) = 0;

//     static StatusOr<std::unique_ptr<OrcColumnWriter>> create(const TypeDescriptor& type, const orc::Type* orc_type,
//                                                              bool nullable, const OrcMappingPtr& orc_mapping,
//                                                              OrcChunkWriter* writer);
//     const orc::Type* get_orc_type() { return _orc_type; }

// protected:
//     const TypeDescriptor& _type;
//     const orc::Type* _orc_type;
//     bool _nullable;
//     OrcChunkWriter* _writer;
// };
// } // namespace starrocks