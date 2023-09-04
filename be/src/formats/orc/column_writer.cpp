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

// #include "formats/orc/column_writer.h"

// #include "common/statusor.h"
// #include "formats/orc/orc_chunk_writer.h"
// #include "formats/orc/utils.h"

// namespace starrocks {

// StatusOr<std::unique_ptr<OrcColumnWriter>> OrcColumnWriter::create(const TypeDescriptor& type,
//                                                                    const orc::Type* orc_type, bool nullable,
//                                                                 //    const OrcMappingPtr& orc_mapping,
//                                                                    OrcChunkWriter* writer) {
//     if (type.is_complex_type() && orc_mapping == nullptr) {
//         return Status::InternalError("Complex type must having OrcMapping");
//     }
//     if (orc_type == nullptr) {
//         return Status::InternalError("Each ColumnWriter's ORC type must non-nullptr");
//     }

//     switch (type.type) {
//     case TYPE_BOOLEAN:
//         return std::make_unique<BooleanColumnWriter>(type, orc_type, nullable, writer);
//     case TYPE_TINYINT:
//         return std::make_unique<IntColumnWriter<TYPE_TINYINT>>(type, orc_type, nullable, writer);
//     // case TYPE_SMALLINT:
//     //     return std::make_unique<IntColumnReader<TYPE_SMALLINT>>(type, orc_type, nullable, reader);
//     // case TYPE_INT:
//     //     return std::make_unique<IntColumnReader<TYPE_INT>>(type, orc_type, nullable, reader);
//     // case TYPE_BIGINT:
//     //     return std::make_unique<IntColumnReader<TYPE_BIGINT>>(type, orc_type, nullable, reader);
//     // case TYPE_LARGEINT:
//     //     return std::make_unique<IntColumnReader<TYPE_LARGEINT>>(type, orc_type, nullable, reader);
//     // case TYPE_FLOAT:
//     //     return std::make_unique<FloatColumnReader<TYPE_FLOAT>>(type, orc_type, nullable, reader);
//     // case TYPE_DOUBLE:
//     //     return std::make_unique<FloatColumnReader<TYPE_DOUBLE>>(type, orc_type, nullable, reader);
//     // case TYPE_DECIMAL:
//     //     return std::make_unique<DecimalColumnReader>(type, orc_type, nullable, reader);
//     // case TYPE_DECIMALV2:
//     //     return std::make_unique<DecimalColumnReader>(type, orc_type, nullable, reader);
//     // case TYPE_DECIMAL32:
//     //     return std::make_unique<Decimal32Or64Or128ColumnReader<TYPE_DECIMAL32>>(type, orc_type, nullable, reader);
//     // case TYPE_DECIMAL64:
//     //     return std::make_unique<Decimal32Or64Or128ColumnReader<TYPE_DECIMAL64>>(type, orc_type, nullable, reader);
//     // case TYPE_DECIMAL128:
//     //     return std::make_unique<Decimal32Or64Or128ColumnReader<TYPE_DECIMAL128>>(type, orc_type, nullable, reader);
//     // case TYPE_CHAR:
//     //     return std::make_unique<StringColumnReader>(type, orc_type, nullable, reader);
//     // case TYPE_VARCHAR:
//     //     return std::make_unique<StringColumnReader>(type, orc_type, nullable, reader);
//     // case TYPE_VARBINARY:
//     //     return std::make_unique<VarbinaryColumnReader>(type, orc_type, nullable, reader);
//     // case TYPE_DATE:
//     //     return std::make_unique<DateColumnReader>(type, orc_type, nullable, reader);
//     // case TYPE_DATETIME:
//     //     if (orc_type->getKind() == orc::TypeKind::TIMESTAMP) {
//     //         return std::make_unique<TimestampColumnReader<false>>(type, orc_type, nullable, reader);
//     //     } else if (orc_type->getKind() == orc::TypeKind::TIMESTAMP_INSTANT) {
//     //         return std::make_unique<TimestampColumnReader<true>>(type, orc_type, nullable, reader);
//     //     } else {
//     //         return Status::InternalError("Failed to create column reader about TYPE_DATETIME");
//     //     }
//     // case TYPE_STRUCT: {
//     //     std::vector<std::unique_ptr<ORCColumnReader>> child_readers;
//     //     for (size_t i = 0; i < type.children.size(); i++) {
//     //         const TypeDescriptor& child_type = type.children[i];
//     //         ASSIGN_OR_RETURN(
//     //                 std::unique_ptr<ORCColumnReader> child_reader,
//     //                 ORCColumnReader::create(child_type, orc_mapping->get_orc_type_child_mapping(i).orc_type, true,
//     //                                         orc_mapping->get_orc_type_child_mapping(i).orc_mapping, reader));
//     //         child_readers.emplace_back(std::move(child_reader));
//     //     }
//     //     return std::make_unique<StructColumnReader>(type, orc_type, nullable, reader, std::move(child_readers));
//     // }

//     // case TYPE_ARRAY: {
//     //     std::vector<std::unique_ptr<ORCColumnReader>> child_readers{};
//     //     const TypeDescriptor& child_type = type.children[0];
//     //     ASSIGN_OR_RETURN(std::unique_ptr<ORCColumnReader> child_reader,
//     //                      ORCColumnReader::create(child_type, orc_mapping->get_orc_type_child_mapping(0).orc_type, true,
//     //                                              orc_mapping->get_orc_type_child_mapping(0).orc_mapping, reader));
//     //     child_readers.emplace_back(std::move(child_reader));
//     //     return std::make_unique<ArrayColumnReader>(type, orc_type, nullable, reader, std::move(child_readers));
//     // }
//     // case TYPE_MAP: {
//     //     std::vector<std::unique_ptr<ORCColumnReader>> child_readers{};
//     //     const TypeDescriptor& key_type = type.children[0];
//     //     const TypeDescriptor& value_type = type.children[1];
//     //     if (key_type.is_unknown_type()) {
//     //         child_readers.emplace_back(nullptr);
//     //     } else {
//     //         ASSIGN_OR_RETURN(
//     //                 std::unique_ptr<ORCColumnReader> key_reader,
//     //                 ORCColumnReader::create(key_type, orc_mapping->get_orc_type_child_mapping(0).orc_type, true,
//     //                                         orc_mapping->get_orc_type_child_mapping(0).orc_mapping, reader));
//     //         child_readers.emplace_back(std::move(key_reader));
//     //     }

//     //     if (value_type.is_unknown_type()) {
//     //         child_readers.emplace_back(nullptr);
//     //     } else {
//     //         ASSIGN_OR_RETURN(
//     //                 std::unique_ptr<ORCColumnReader> value_reader,
//     //                 ORCColumnReader::create(value_type, orc_mapping->get_orc_type_child_mapping(1).orc_type, true,
//     //                                         orc_mapping->get_orc_type_child_mapping(1).orc_mapping, reader));
//     //         child_readers.emplace_back(std::move(value_reader));
//     //     }
//     //     return std::make_unique<MapColumnReader>(type, orc_type, nullable, reader, std::move(child_readers));
//     // }
//     default:
//         return Status::InternalError("Unsupported type");
//     }
// }

// } // namespace starrocks
