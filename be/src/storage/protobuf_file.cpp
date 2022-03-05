// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/protobuf_file.h"

#include <google/protobuf/message.h>

#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "storage/olap_define.h"
#include "storage/utils.h"
#include "util/raw_container.h"

namespace starrocks {

typedef struct _FixedFileHeader {
    uint64_t magic_number;
    uint32_t version;
    // file's total length
    uint64_t file_length;
    // checksum for content except FileHeader
    uint32_t checksum;
    // length for protobuf content
    uint64_t protobuf_length;
    // checksum for protobuf
    uint32_t protobuf_checksum;
} __attribute__((packed)) FixedFileHeader;

ProtobufFile::ProtobufFile(std::string path, Env* env) : _path(std::move(path)), _env(env ? env : Env::Default()) {}

Status ProtobufFile::save(const ::google::protobuf::Message& message, bool sync) {
    uint32_t unused_flag = 0;
    FixedFileHeader header;
    std::string serialized_message = message.SerializeAsString();
    header.protobuf_checksum = olap_adler32(ADLER32_INIT, serialized_message.c_str(), serialized_message.size());
    header.checksum = 0;
    header.protobuf_length = serialized_message.size();
    header.file_length = sizeof(header) + sizeof(unused_flag) + serialized_message.size();
    header.version = OLAP_DATA_VERSION_APPLIED;
    header.magic_number = OLAP_FIX_HEADER_MAGIC_NUMBER;

    std::unique_ptr<WritableFile> output_file;
    ASSIGN_OR_RETURN(output_file, _env->new_writable_file(_path));
    RETURN_IF_ERROR(output_file->append(Slice((const char*)(&header), sizeof(header))));
    RETURN_IF_ERROR(output_file->append(Slice((const char*)(&unused_flag), sizeof(unused_flag))));
    RETURN_IF_ERROR(output_file->append(serialized_message));
    return sync ? output_file->sync() : Status::OK();
}

Status ProtobufFile::load(::google::protobuf::Message* message) {
    ASSIGN_OR_RETURN(auto input_file, _env->new_sequential_file(_path));

    FixedFileHeader header;
    ASSIGN_OR_RETURN(auto nread, input_file->read(&header, sizeof(header)));
    if (nread != sizeof(header)) {
        return Status::Corruption("fail to read header");
    }
    if (header.magic_number != OLAP_FIX_HEADER_MAGIC_NUMBER) {
        return Status::Corruption(strings::Substitute("invalid magic number $0", header.magic_number));
    }

    uint32_t unused_flag;
    ASSIGN_OR_RETURN(nread, input_file->read(&unused_flag, sizeof(unused_flag)));
    if (UNLIKELY(nread != sizeof(unused_flag))) {
        return Status::Corruption("fail to read flag");
    }

    std::string str;
    raw::stl_string_resize_uninitialized(&str, header.protobuf_length + 1);
    ASSIGN_OR_RETURN(nread, input_file->read(str.data(), str.size()));
    str.resize(nread);
    if (str.size() != header.protobuf_length) {
        return Status::Corruption("mismatched serialized size");
    }
    if (olap_adler32(ADLER32_INIT, str.data(), str.size()) != header.protobuf_checksum) {
        return Status::Corruption("mismatched checksum");
    }
    if (!message->ParseFromString(str)) {
        return Status::Corruption("parse protobuf message failed");
    }
    return Status::OK();
}

} // namespace starrocks
