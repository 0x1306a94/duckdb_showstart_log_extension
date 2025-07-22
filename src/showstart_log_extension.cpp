#define DUCKDB_EXTENSION_MAIN

#include "showstart_log_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp> // 引入CreateTableFunctionInfo
#include <fstream>
#include <iostream>
#include <sstream>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

static void ParseLogFile(FileSystem &fs, const string &file_path, DataChunk &output, idx_t &row_index) {
	std::ifstream file(file_path);
	std::string line;

	if (!file.is_open()) {
		throw IOException("Failed to open file: " + file_path);
	}

	while (std::getline(file, line)) {
		// std::cout << "Line: " << line << std::endl;
		StringUtil::Trim(line);
		if (line.empty()) {
			continue;
		}

		// if (line.empty() && !StringUtil::StartsWith(line, "ShowStart:")) {
		// 	continue;
		// }

		if (StringUtil::StartsWith(line, "ShowStart:") && StringUtil::EndsWith(line, "|$")) {
			line = line.substr(string("ShowStart:").length());
			line = line.substr(0, line.length() - string("|$").length());
		} else if (StringUtil::StartsWith(line, "ShowStart:") && StringUtil::EndsWith(line, "|#")) {
			line = line.substr(string("ShowStart:").length());
			line = line.substr(0, line.length() - string("|#").length());
		} else {
			continue;
		}

		std::vector<std::string> parts = StringUtil::Split(line, "|");
		if (parts.size() < 8) {
			continue;
		}

		int64_t timestamp_ms = static_cast<int64_t>(std::stoll(parts[1]));
		std::string logName = parts[2];
		std::string level = parts[3];
		std::string logTag = parts[4];
		std::string filename = parts[5];
		std::string fileline = parts[6];
		std::string func = parts[7];
		std::string message = parts[9];
		message = message.substr(string("message:").length());

		std::cout << "Line: size " << parts.size() << " timestamp_ms " << timestamp_ms << " message " << message
		          << std::endl;

		idx_t current_row = row_index % STANDARD_VECTOR_SIZE;
		output.SetValue(0, current_row, Value::TIMESTAMPMS(timestamp_ms_t(timestamp_ms))); // Timestamp
		output.SetValue(1, current_row, Value::BLOB_RAW(logName));                         // LogName
		output.SetValue(2, current_row, Value::BLOB_RAW(level));                           // Level
		output.SetValue(3, current_row, Value::BLOB_RAW(logTag));                          // LogTag
		output.SetValue(4, current_row, Value::BLOB_RAW(filename + "." + fileline));       // func
		output.SetValue(5, current_row, Value::BLOB_RAW(func));                            // func
		output.SetValue(6, current_row, Value::BLOB_RAW(message));                         // Message

		row_index++;

		if (row_index % STANDARD_VECTOR_SIZE == 0) {
			output.SetCardinality(STANDARD_VECTOR_SIZE);
			return;
		}
	}

	if (row_index % STANDARD_VECTOR_SIZE != 0) {
		output.SetCardinality(row_index % STANDARD_VECTOR_SIZE);
	}

	file.close();
}

static void ScanDirectory(FileSystem &fs, const string &path, vector<string> &files) {
	fs.ListFiles(path, [&](const string &fname, bool is_dir) {
		if (!is_dir && StringUtil::EndsWith(StringUtil::Lower(fname), ".xlog.log")) {
			files.push_back(fs.JoinPath(path, fname));
		}
	});
}

static unique_ptr<FunctionData> ReadShowstartLogBind(ClientContext &context, TableFunctionBindInput &bind_input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ReadShowstartLogFunctionData>();
	auto &fs = FileSystem::GetFileSystem(context);

	string path = bind_input.inputs[0].ToString();

	if (fs.IsDirectory(path)) {
		ScanDirectory(fs, path, result->files);
	} else {
		result->files.push_back(path);
	}

	names.emplace_back("timestamp");
	return_types.emplace_back(LogicalType::TIMESTAMP_MS);
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("level");
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("tag");
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("file");
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("func");
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("message");
	return_types.emplace_back(LogicalType::BLOB);

	return std::move(result);
}

static void ReadShowstartLogFunction(ClientContext &context, TableFunctionInput &data_input, DataChunk &output) {
	auto &data = (ReadShowstartLogFunctionData &)*data_input.bind_data;
	auto &fs = FileSystem::GetFileSystem(context);

	if (data.row_index >= data.files.size() * STANDARD_VECTOR_SIZE) {
		output.SetCardinality(0);
		return;
	}

	idx_t output_offset = 0;
	while (data.row_index < data.files.size() * STANDARD_VECTOR_SIZE) {
		idx_t file_index = data.row_index / STANDARD_VECTOR_SIZE;
		idx_t row_index_in_file = data.row_index % STANDARD_VECTOR_SIZE;

		output.Reset();

		ParseLogFile(fs, data.files[file_index], output, data.row_index);

		if (output.size() > 0) {
			break;
		} else {
			data.row_index += STANDARD_VECTOR_SIZE;
		}
	}

	if (output.size() == 0) {
		output.SetCardinality(0);
	}
}

static void LoadInternal(DatabaseInstance &instance) {
	CreateTableFunctionInfo read_showstart_log_info(
	    TableFunction("read_showstart_log", {LogicalType::VARCHAR}, ReadShowstartLogFunction, ReadShowstartLogBind));
	ExtensionUtil::RegisterFunction(instance, read_showstart_log_info);
	std::string description = StringUtil::Format("秀动log分析");
	ExtensionUtil::RegisterExtension(instance, "showstart_log", ExtensionLoadedInfo {std::move(description)});
}

void ShowstartLogExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string ShowstartLogExtension::Name() {
	return "showstart_log";
}

std::string ShowstartLogExtension::Version() const {
#ifdef EXT_VERSION_QUACK
	return EXT_VERSION_QUACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void showstart_log_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::ShowstartLogExtension>();
}

DUCKDB_EXTENSION_API const char *showstart_log_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif