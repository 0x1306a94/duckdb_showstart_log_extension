#define DUCKDB_EXTENSION_MAIN

#include "showstart_log_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <fstream>
#include <iostream>
#include <sstream>

namespace duckdb {

static void ReadShowstartLogFunction(ClientContext &context, TableFunctionInput &data_input, DataChunk &output) {
	auto &data = (ReadShowstartLogFunctionData &)*data_input.bind_data;
	auto &fs = FileSystem::GetFileSystem(context);

	idx_t row_index = 0;
	output.Reset();

	while (!data.finished && row_index < STANDARD_VECTOR_SIZE) {
		if (!data.current_file.is_open()) {
			data.OpenNextFile(fs);
			if (data.finished) {
				break;
			}
		}

		std::string line;
		std::string buffer;
		bool in_log_block = false;

		while (std::getline(data.current_file, line)) {
			StringUtil::Trim(line);
			if (line.empty()) {
				continue;
			}

			if (StringUtil::StartsWith(line, "ShowStart:|")) {
				// 开始新的日志块
				buffer = line;
				in_log_block = true;
			} else if (in_log_block) {
				// 累加日志内容
				buffer += "\n" + line;
			}

			if (!buffer.empty() && (StringUtil::EndsWith(buffer, "|$") || StringUtil::EndsWith(buffer, "|#"))) {
				// 完整日志块已读取
				std::string log_line = buffer.substr(std::string("ShowStart:|").length());
				log_line = log_line.substr(0, log_line.length() - 2); // 去掉 |# 或 |$
				buffer.clear();
				in_log_block = false;

				// 解析字段
				vector<std::string> parts = StringUtil::Split(log_line, "|");
				size_t parts_size = parts.size();
				if (parts_size < 10) {
					continue;
				}

				std::string time = parts[0];
				int64_t timestamp_ms = static_cast<int64_t>(std::stoll(parts[1]));
				std::string logName = parts[2];
				std::string level = parts[3];
				std::string logTag = parts[4];
				std::string filename = parts[5];
				std::string fileline = parts[6];
				std::string func = parts[7];
				std::string message = parts[9];
				message = message.substr(std::string("message:").length());

				output.SetValue(0, row_index, Value(time));
				output.SetValue(1, row_index, Value::TIMESTAMPMS(timestamp_ms_t(timestamp_ms)));
				output.SetValue(2, row_index, Value(logName));
				output.SetValue(3, row_index, Value(level));
				output.SetValue(4, row_index, Value(logTag));
				output.SetValue(5, row_index, Value(filename + "." + fileline));
				output.SetValue(6, row_index, Value(func));
				output.SetValue(7, row_index, Value(message));

				auto extra_value = Value();
				if (parts_size > 12) {
					std::string extra_info;
					for (size_t i = 12; i < parts_size; i++) {
						extra_info += parts[i];
						if (i < parts_size - 1) {
							extra_info += " ";
						}
					}
					extra_value = Value(extra_info);
				}
				output.SetValue(8, row_index, extra_value);

				row_index++;
				if (row_index == STANDARD_VECTOR_SIZE) {
					output.SetCardinality(row_index);
					return;
				}
			}
		}

		data.current_file.close(); // 当前文件读取完毕
	}

	output.SetCardinality(row_index);
}

static unique_ptr<FunctionData> ReadShowstartLogBind(ClientContext &context, TableFunctionBindInput &bind_input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ReadShowstartLogFunctionData>();
	auto multi_file_reader = MultiFileReader::Create(bind_input.table_function);
	result->files =
	    multi_file_reader->CreateFileList(context, bind_input.inputs[0], FileGlobOptions::ALLOW_EMPTY)->GetAllFiles();

	names.emplace_back("time");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("timestamp");
	return_types.emplace_back(LogicalType::TIMESTAMP_MS);
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("level");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("tag");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("file");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("func");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("message");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("extra");
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(result);
}

#ifdef DUCKDB_CPP_EXTENSION_ENTRY
static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
#else
static void LoadInternal(DatabaseInstance &instance) {
#endif
	CreateTableFunctionInfo read_showstart_log_info(
	    TableFunction("read_showstart_log", {LogicalType::VARCHAR}, ReadShowstartLogFunction, ReadShowstartLogBind));
	ExtensionUtil::RegisterFunction(instance, read_showstart_log_info);
	std::string description = StringUtil::Format("秀动log分析");
	ExtensionUtil::RegisterExtension(instance, "showstart_log", ExtensionLoadedInfo {std::move(description)});
}

#ifdef DUCKDB_CPP_EXTENSION_ENTRY
void ShowstartLogExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
#else
void ShowstartLogExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
#endif

std::string ShowstartLogExtension::Name() {
	return "showstart_log";
}

std::string ShowstartLogExtension::Version() const {
#ifdef EXT_VERSION_SHOWSTART_LOG
	return EXT_VERSION_SHOWSTART_LOG;
#else
	return "";
#endif
}

void ReadShowstartLogFunctionData::OpenNextFile(FileSystem &fs) {
	if (file_index >= files.size()) {
		finished = true;
		return;
	}
	current_path = files[file_index].path;
	current_file.close();
	current_file = std::ifstream(current_path);
	if (!current_file.is_open()) {
		throw IOException("Could not open file: " + current_path);
	}
	file_index++;
}

} // namespace duckdb

extern "C" {

#ifdef DUCKDB_CPP_EXTENSION_ENTRY
DUCKDB_CPP_EXTENSION_ENTRY(showstart_log, loader) {
	duckdb::LoadInternal(loader);
}
#else
DUCKDB_EXTENSION_API void showstart_log_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::ShowstartLogExtension>();
}
#endif

DUCKDB_EXTENSION_API const char *showstart_log_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif