#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

#include <fstream>

namespace duckdb {

class ShowstartLogExtension : public Extension {
public:
#ifdef DUCKDB_CPP_EXTENSION_ENTRY
	void Load(ExtensionLoader &loader) override;
#else
	void Load(DuckDB &db) override;
#endif
	std::string Name() override;
	std::string Version() const override;
};

struct ReadShowstartLogFunctionData : public TableFunctionData {
	vector<OpenFileInfo> files;
	idx_t file_index = 0;
	std::ifstream current_file;
	std::string current_path;
	bool finished = false;

	ReadShowstartLogFunctionData() = default;
	void OpenNextFile(FileSystem &fs);
};

} // namespace duckdb
