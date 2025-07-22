#ifndef DUCKDB_SHOWSTART_LOG_EXTENSION_H
#define DUCKDB_SHOWSTART_LOG_EXTENSION_H

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class ShowstartLogExtension {
public:
	static void Load(DuckDB &db);
	static std::string Name();
	std::string Version() const;
};

struct ReadShowstartLogFunctionData : public TableFunctionData {
	ReadShowstartLogFunctionData() {}
	vector<std::string> files;
	idx_t row_index = 0;
};

} // namespace duckdb

#endif