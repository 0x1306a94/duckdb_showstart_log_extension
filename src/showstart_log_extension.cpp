#define DUCKDB_EXTENSION_MAIN

#include "showstart_log_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
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
