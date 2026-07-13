#include "connector/duckdb_connector.h"

namespace kuzu {
namespace duckdb_extension {

std::string quoteDuckDBIdentifier(const std::string& identifier) {
    std::string result = "\"";
    for (const auto character : identifier) {
        if (character == '"') {
            result += "\"";
        }
        result += character;
    }
    return result + "\"";
}

std::string quoteDuckDBStringLiteral(const std::string& value) {
    std::string result = "'";
    for (const auto character : value) {
        if (character == '\'') {
            result += "'";
        }
        result += character;
    }
    return result + "'";
}

std::unique_ptr<duckdb::MaterializedQueryResult> DuckDBConnector::executeQuery(
    std::string query) const {
    KU_ASSERT(instance != nullptr && connection != nullptr);
    auto result = connection->Query(query);
    if (result->HasError()) {
        throw common::Exception{result->GetError()};
    }
    return result;
}

DuckDBStreamingResult DuckDBConnector::executeStreamingQuery(std::string query) const {
    KU_ASSERT(instance != nullptr && connection != nullptr);
    auto queryConnection = std::make_unique<duckdb::Connection>(*instance);
    auto result = queryConnection->SendQuery(query);
    if (result->HasError()) {
        throw common::Exception{result->GetError()};
    }
    return DuckDBStreamingResult{std::move(queryConnection), std::move(result)};
}

} // namespace duckdb_extension
} // namespace kuzu
