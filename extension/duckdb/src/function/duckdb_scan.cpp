#include "function/duckdb_scan.h"

#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "connector/duckdb_connector.h"
#include "function/table/bind_input.h"
#include "function/table/table_function.h"
#include "processor/execution_context.h"

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace duckdb_extension {

std::string DuckDBScanBindData::getColumnsToSelect() const {
    auto skips = getColumnSkips();
    std::string columnNames;
    auto numSkippedColumns =
        std::count_if(skips.begin(), skips.end(), [](auto item) { return item; });
    if (getNumColumns() == numSkippedColumns) {
        return quoteDuckDBIdentifier(columnNamesInDuckDB[0]);
    }
    for (auto i = 0u; i < getNumColumns(); i++) {
        if (skips[i]) {
            continue;
        }
        if (!columnNames.empty()) {
            columnNames += ",";
        }
        columnNames += quoteDuckDBIdentifier(columnNamesInDuckDB[i]);
    }
    return columnNames;
}

std::string DuckDBScanBindData::getQuery() const {
    auto finalQuery = query;
    const auto projectionPosition = finalQuery.find("{}");
    KU_ASSERT(projectionPosition != std::string::npos);
    finalQuery.replace(projectionPosition, 2, getColumnsToSelect());
    return finalQuery;
}

DuckDBScanSharedState::DuckDBScanSharedState(
    std::unique_ptr<duckdb::MaterializedQueryResult> queryResult)
    : function::TableFuncSharedState{queryResult->RowCount()}, queryResult{std::move(queryResult)} {
}

DuckDBScanSharedState::DuckDBScanSharedState(std::unique_ptr<duckdb::Connection> connection,
    std::unique_ptr<duckdb::QueryResult> queryResult)
    : function::TableFuncSharedState{0}, connection{std::move(connection)},
      queryResult{std::move(queryResult)} {}

struct DuckDBScanFunction {
    static constexpr char DUCKDB_SCAN_FUNC_NAME[] = "duckdb_scan";

    static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output);

    static std::unique_ptr<TableFuncBindData> bindFunc(
        std::shared_ptr<DuckDBTableScanInfo> bindData, main::ClientContext* /*context*/,
        const TableFuncBindInput* input);

    static std::unique_ptr<TableFuncSharedState> initSharedState(
        const TableFuncInitSharedStateInput& input);

    static std::unique_ptr<TableFuncLocalState> initLocalState(
        const TableFuncInitLocalStateInput& input);
};

std::unique_ptr<TableFuncSharedState> DuckDBScanFunction::initSharedState(
    const TableFuncInitSharedStateInput& input) {
    auto scanBindData = input.bindData->constPtrCast<DuckDBScanBindData>();
    std::string predicatesString = "";
    for (auto& predicates : scanBindData->getColumnPredicates()) {
        if (predicates.isEmpty()) {
            continue;
        }
        if (predicatesString.empty()) {
            predicatesString = " WHERE " + predicates.toString();
        } else {
            predicatesString += stringFormat(" AND {}", predicates.toString());
        }
    }
    auto result =
        scanBindData->connector.executeStreamingQuery(scanBindData->getQuery() + predicatesString);
    return std::make_unique<DuckDBScanSharedState>(std::move(result.connection),
        std::move(result.queryResult));
}

std::unique_ptr<TableFuncLocalState> DuckDBScanFunction::initLocalState(
    const TableFuncInitLocalStateInput&) {
    return std::make_unique<TableFuncLocalState>();
}

offset_t DuckDBScanFunction::tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto duckdbScanSharedState = input.sharedState->ptrCast<DuckDBScanSharedState>();
    auto duckdbScanBindData = input.bindData->constPtrCast<DuckDBScanBindData>();
    std::unique_ptr<duckdb::DataChunk> result;
    {
        std::lock_guard<std::mutex> lock{duckdbScanSharedState->mtx};
        if (duckdbScanSharedState->finished) {
            return 0;
        }
        try {
            result = duckdbScanSharedState->queryResult->Fetch();
        } catch (const std::exception& e) {
            throw RuntimeException(
                stringFormat("Failed to fetch DuckDB query result: {}", e.what()));
        }
        if (duckdbScanSharedState->queryResult->HasError()) {
            throw RuntimeException(stringFormat("Failed to fetch DuckDB query result: {}",
                duckdbScanSharedState->queryResult->GetError()));
        }
        if (result == nullptr) {
            duckdbScanSharedState->finished = true;
            return 0;
        }
    }
    duckdbScanBindData->converter.convertDuckDBResultToVector(*result, output.dataChunk,
        duckdbScanBindData->getColumnSkips());
    return output.dataChunk.state->getSelVector().getSelSize();
}

std::unique_ptr<TableFuncBindData> DuckDBScanFunction::bindFunc(
    std::shared_ptr<DuckDBTableScanInfo> scanInfo, main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto columnNames =
        TableFunction::extractYieldVariables(scanInfo->getColumnNames(), input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, scanInfo->getColumnTypes(*context));
    return std::make_unique<DuckDBScanBindData>(scanInfo->getTemplateQuery(*context),
        scanInfo->getColumnNames(), scanInfo->getConnector(), std::move(columns));
}

TableFunction getScanFunction(std::shared_ptr<DuckDBTableScanInfo> scanInfo) {
    auto function =
        TableFunction(DuckDBScanFunction::DUCKDB_SCAN_FUNC_NAME, std::vector<LogicalTypeID>{});
    function.tableFunc = DuckDBScanFunction::tableFunc;
    function.bindFunc = std::bind(DuckDBScanFunction::bindFunc, scanInfo, std::placeholders::_1,
        std::placeholders::_2);
    function.initSharedStateFunc = DuckDBScanFunction::initSharedState;
    function.initLocalStateFunc = DuckDBScanFunction::initLocalState;
    return function;
}

} // namespace duckdb_extension
} // namespace kuzu
