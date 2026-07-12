#include "binder/binder.h"
#include "function/table/bind_data.h"
#include "function/table/simple_table_function.h"
#include "main/client_context.h"
#include "storage/page_manager.h"

namespace kuzu {
namespace function {

struct FreeSpaceInfoBindData final : TableFuncBindData {
    std::vector<storage::PageRange> entries;

    FreeSpaceInfoBindData(binder::expression_vector columns,
        std::vector<storage::PageRange> entries)
        : TableFuncBindData{std::move(columns), entries.size()}, entries{std::move(entries)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<FreeSpaceInfoBindData>(columns, entries);
    }
};

static common::offset_t internalTableFunc(const TableFuncMorsel& morsel,
    const TableFuncInput& input, common::DataChunk& output) {
    const auto bindData = input.bindData->constPtrCast<FreeSpaceInfoBindData>();
    common::row_idx_t outputOffset = 0;
    for (auto entryOffset = morsel.startOffset; entryOffset < morsel.endOffset; ++entryOffset) {
        const auto& freeEntry = bindData->entries[entryOffset];
        output.getValueVectorMutable(0).setValue<uint64_t>(outputOffset, freeEntry.startPageIdx);
        output.getValueVectorMutable(1).setValue<uint64_t>(outputOffset, freeEntry.numPages);
        ++outputOffset;
    }
    return outputOffset;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* context,
    const TableFuncBindInput* input) {
    std::vector<std::string> columnNames = {"start_page_idx", "num_pages"};
    std::vector<common::LogicalType> columnTypes;
    columnTypes.push_back(common::LogicalType::UINT64());
    columnTypes.push_back(common::LogicalType::UINT64());
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<FreeSpaceInfoBindData>(columns,
        storage::PageManager::Get(*context)->getFreeEntries());
}

function_set FreeSpaceInfoFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<common::LogicalTypeID>{});
    function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = SimpleTableFunc::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
