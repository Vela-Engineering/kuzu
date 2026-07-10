#include "storage/free_space_manager.h"

#include <algorithm>

#include "common/exception/runtime.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/in_mem_file_writer.h"
#include "common/serializer/serializer.h"
#include "common/utils.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/file_handle.h"
#include "storage/page_range.h"

namespace kuzu::storage {
static FreeSpaceManager::sorted_free_list_t& getFreeList(
    std::vector<FreeSpaceManager::sorted_free_list_t>& freeLists, common::idx_t level) {
    if (level >= freeLists.size()) {
        freeLists.resize(level + 1,
            FreeSpaceManager::sorted_free_list_t{&FreeSpaceManager::entryCmp});
    }
    return freeLists[level];
}

FreeSpaceManager::FreeSpaceManager() : freeLists{}, numEntries(0), needClearEvictedEntries(false){};

common::idx_t FreeSpaceManager::getLevel(common::page_idx_t numPages) {
    // level is exponent of largest power of 2 that is <= numPages
    // e.g. 2 -> level 1, 5 -> level 2
    KU_ASSERT(numPages > 0);
    return common::CountZeros<common::page_idx_t>::Trailing(std::bit_floor(numPages));
}

bool FreeSpaceManager::entryCmp(const PageRange& a, const PageRange& b) {
    return a.numPages == b.numPages ? a.startPageIdx < b.startPageIdx : a.numPages < b.numPages;
}

void FreeSpaceManager::addFreePages(PageRange entry) {
    if (entry.numPages == 0) {
        throw common::RuntimeException("Cannot add an empty range to the free space manager.");
    }
    const auto entryLevel = getLevel(entry.numPages);
    if (getFreeList(freeLists, entryLevel).contains(entry)) {
        throw common::RuntimeException(common::stringFormat(
            "Duplicate free page range [{}, {}).", entry.startPageIdx,
            entry.startPageIdx + entry.numPages));
    }
    getFreeList(freeLists, entryLevel).insert(entry);
    ++numEntries;
}

void FreeSpaceManager::evictAndAddFreePages(FileHandle* fileHandle, PageRange entry) {
    evictPages(fileHandle, entry);
    addFreePages(entry);
}

void FreeSpaceManager::addUncheckpointedFreePages(PageRange entry) {
    uncheckpointedFreePageRanges.push_back(entry);
}

static bool isValidPageRange(const PageRange& entry) {
    return entry.startPageIdx != common::INVALID_PAGE_IDX && entry.numPages > 0;
}

static common::page_idx_t getEndPageIdx(const PageRange& entry) {
    return entry.startPageIdx + entry.numPages;
}

static bool overlaps(const PageRange& a, const PageRange& b) {
    return isValidPageRange(a) && isValidPageRange(b) && a.startPageIdx < getEndPageIdx(b) &&
           b.startPageIdx < getEndPageIdx(a);
}

static bool areSamePageRange(const PageRange& a, const PageRange& b) {
    return a.startPageIdx == b.startPageIdx && a.numPages == b.numPages;
}

static FreeSpaceManager::free_list_t subtractPageRange(PageRange entry,
    const PageRange& rangeToExclude) {
    if (!overlaps(entry, rangeToExclude)) {
        return {entry};
    }
    FreeSpaceManager::free_list_t ret;
    const auto entryEnd = getEndPageIdx(entry);
    const auto excludeEnd = getEndPageIdx(rangeToExclude);
    if (entry.startPageIdx < rangeToExclude.startPageIdx) {
        ret.emplace_back(entry.startPageIdx, rangeToExclude.startPageIdx - entry.startPageIdx);
    }
    if (excludeEnd < entryEnd) {
        ret.emplace_back(excludeEnd, entryEnd - excludeEnd);
    }
    return ret;
}

static FreeSpaceManager::free_list_t subtractPageRanges(PageRange entry,
    const std::vector<PageRange>& rangesToExclude) {
    if (!isValidPageRange(entry)) {
        return {};
    }
    FreeSpaceManager::free_list_t remainingRanges{entry};
    for (const auto& rangeToExclude : rangesToExclude) {
        FreeSpaceManager::free_list_t nextRemainingRanges;
        for (const auto& remainingRange : remainingRanges) {
            auto subtractedRanges = subtractPageRange(remainingRange, rangeToExclude);
            nextRemainingRanges.insert(nextRemainingRanges.end(), subtractedRanges.begin(),
                subtractedRanges.end());
        }
        remainingRanges = std::move(nextRemainingRanges);
        if (remainingRanges.empty()) {
            break;
        }
    }
    return remainingRanges;
}

bool FreeSpaceManager::addUncheckpointedFreePages(PageRange entry,
    const std::vector<PageRange>& rangesToExclude) {
    auto remainingRanges = subtractPageRanges(entry, rangesToExclude);
    uncheckpointedFreePageRanges.insert(uncheckpointedFreePageRanges.end(), remainingRanges.begin(),
        remainingRanges.end());
    return !remainingRanges.empty();
}

bool FreeSpaceManager::removeFreePages(PageRange entry) {
    if (!isValidPageRange(entry)) {
        return false;
    }
    bool removedAnyPages = false;
    free_list_t remainingCheckpointedRanges;
    auto entryIt = FreeEntryIterator{freeLists};
    while (!entryIt.done()) {
        const auto freeEntry = *entryIt;
        auto remainingRanges = subtractPageRange(freeEntry, entry);
        removedAnyPages = removedAnyPages || remainingRanges.size() != 1 ||
                          !areSamePageRange(remainingRanges[0], freeEntry);
        remainingCheckpointedRanges.insert(remainingCheckpointedRanges.end(),
            remainingRanges.begin(), remainingRanges.end());
        ++entryIt;
    }
    if (removedAnyPages) {
        resetFreeLists();
        for (const auto& remainingRange : remainingCheckpointedRanges) {
            addFreePages(remainingRange);
        }
    }

    free_list_t remainingUncheckpointedRanges;
    for (const auto& freeEntry : uncheckpointedFreePageRanges) {
        auto remainingRanges = subtractPageRange(freeEntry, entry);
        removedAnyPages = removedAnyPages || remainingRanges.size() != 1 ||
                          !areSamePageRange(remainingRanges[0], freeEntry);
        remainingUncheckpointedRanges.insert(remainingUncheckpointedRanges.end(),
            remainingRanges.begin(), remainingRanges.end());
    }
    uncheckpointedFreePageRanges = std::move(remainingUncheckpointedRanges);
    return removedAnyPages;
}

void FreeSpaceManager::rollbackCheckpoint() {
    uncheckpointedFreePageRanges.clear();
}

// This also removes the chunk from the free space manager
std::optional<PageRange> FreeSpaceManager::popFreePages(common::page_idx_t numPages) {
    if (numPages > 0) {
        auto levelToSearch = getLevel(numPages);
        for (; levelToSearch < freeLists.size(); ++levelToSearch) {
            auto& curList = freeLists[levelToSearch];
            auto entryIt = curList.lower_bound(PageRange{0, numPages});
            if (entryIt != curList.end()) {
                auto entry = *entryIt;
                curList.erase(entryIt);
                --numEntries;
                return splitPageRange(entry, numPages);
            }
        }
    }
    return std::nullopt;
}

PageRange FreeSpaceManager::splitPageRange(PageRange chunk, common::page_idx_t numRequiredPages) {
    if (chunk.numPages < numRequiredPages) {
        throw common::RuntimeException(common::stringFormat(
            "Cannot allocate {} pages from a free range containing {} pages.", numRequiredPages,
            chunk.numPages));
    }
    PageRange ret{chunk.startPageIdx, numRequiredPages};
    if (numRequiredPages < chunk.numPages) {
        PageRange remainingEntry{chunk.startPageIdx + numRequiredPages,
            chunk.numPages - numRequiredPages};
        addFreePages(remainingEntry);
    }
    return ret;
}

struct SerializePagesUsedTracker {
    common::page_idx_t numPagesUsed;
    uint64_t numBytesUsedInPage;

    void updatePagesUsed(uint64_t numBytesToAdd) {
        if (numBytesUsedInPage + numBytesToAdd > common::InMemFileWriter::getPageSize()) {
            ++numPagesUsed;
            numBytesUsedInPage = 0;
        }
        numBytesUsedInPage += numBytesToAdd;
    }

    template<typename T>
    void processValue(T) {
        updatePagesUsed(sizeof(T));
    }

    void processDebuggingInfo(const std::string& value) {
        updatePagesUsed(sizeof(uint64_t) + value.size());
    }
};

struct ValueSerializer {
    common::Serializer& ser;

    template<typename T>
    void processValue(T value) {
        ser.write(value);
    }

    void processDebuggingInfo(const std::string& value) { ser.writeDebuggingInfo(value); }
};

template<typename ValueProcessor>
static common::row_idx_t serializeCheckpointedEntries(
    const std::vector<FreeSpaceManager::sorted_free_list_t>& freeLists, ValueProcessor& ser) {
    auto entryIt = FreeEntryIterator{freeLists};
    common::row_idx_t numWrittenEntries = 0;
    while (!entryIt.done()) {
        const auto entry = *entryIt;
        ser.processValue(entry.startPageIdx);
        ser.processValue(entry.numPages);
        ++entryIt;
        ++numWrittenEntries;
    }
    return numWrittenEntries;
}

template<typename ValueProcessor>
static common::row_idx_t serializeUncheckpointedEntries(
    const FreeSpaceManager::free_list_t& uncheckpointedEntries, ValueProcessor& ser) {
    for (const auto& entry : uncheckpointedEntries) {
        ser.processValue(entry.startPageIdx);
        ser.processValue(entry.numPages);
    }
    return uncheckpointedEntries.size();
}

template<typename ValueProcessor>
void FreeSpaceManager::serializeInternal(ValueProcessor& ser) const {
    // we also serialize uncheckpointed entries as serialize() may be called before
    // finalizeCheckpoint()
    ser.processDebuggingInfo("page_manager");
    const auto numEntries = getNumEntries() + uncheckpointedFreePageRanges.size();
    ser.processDebuggingInfo("numEntries");
    ser.processValue(numEntries);
    ser.processDebuggingInfo("entries");
    [[maybe_unused]] const auto numCheckpointedEntries =
        serializeCheckpointedEntries(freeLists, ser);
    [[maybe_unused]] const auto numUncheckpointedEntries =
        serializeUncheckpointedEntries(uncheckpointedFreePageRanges, ser);
    KU_ASSERT(numCheckpointedEntries + numUncheckpointedEntries == numEntries);
}

common::page_idx_t FreeSpaceManager::getMaxNumPagesForSerialization() const {
    SerializePagesUsedTracker ser{};
    serializeInternal(ser);
    return ser.numPagesUsed + (ser.numBytesUsedInPage > 0);
}

void FreeSpaceManager::serialize(common::Serializer& ser) const {
    ValueSerializer serWrapper{.ser = ser};
    serializeInternal(serWrapper);
}

bool FreeSpaceManager::deserialize(common::Deserializer& deSer, common::page_idx_t maxNumPages,
    const std::vector<PageRange>& rangesToExclude) {
    std::string key;

    deSer.validateDebuggingInfo(key, "page_manager");
    deSer.validateDebuggingInfo(key, "numEntries");
    common::row_idx_t numEntries{};
    deSer.deserializeValue<common::row_idx_t>(numEntries);

    deSer.validateDebuggingInfo(key, "entries");
    free_list_t sanitizedEntries;
    bool repaired = false;
    for (common::row_idx_t i = 0; i < numEntries; ++i) {
        PageRange entry{};
        deSer.deserializeValue<common::page_idx_t>(entry.startPageIdx);
        deSer.deserializeValue<common::page_idx_t>(entry.numPages);
        if (!isValidPageRange(entry) || entry.startPageIdx >= maxNumPages) {
            repaired = true;
            continue;
        }
        const auto maxRangePages = maxNumPages - entry.startPageIdx;
        if (entry.numPages > maxRangePages) {
            entry.numPages = maxRangePages;
            repaired = true;
        }
        auto remainingRanges = subtractPageRanges(entry, rangesToExclude);
        if (remainingRanges.size() != 1 || !areSamePageRange(remainingRanges[0], entry)) {
            repaired = true;
        }
        sanitizedEntries.insert(sanitizedEntries.end(), remainingRanges.begin(),
            remainingRanges.end());
    }
    std::sort(sanitizedEntries.begin(), sanitizedEntries.end(),
        [](const auto& a, const auto& b) { return a.startPageIdx < b.startPageIdx; });
    free_list_t mergedEntries;
    for (const auto& entry : sanitizedEntries) {
        if (mergedEntries.empty() || getEndPageIdx(mergedEntries.back()) < entry.startPageIdx) {
            mergedEntries.push_back(entry);
            continue;
        }
        const auto mergedEnd = getEndPageIdx(mergedEntries.back());
        const auto entryEnd = getEndPageIdx(entry);
        if (mergedEnd > entry.startPageIdx) {
            repaired = true;
        }
        mergedEntries.back().numPages = std::max(mergedEnd, entryEnd) -
                                        mergedEntries.back().startPageIdx;
    }
    resetFreeLists();
    for (const auto& entry : mergedEntries) {
        addFreePages(entry);
    }
    return repaired;
}

void FreeSpaceManager::evictPages(FileHandle* fileHandle, const PageRange& entry) {
    needClearEvictedEntries = true;
    for (uint64_t i = 0; i < entry.numPages; ++i) {
        const auto pageIdx = entry.startPageIdx + i;
        fileHandle->removePageFromFrameIfNecessary(pageIdx);
    }
}

void FreeSpaceManager::finalizeCheckpoint(FileHandle* fileHandle) {
    // evict pages before they're added to the free list
    for (const auto& entry : uncheckpointedFreePageRanges) {
        evictPages(fileHandle, entry);
    }

    mergePageRanges(std::move(uncheckpointedFreePageRanges), fileHandle);
    uncheckpointedFreePageRanges.clear();
}

void FreeSpaceManager::resetFreeLists() {
    freeLists.clear();
    numEntries = 0;
}

void FreeSpaceManager::mergePageRanges(free_list_t newInitialEntries, FileHandle* fileHandle) {
    free_list_t allEntries = std::move(newInitialEntries);
    for (const auto& freeList : freeLists) {
        allEntries.insert(allEntries.end(), freeList.begin(), freeList.end());
    }
    if (allEntries.empty()) {
        return;
    }

    resetFreeLists();
    std::sort(allEntries.begin(), allEntries.end(), [](const auto& entryA, const auto& entryB) {
        return entryA.startPageIdx < entryB.startPageIdx;
    });

    PageRange prevEntry = allEntries[0];
    for (common::row_idx_t i = 1; i < allEntries.size(); ++i) {
        const auto& entry = allEntries[i];
        const auto previousEnd = prevEntry.startPageIdx + prevEntry.numPages;
        if (previousEnd > entry.startPageIdx) {
            throw common::RuntimeException(common::stringFormat(
                "Overlapping free page ranges [{}, {}) and [{}, {}).", prevEntry.startPageIdx,
                previousEnd, entry.startPageIdx, entry.startPageIdx + entry.numPages));
        }
        if (previousEnd == entry.startPageIdx) {
            prevEntry.numPages += entry.numPages;
        } else {
            addFreePages(prevEntry);
            prevEntry = entry;
        }
    }
    handleLastPageRange(prevEntry, fileHandle);
}

void FreeSpaceManager::handleLastPageRange(PageRange pageRange, FileHandle* fileHandle) {
    KU_UNUSED(fileHandle);
    addFreePages(pageRange);
}

common::row_idx_t FreeSpaceManager::getNumEntries() const {
    return numEntries;
}

std::vector<PageRange> FreeSpaceManager::getEntries(common::row_idx_t startOffset,
    common::row_idx_t endOffset) const {
    KU_ASSERT(endOffset >= startOffset);
    std::vector<PageRange> ret;
    FreeEntryIterator it{freeLists};
    it.advance(startOffset);
    while (ret.size() < endOffset - startOffset) {
        KU_ASSERT(!it.done());
        ret.push_back(*it);
        ++it;
    }
    return ret;
}

void FreeSpaceManager::clearEvictedBufferManagerEntriesIfNeeded(BufferManager* bufferManager) {
    if (needClearEvictedEntries) {
        bufferManager->removeEvictedCandidates();
        needClearEvictedEntries = false;
    }
}

void FreeEntryIterator::advance(common::row_idx_t numEntries) {
    for (common::row_idx_t i = 0; i < numEntries; ++i) {
        ++(*this);
    }
}

void FreeEntryIterator::operator++() {
    KU_ASSERT(freeListIdx < freeLists.size());
    ++freeListIt;
    if (freeListIt == freeLists[freeListIdx].end()) {
        ++freeListIdx;
        advanceFreeListIdx();
    }
}

bool FreeEntryIterator::done() const {
    return freeListIdx >= freeLists.size();
}

void FreeEntryIterator::advanceFreeListIdx() {
    for (; freeListIdx < freeLists.size(); ++freeListIdx) {
        if (!freeLists[freeListIdx].empty()) {
            freeListIt = freeLists[freeListIdx].begin();
            break;
        }
    }
}

PageRange FreeEntryIterator::operator*() const {
    KU_ASSERT(freeListIdx < freeLists.size() && freeListIt != freeLists[freeListIdx].end());
    return *freeListIt;
}

} // namespace kuzu::storage
