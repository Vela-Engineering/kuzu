#include "storage/page_manager.h"

#include "common/exception/runtime.h"
#include "common/uniq_lock.h"
#include "storage/file_handle.h"
#include "storage/storage_manager.h"

namespace kuzu::storage {
static constexpr bool ENABLE_FSM = true;

PageRange PageManager::allocatePageRange(common::page_idx_t numPages) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        auto allocatedFreeChunk = freeSpaceManager->popFreePages(numPages);
        if (allocatedFreeChunk.has_value()) {
            version.fetch_add(1, std::memory_order_relaxed);
            return {*allocatedFreeChunk};
        }
    }
    auto startPageIdx = fileHandle->addNewPages(numPages);
    const auto fileNumPages = fileHandle->getNumPages();
    if (startPageIdx > fileNumPages || numPages > fileNumPages - startPageIdx) {
        throw common::RuntimeException(common::stringFormat(
            "Allocated page range [{}, {}) exceeds the file size of {} pages.", startPageIdx,
            startPageIdx + numPages, fileNumPages));
    }
    version.fetch_add(1, std::memory_order_relaxed);
    return PageRange(startPageIdx, numPages);
}

void PageManager::freePageRange(PageRange entry) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        // Freed pages cannot be immediately reused to ensure checkpoint recovery works
        // Instead they are reusable after the end of the next checkpoint
        freeSpaceManager->addUncheckpointedFreePages(entry);
        version.fetch_add(1, std::memory_order_relaxed);
    }
}

void PageManager::freePageRange(PageRange entry,
    const std::vector<PageRange>& rangesToExclude) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        if (freeSpaceManager->addUncheckpointedFreePages(entry, rangesToExclude)) {
            version.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

bool PageManager::removeFreePageRange(PageRange entry) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        const auto removedPages = freeSpaceManager->removeFreePages(entry);
        if (removedPages) {
            version.fetch_add(1, std::memory_order_relaxed);
        }
        return removedPages;
    }
    return false;
}

common::page_idx_t PageManager::estimatePagesNeededForSerialize() {
    common::UniqLock lck{mtx};
    return freeSpaceManager->getMaxNumPagesForSerialization();
}

void PageManager::freeImmediatelyRewritablePageRange(FileHandle* fileHandle, PageRange entry) {
    if constexpr (ENABLE_FSM) {
        common::UniqLock lck{mtx};
        freeSpaceManager->evictAndAddFreePages(fileHandle, entry);
        version.fetch_add(1, std::memory_order_relaxed);
    }
}

void PageManager::serialize(common::Serializer& serializer) {
    common::UniqLock lck{mtx};
    freeSpaceManager->serialize(serializer);
}

void PageManager::deserialize(common::Deserializer& deSer,
    const std::vector<PageRange>& rangesToExclude) {
    common::UniqLock lck{mtx};
    if (freeSpaceManager->deserialize(deSer, fileHandle->getNumPages(), rangesToExclude)) {
        version.fetch_add(1, std::memory_order_relaxed);
    }
}

void PageManager::finalizeCheckpoint() {
    common::UniqLock lck{mtx};
    freeSpaceManager->finalizeCheckpoint(fileHandle);
}

void PageManager::rollbackCheckpoint() {
    common::UniqLock lck{mtx};
    freeSpaceManager->rollbackCheckpoint();
}

common::row_idx_t PageManager::getNumFreeEntries() const {
    common::UniqLock lck{mtx};
    return freeSpaceManager->getNumEntries();
}

std::vector<PageRange> PageManager::getFreeEntries() const {
    common::UniqLock lck{mtx};
    return freeSpaceManager->getEntries(0, freeSpaceManager->getNumEntries());
}

std::vector<PageRange> PageManager::getFreeEntries(common::row_idx_t startOffset,
    common::row_idx_t endOffset) const {
    common::UniqLock lck{mtx};
    return freeSpaceManager->getEntries(startOffset, endOffset);
}

void PageManager::clearEvictedBMEntriesIfNeeded(BufferManager* bufferManager) {
    common::UniqLock lck{mtx};
    freeSpaceManager->clearEvictedBufferManagerEntriesIfNeeded(bufferManager);
}

PageManager* PageManager::Get(const main::ClientContext& context) {
    return StorageManager::Get(context)->getDataFH()->getPageManager();
}

} // namespace kuzu::storage
