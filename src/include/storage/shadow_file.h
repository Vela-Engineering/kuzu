#pragma once

#include <optional>

#include "common/types/uuid.h"
#include "storage/file_handle.h"

namespace kuzu {
namespace storage {

struct ShadowPageRecord {
    common::file_idx_t originalFileIdx = common::INVALID_PAGE_IDX;
    common::page_idx_t originalPageIdx = common::INVALID_PAGE_IDX;
    common::page_idx_t shadowPageIdx = common::INVALID_PAGE_IDX;

    void serialize(common::Serializer& serializer) const;
    static ShadowPageRecord deserialize(
        common::Deserializer& deserializer, bool includeShadowPageIdx = false);
};

struct ShadowFileHeader {
    common::ku_uuid_t databaseID{0};
    common::page_idx_t numShadowPages = 0;
    uint64_t checkpointPageWatermarkMagic = 0;
    common::ku_uuid_t checkpointID{0};
    common::page_idx_t checkpointStartDataFileNumPages = common::INVALID_PAGE_IDX;
    common::page_idx_t checkpointStartDataFileNumPagesCheck = common::INVALID_PAGE_IDX;
};
static_assert(std::is_trivially_copyable_v<ShadowFileHeader>);

class BufferManager;
// NOTE: This class is NOT thread-safe for now, as we are not checkpointing in parallel yet.
class ShadowFile {
public:
    ShadowFile(BufferManager& bm, common::VirtualFileSystem* vfs, const std::string& databasePath);

    // TODO(Guodong): Remove originalFile param.
    bool hasShadowPage(common::file_idx_t originalFile, common::page_idx_t originalPage) const {
        return shadowPagesMap.contains(originalFile) &&
               shadowPagesMap.at(originalFile).contains(originalPage);
    }
    void clearShadowPage(common::file_idx_t originalFile, common::page_idx_t originalPage);
    common::page_idx_t getShadowPage(common::file_idx_t originalFile,
        common::page_idx_t originalPage) const;
    common::page_idx_t getOrCreateShadowPage(common::file_idx_t originalFile,
        common::page_idx_t originalPage);

    FileHandle& getShadowingFH() const { return *shadowingFH; }

    void applyShadowPages(main::ClientContext& context) const;

    common::ku_uuid_t beginCheckpoint(main::ClientContext& context,
        common::page_idx_t dataFileNumPages);
    common::page_idx_t getCheckpointStartDataFileNumPages() const {
        return checkpointStartDataFileNumPages;
    }
    void flushAll(main::ClientContext& context);
    void clear(BufferManager& bm);
    void reset();

    // Replay shadow page records from the shadow file to the original data file. This is used
    // during recovery.
    static void replayShadowPageRecords(main::ClientContext& context,
        common::ku_uuid_t expectedDatabaseID,
        std::optional<common::ku_uuid_t> expectedCheckpointID,
        std::optional<common::page_idx_t> expectedCheckpointStartDataFileNumPages,
        std::optional<common::page_idx_t> expectedCheckpointEndDataFileNumPages);
    static void rollbackCheckpoint(main::ClientContext& context,
        std::optional<common::ku_uuid_t> expectedDatabaseID,
        std::optional<common::ku_uuid_t> expectedCheckpointID,
        std::optional<common::page_idx_t> expectedCheckpointStartDataFileNumPages);

private:
    FileHandle* getOrCreateShadowingFH();
    void writeHeader(main::ClientContext& context) const;

private:
    BufferManager& bm;
    std::string shadowFilePath;
    common::VirtualFileSystem* vfs;
    // This is the file handle for the shadow file. It is created lazily when the first shadow page
    // is created.
    FileHandle* shadowingFH;
    // The map caches shadow page idxes for pages in original files.
    std::unordered_map<common::file_idx_t,
        std::unordered_map<common::page_idx_t, common::page_idx_t>>
        shadowPagesMap;
    std::vector<ShadowPageRecord> shadowPageRecords;
    common::page_idx_t checkpointStartDataFileNumPages = common::INVALID_PAGE_IDX;
    std::optional<common::ku_uuid_t> checkpointID;
};

} // namespace storage
} // namespace kuzu
