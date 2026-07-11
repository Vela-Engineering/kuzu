#pragma once

#include "common/api.h"
#include "exception.h"

namespace kuzu {
namespace common {

class KUZU_API WALCommitException : public Exception {
public:
    explicit WALCommitException(const std::exception& e)
        : Exception("WAL synchronization failed; the commit outcome is unknown and the database "
                    "must be restarted: " +
                    std::string{e.what()}) {}
};

class KUZU_API FatalCommitException : public Exception {
public:
    explicit FatalCommitException(const std::exception& e)
        : Exception("The commit is durable but could not be published; the database must be "
                    "restarted: " +
                    std::string{e.what()}) {}
};

} // namespace common
} // namespace kuzu
