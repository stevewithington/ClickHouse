#pragma once

#include <Disks/IDisk.h>
#include <Disks/IObjectStorage.h>

namespace DB
{

class DiskObjectStorage : public IDisk
{
public:
    DiskObjectStorage(
        const String & name_, const String & remote_fs_root_path_,
        const String & log_name, DiskPtr metadata_disk_,
        ObjectStoragePtr && object_storage_)
        : name(name_)
        , remote_fs_root_path(remote_fs_root_path_)
        , log (&Poco::Logger::get(log_name))
        , metadata_disk(metadata_disk_)
        , object_storage(std::move(object_storage_))
    {}

    struct Metadata;
    using MetadataUpdater = std::function<bool(Metadata & metadata)>;

    const String & getName() const final override { return name; }

    const String & getPath() const final override { return metadata_disk->getPath(); }

    std::vector<String> getRemotePaths(const String & local_path) const final override;

    void getRemotePathsRecursive(const String & local_path, std::vector<LocalPathWithRemotePaths> & paths_map) override;

    /// Methods for working with metadata. For some operations (like hardlink
    /// creation) metadata can be updated concurrently from multiple threads
    /// (file actually rewritten on disk). So additional RW lock is required for
    /// metadata read and write, but not for create new metadata.
    Metadata readMetadata(const String & path) const;
    Metadata readMetadataUnlocked(const String & path, std::shared_lock<std::shared_mutex> &) const;
    Metadata readUpdateAndStoreMetadata(const String & path, bool sync, MetadataUpdater updater);
    Metadata readUpdateStoreMetadataAndRemove(const String & path, bool sync, MetadataUpdater updater);

    Metadata readOrCreateUpdateAndStoreMetadata(const String & path, WriteMode mode, bool sync, MetadataUpdater updater);

    Metadata createAndStoreMetadata(const String & path, bool sync);
    Metadata createUpdateAndStoreMetadata(const String & path, bool sync, MetadataUpdater updater);

    UInt64 getTotalSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getUnreservedSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getKeepingFreeSpace() const override { return 0; }

    bool exists(const String & path) const override;


    bool isFile(const String & path) const override;

    void createFile(const String & path) override;

    size_t getFileSize(const String & path) const override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void removeFile(const String & path) override { removeSharedFile(path, false); }

    void removeFileIfExists(const String & path) override { removeSharedFileIfExists(path, false); }

    void removeRecursive(const String & path) override { removeSharedRecursive(path, false, {}); }

    void removeSharedFile(const String & path, bool delete_metadata_only) override;

    void removeFromRemoteFS(const std::vector<String> & paths);

    DiskPtr getMetadataDiskIfExistsOrSelf() override { return metadata_disk; }

    UInt32 getRefCount(const String & path) const override;

    /// Return metadata for each file path. Also, before serialization reset
    /// ref_count for each metadata to zero. This function used only for remote
    /// fetches/sends in replicated engines. That's why we reset ref_count to zero.
    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    String getUniqueId(const String & path) const override;

    /// TODO Check object exists
    bool checkUniqueId(const String & id) const override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    void listFiles(const String & path, std::vector<String> & file_names) override;

    void setReadOnly(const String & path) override;

    bool isDirectory(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override { moveFile(from_path, to_path); }

    void removeDirectory(const String & path) override;

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) override;

    bool isRemote() const override { return true; }

    bool supportZeroCopyReplication() const override { return true; }

    bool supportParallelWrite() const override { return true; }

    void shutdown() override;

    void startup() override;

private:
    const String name;
    const String remote_fs_root_path;
    Poco::Logger * log;
    DiskPtr metadata_disk;
    ObjectStoragePtr object_storage;

    mutable std::shared_mutex metadata_mutex;
    void removeMetadata(const String & path, std::vector<String> & paths_to_remove);

    void removeMetadataRecursive(const String & path, std::unordered_map<String, std::vector<String>> & paths_to_remove);

};

struct DiskObjectStorage::Metadata
{
    using Updater = std::function<bool(DiskObjectStorage::Metadata & metadata)>;
    /// Metadata file version.
    static constexpr UInt32 VERSION_ABSOLUTE_PATHS = 1;
    static constexpr UInt32 VERSION_RELATIVE_PATHS = 2;
    static constexpr UInt32 VERSION_READ_ONLY_FLAG = 3;

    /// Remote FS objects paths and their sizes.
    std::vector<BlobPathWithSize> remote_fs_objects;

    /// URI
    const String & remote_fs_root_path;

    /// Relative path to metadata file on local FS.
    const String metadata_file_path;

    DiskPtr metadata_disk;

    /// Total size of all remote FS (S3, HDFS) objects.
    size_t total_size = 0;

    /// Number of references (hardlinks) to this metadata file.
    ///
    /// FIXME: Why we are tracking it explicetly, without
    /// info from filesystem????
    UInt32 ref_count = 0;

    /// Flag indicates that file is read only.
    bool read_only = false;

    Metadata(
        const String & remote_fs_root_path_,
        DiskPtr metadata_disk_,
        const String & metadata_file_path_);

    void addObject(const String & path, size_t size);

    static Metadata readMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_);
    static Metadata readUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, Updater updater);
    static Metadata readUpdateStoreMetadataAndRemove(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, Updater updater);

    static Metadata createAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync);
    static Metadata createUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, Updater updater);
    static Metadata createAndStoreMetadataIfNotExists(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, bool overwrite);

    /// Serialize metadata to string (very same with saveToBuffer)
    std::string serializeToString();

private:
    /// Fsync metadata file if 'sync' flag is set.
    void save(bool sync = false);
    void saveToBuffer(WriteBuffer & buffer, bool sync);
    void load();


};


}
