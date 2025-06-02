#ifndef WAL_H
#define WAL_H

#include "tinyLRU.hpp"

#include <cstdint>
#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <fstream>
#include <unordered_map>
#include <functional>

#include <filesystem>
namespace fs = std::filesystem;

class WAL
{
public:
    enum class LogFormat
    {
        Binary = 0,
        JSON = 1
    };

    struct BatchEntry
    {
        uint64_t index;
        size_t size;
    };

    class Batch
    {
    public:
        void Write(uint64_t index, const std::vector<uint8_t> &data);
        void Clear();

        std::vector<BatchEntry> entries;
        std::vector<uint8_t> datas;
    };

    struct Segment
    {
        std::string path;
        uint64_t index;
        std::vector<uint8_t> ebuf;
        std::vector<std::pair<size_t, size_t>> epos; // start and end positions
    };

    struct Options
    {
        bool no_sync = false;
        size_t segment_size = 20971520; // 20MB
        LogFormat log_format = LogFormat::Binary;
        size_t segment_cache_size = 2;
        bool no_copy = false;
        uint32_t dir_perms = 0750;
        uint32_t file_perms = 0640;
    };

    static const Options DefaultOptions;

    WAL(const std::string &path, const Options &options = DefaultOptions);
    ~WAL();

    // Disallow copying
    WAL(const WAL &) = delete;
    WAL &operator=(const WAL &) = delete;

    std::vector<std::shared_ptr<Segment>> segments_;

    // Core operations
    void Write(uint64_t index, const std::vector<uint8_t> &data);
    std::vector<uint8_t> Read(uint64_t index);
    uint64_t FirstIndex();
    uint64_t LastIndex();
    void WriteBatch(Batch *batch);
    void TruncateFront(uint64_t index);
    void TruncateBack(uint64_t index);
    void Sync();
    void Close();
    void ClearCache();
    void printSegmentInfo();

private:
    void Load();
    void LoadSegmentEntries(std::shared_ptr<Segment> segment);
    int FindSegment(uint64_t index) const;
    std::shared_ptr<Segment> LoadSegment(uint64_t index);
    void CycleSegment();
    void TruncateFrontInternal(uint64_t index);
    void TruncateBackInternal(uint64_t index);
    void PushCache(int seg_idx);
    void ClearCacheInternal();

    static std::string SegmentName(uint64_t index);
    static std::pair<std::vector<uint8_t>, std::pair<size_t, size_t>>
    AppendEntry(const std::vector<uint8_t> &dst, uint64_t index,
                const std::vector<uint8_t> &data, LogFormat format);
    static std::vector<uint8_t> ReadJSON(const std::vector<uint8_t> &edata);
    static std::vector<uint8_t> ReadBinary(const std::vector<uint8_t> &edata, bool no_copy);

    mutable std::mutex mutex_;
    std::string path_;
    Options options_;
    bool closed_ = false;
    bool corrupt_ = false;

    uint64_t first_index_ = 0;
    uint64_t last_index_ = 0;
    std::unique_ptr<std::fstream> sfile_;
    Batch wbatch_;

    // Simple LRU cache implementation
    tinylru::tinyLRU<int, std::shared_ptr<Segment>> scache_;
};

#endif // WAL_H