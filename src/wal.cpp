#include "wal.h"
#include "utils.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <stdexcept>
#include <system_error>

namespace fs = std::filesystem;

const WAL::Options WAL::DefaultOptions{};

WAL::WAL(const std::string &path, const Options &options)
    : path_(fs::absolute(path).string()), options_(options)
{

    if (path == ":memory:")
    {
        throw std::runtime_error("in-memory log not supported");
    }

    if (options_.segment_cache_size == 0)
    {
        options_.segment_cache_size = DefaultOptions.segment_cache_size;
    }
    if (options_.segment_size == 0)
    {
        options_.segment_size = DefaultOptions.segment_size;
    }
    if (options_.dir_perms == 0)
    {
        options_.dir_perms = DefaultOptions.dir_perms;
    }
    if (options_.file_perms == 0)
    {
        options_.file_perms = DefaultOptions.file_perms;
    }

    fs::create_directories(path_);

    Load();
}

WAL::~WAL()
{
    Close();
}

void WAL::Write(uint64_t index, const std::vector<uint8_t> &data)
{
    // std::lock_guard<std::mutex> lock(mutex_);
    if (corrupt_)
    {
        throw std::runtime_error("log corrupt");
    }
    if (closed_)
    {
        throw std::runtime_error("log closed");
    }

    wbatch_.Clear();
    wbatch_.Write(index, data);

    WriteBatch(&wbatch_);
}

std::vector<uint8_t> WAL::Read(uint64_t index)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (corrupt_)
    {
        throw std::runtime_error("log corrupt");
    }
    if (closed_)
    {
        throw std::runtime_error("log closed");
    }
    if (index == 0 || index < first_index_ || index > last_index_)
    {
        throw std::runtime_error("not found");
    }

    auto s = LoadSegment(index);
    const auto &epos = s->epos[index - s->index];
    const auto edata = std::vector<uint8_t>(
        s->ebuf.begin() + epos.first,
        s->ebuf.begin() + epos.second);

    if (options_.log_format == LogFormat::JSON)
    {
        return ReadJSON(edata);
    }
    return ReadBinary(edata, options_.no_copy);
}

uint64_t WAL::FirstIndex()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (corrupt_)
    {
        throw std::runtime_error("log corrupt");
    }
    if (closed_)
    {
        throw std::runtime_error("log closed");
    }
    if (last_index_ == 0)
    {
        return 0;
    }
    return first_index_;
}

uint64_t WAL::LastIndex()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (corrupt_)
    {
        throw std::runtime_error("log corrupt");
    }
    if (closed_)
    {
        throw std::runtime_error("log closed");
    }
    if (last_index_ == 0)
    {
        return 0;
    }
    return last_index_;
}

void WAL::TruncateFront(uint64_t index)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (corrupt_)
    {
        throw std::runtime_error("log corrupt");
    }
    if (closed_)
    {
        throw std::runtime_error("log closed");
    }
    TruncateFrontInternal(index);
    std::cout << "segments size after truncate: " << segments_.size() << std::endl;
}

void WAL::TruncateBack(uint64_t index)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (corrupt_)
    {
        throw std::runtime_error("log corrupt");
    }
    if (closed_)
    {
        throw std::runtime_error("log closed");
    }
    TruncateBackInternal(index);
    std::cout << "segments size after truncate: " << segments_.size() << std::endl;
}

// void WAL::Sync() {
//     std::lock_guard<std::mutex> lock(mutex_);
//     if (corrupt_) {
//         throw std::runtime_error("log corrupt");
//     }
//     if (closed_) {
//         throw std::runtime_error("log closed");
//     }
//     if (sfile_) {
//         sfile_->flush();
//         if (!options_.no_sync) {
//             // On Windows, we need to handle this differently
//             #ifdef _WIN32
//             HANDLE hFile = (HANDLE)_get_osfhandle(sfile_->native_handle());
//             FlushFileBuffers(hFile);
//             #else
//             fsync(sfile_->native_handle());
//             #endif
//         }
//     }
// }

// src/wal.cpp
// void WAL::Sync()
// {
//     std::lock_guard<std::mutex> lock(mutex_);
//     if (corrupt_)
//     {
//         throw std::runtime_error("log corrupt");
//     }
//     if (closed_)
//     {
//         throw std::runtime_error("log closed");
//     }
//     if (sfile_)
//     {
//         sfile_->flush();
//         if (!options_.no_sync)
//         {
//             // 更简单的跨平台同步方式
//             std::ofstream ofs;
//             ofs.open(sfile_->path(), std::ios::out | std::ios::binary);
//             if (ofs)
//             {
//                 ofs.flush();
//                 ofs.close();
//             }
//         }
//     }
// }

void WAL::Sync()
{
    // std::lock_guard<std::mutex> lock(mutex_);
    if (corrupt_)
    {
        throw std::runtime_error("log corrupt");
    }
    if (closed_)
    {
        throw std::runtime_error("log closed");
    }
    if (sfile_)
    {

        sfile_->flush();

        if (!options_.no_sync)
        {
            // 保存当前路径
            std::string current_path = segments_.back()->path;

            // 关闭并重新打开文件以确保同步
            sfile_->close();
            sfile_->open(current_path,
                         std::ios::binary | std::ios::out | std::ios::in);
            sfile_->seekp(0, std::ios::end);
        }
    }
}

void WAL::Close()
{

    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_)
    {
        if (corrupt_)
        {
            throw std::runtime_error("log corrupt");
        }
        return;
    }

    if (sfile_)
    {

        Sync();
        sfile_->close();
    }
    closed_ = true;
    if (corrupt_)
    {
        throw std::runtime_error("log corrupt");
    }
}

void WAL::ClearCache()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (corrupt_)
    {
        throw std::runtime_error("log corrupt");
    }
    if (closed_)
    {
        throw std::runtime_error("log closed");
    }
    ClearCacheInternal();
}

// Private methods
void WAL::Load()
{
    // 直接使用成员变量 segments_ 替代局部变量 segments
    segments_.clear();

    int start_idx = -1;
    int end_idx = -1;

    // 1. 扫描目录并收集段文件
    for (const auto &entry : fs::directory_iterator(path_))
    {
        if (!entry.is_regular_file())
            continue;

        std::string name = entry.path().filename().string();
        if (name.size() < 20)
            continue;

        uint64_t index;
        try
        {
            index = std::stoull(name.substr(0, 20));
        }
        catch (...)
        {
            continue;
        }

        if (index == 0)
            continue;

        bool is_start = (name.size() == 26) && (name.substr(20) == ".START");
        bool is_end = (name.size() == 24) && (name.substr(20) == ".END");

        if (name.size() == 20 || is_start || is_end)
        {
            auto seg = std::make_shared<Segment>();
            seg->index = index;
            seg->path = entry.path().string();

            if (is_start)
            {
                start_idx = segments_.size();
            }
            else if (is_end && end_idx == -1)
            {
                end_idx = segments_.size();
            }

            segments_.push_back(seg);
        }
    }

    // 2. 排序段文件
    std::sort(segments_.begin(), segments_.end(),
              [](const auto &a, const auto &b)
              { return a->index < b->index; });

    // 3. 处理空日志情况
    if (segments_.empty())
    {
        auto seg = std::make_shared<Segment>();
        seg->index = 1;
        seg->path = (fs::path(path_) / SegmentName(1)).string();
        segments_.push_back(seg);
        first_index_ = 1;
        last_index_ = 0;

        sfile_ = std::make_unique<std::fstream>(
            seg->path,
            std::ios::binary | std::ios::out | std::ios::in | std::ios::trunc);
        if (!*sfile_)
        {
            segments_.clear(); // 清理已添加的segment
            throw std::runtime_error("failed to create segment file");
        }
        return;
    }

    // 4. 处理START/END段
    try
    {
        if (start_idx != -1)
        {
            if (end_idx != -1)
            {
                throw std::runtime_error("log corrupt: both START and END segments exist");
            }

            // 删除START之前的段文件
            for (int i = 0; i < start_idx; i++)
            {
                if (fs::remove(segments_[i]->path))
                {
                    std::cerr << "Deleted segment: " << segments_[i]->path << std::endl;
                }
            }
            segments_.erase(segments_.begin(), segments_.begin() + start_idx);

            // 重命名START段
            fs::path final_path = segments_[0]->path;
            final_path.replace_extension("");
            fs::rename(segments_[0]->path, final_path);
            segments_[0]->path = final_path.string();
        }

        if (end_idx != -1)
        {
            // 删除END之后的段文件
            for (int i = segments_.size() - 1; i > end_idx; i--)
            {
                if (fs::remove(segments_[i]->path))
                {
                    std::cerr << "Deleted segment: " << segments_[i]->path << std::endl;
                }
            }
            segments_.erase(segments_.begin() + end_idx + 1, segments_.end());

            // 处理重复索引
            if (segments_.size() > 1 &&
                segments_[segments_.size() - 2]->index == segments_.back()->index)
            {
                segments_[segments_.size() - 2] = segments_.back();
                segments_.pop_back();
            }

            // 重命名END段
            fs::path final_path = segments_.back()->path;
            final_path.replace_extension("");
            fs::rename(segments_.back()->path, final_path);
            segments_.back()->path = final_path.string();
        }
    }
    catch (...)
    {
        segments_.clear();
        throw;
    }

    // 5. 初始化最后段
    first_index_ = segments_[0]->index;
    auto last_seg = segments_.back();

    sfile_ = std::make_unique<std::fstream>(
        last_seg->path,
        std::ios::binary | std::ios::out | std::ios::in);
    if (!*sfile_)
    {
        segments_.clear();
        throw std::runtime_error("failed to open segment file");
    }

    sfile_->seekp(0, std::ios::end);
    LoadSegmentEntries(last_seg);
    last_index_ = last_seg->index + last_seg->epos.size() - 1;
}

void WAL::LoadSegmentEntries(std::shared_ptr<Segment> segment)
{
    std::ifstream file(segment->path, std::ios::binary | std::ios::ate);
    if (!file)
    {
        throw std::runtime_error("failed to open segment file for reading");
    }

    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);

    segment->ebuf.resize(size);
    if (!file.read(reinterpret_cast<char *>(segment->ebuf.data()), size))
    {
        throw std::runtime_error("failed to read segment file");
    }

    segment->epos.clear();
    size_t pos = 0;
    uint64_t exidx = segment->index;

    while (pos < segment->ebuf.size())
    {
        size_t n = 0;
        if (options_.log_format == LogFormat::JSON)
        {
            // Find next newline
            auto nl_pos = std::find(
                segment->ebuf.begin() + pos,
                segment->ebuf.end(),
                '\n');
            if (nl_pos == segment->ebuf.end())
            {
                throw std::runtime_error("log corrupt");
            }
            n = std::distance(segment->ebuf.begin() + pos, nl_pos) + 1;
        }
        else
        {
            // Binary format
            uint64_t data_size;
            size_t varint_len = ReadVarint(
                segment->ebuf.data() + pos,
                segment->ebuf.size() - pos,
                &data_size);
            if (varint_len == 0)
            {
                throw std::runtime_error("log corrupt");
            }
            if (segment->ebuf.size() - pos - varint_len < data_size)
            {
                throw std::runtime_error("log corrupt");
            }
            n = varint_len + data_size;
        }

        segment->epos.emplace_back(pos, pos + n);
        pos += n;
        exidx++;
    }
    std::cout << "LoadSegmentEntries" << std::endl;
    for (const auto &entry : segment->epos)
    {
        std::cout << "(" << entry.first << ", " << entry.second << "), ";
    }
    std::cout << "]" << std::endl;
}

// int WAL::FindSegment(uint64_t index) const
// {
//     int low = 0;
//     int high = segments_.size() - 1;
//     int result = -1;

//     while (low <= high)
//     {
//         std::cout << "low: " << low << ", high: " << high << std::endl;
//         int mid = low + (high - low) / 2;
//         if (index >= segments_[mid]->index)
//         {
//             result = mid;
//             low = mid + 1;
//         }
//         else
//         {
//             high = mid - 1;
//         }
//     }

//     return result;
// }

int WAL::FindSegment(uint64_t index) const
{
    int low = 0;
    int high = segments_.size();
    std::cout << "Finding segment for index: " << index << " low: " << low << " high: " << high << std::endl;

    while (low < high)
    {
        int mid = low + (high - low) / 2;
        std::cout << "low: " << low << ", high: " << high << ", index: " << segments_[mid]->index << std::endl;
        if (index >= segments_[mid]->index)
        {
            low = mid + 1;
        }
        else
        {
            high = mid;
        }
    }

    return low - 1; // 返回最后一个满足条件的索引
}

std::shared_ptr<WAL::Segment> WAL::LoadSegment(uint64_t index)
{
    // Check last segment first
    auto last_seg = segments_.back();
    if (index >= last_seg->index)
    {
        return last_seg;
    }

    // Check cache
    for (const auto &[seg_idx, seg] : scache_)
    {
        if (index >= seg->index && index < seg->index + seg->epos.size())
        {
            return seg;
        }
    }

    // Find in segments
    int seg_idx = FindSegment(index);
    if (seg_idx == -1)
    {
        throw std::runtime_error("segment not found");
    }

    auto seg = segments_[seg_idx];
    if (seg->epos.empty())
    {
        LoadSegmentEntries(seg);
    }

    // Update cache
    PushCache(seg_idx);
    return seg;
}

void WAL::CycleSegment()
{
    if (!sfile_)
    {
        throw std::runtime_error("no active segment file");
    }

    sfile_->flush();
    if (!options_.no_sync)
    {
        Sync();
    }
    sfile_->close();

    // Cache the previous segment
    PushCache(segments_.size() - 1);

    auto new_seg = std::make_shared<Segment>();
    new_seg->index = last_index_ + 1;
    new_seg->path = (fs::path(path_) / SegmentName(new_seg->index)).string();

    sfile_ = std::make_unique<std::fstream>(
        new_seg->path,
        std::ios::binary | std::ios::out | std::ios::in | std::ios::trunc);
    if (!*sfile_)
    {
        throw std::runtime_error("failed to create new segment file");
    }

    segments_.push_back(new_seg);
}

void WAL::WriteBatch(Batch *batch)
{
    if (batch->entries.empty())
    {
        return;
    }

    // Check indexes are sequential
    for (size_t i = 0; i < batch->entries.size(); i++)
    {
        if (batch->entries[i].index != last_index_ + i + 1)
        {
            throw std::runtime_error("out of order");
        }
    }
    auto seg = segments_.back();

    if (seg->ebuf.size() > options_.segment_size)
    {
        CycleSegment();
        seg = segments_.back();
    }

    size_t data_pos = 0;
    size_t mark = seg->ebuf.size();

    for (size_t i = 0; i < batch->entries.size(); i++)
    {
        const auto &entry = batch->entries[i];
        std::vector<uint8_t> data(
            batch->datas.begin() + data_pos,
            batch->datas.begin() + data_pos + entry.size);

        auto [new_ebuf, epos] = AppendEntry(
            seg->ebuf, entry.index, data, options_.log_format);
        seg->ebuf = new_ebuf;
        seg->epos.push_back(epos);

        if (seg->ebuf.size() >= options_.segment_size)
        {
            // Write current content and cycle
            if (!sfile_->write(
                    reinterpret_cast<const char *>(seg->ebuf.data() + mark),
                    seg->ebuf.size() - mark))
            {
                throw std::runtime_error("failed to write to segment file");
            }

            last_index_ = entry.index;
            CycleSegment();
            seg = segments_.back();
            mark = 0;
        }

        data_pos += entry.size;
    }

    if (seg->ebuf.size() - mark > 0)
    {
        if (!sfile_->write(
                reinterpret_cast<const char *>(seg->ebuf.data() + mark),
                seg->ebuf.size() - mark))
        {
            throw std::runtime_error("failed to write to segment file");
        }
        last_index_ = batch->entries.back().index;
    }

    if (!options_.no_sync)
    {
        Sync();
    }

    batch->Clear();
}

void WAL::TruncateFrontInternal(uint64_t index)
{
    if (index == 0 || last_index_ == 0 || index < first_index_ || index > last_index_)
    {
        throw std::runtime_error("out of range");
    }
    if (index == first_index_)
    {
        return;
    }

    int seg_idx = FindSegment(index);
    auto seg = LoadSegment(index);
    auto epos = std::vector<std::pair<size_t, size_t>>(
        seg->epos.begin() + (index - seg->index),
        seg->epos.end());
    std::cout << "epos size: " << epos.size() << std::endl;

    std::vector<uint8_t> ebuf(
        seg->ebuf.begin() + epos[0].first,
        seg->ebuf.end());

    // Create temp file
    fs::path temp_path = fs::path(path_) / "TEMP";
    {
        std::ofstream temp_file(temp_path, std::ios::binary | std::ios::trunc);
        if (!temp_file.write(reinterpret_cast<const char *>(ebuf.data()), ebuf.size()))
        {
            throw std::runtime_error("failed to write temp file");
        }
        temp_file.close();
    }

    // Rename to START file
    fs::path start_path = fs::path(path_) / (SegmentName(index) + ".START");
    fs::rename(temp_path, start_path);

    try
    {
        if (seg_idx == static_cast<int>(segments_.size()) - 1)
        {
            // Close tail segment
            sfile_->close();
        }

        // Delete truncated segments
        for (int i = 0; i <= seg_idx; i++)
        {
            fs::remove(segments_[i]->path);
        }

        // Rename START to final name
        fs::path new_path = fs::path(path_) / SegmentName(index);
        fs::rename(start_path, new_path);

        seg->path = new_path.string();
        seg->index = index;

        if (seg_idx == static_cast<int>(segments_.size()) - 1)
        {
            // Reopen tail segment
            sfile_ = std::make_unique<std::fstream>(
                new_path,
                std::ios::binary | std::ios::out | std::ios::in);
            if (!*sfile_)
            {
                throw std::runtime_error("failed to reopen segment file");
            }

            sfile_->seekp(0, std::ios::end);
            if (sfile_->tellp() != static_cast<std::streampos>(ebuf.size()))
            {
                throw std::runtime_error("invalid seek");
            }

            LoadSegmentEntries(seg);
        }

        std::cout << "Before truncation, segments size: " << segments_.size() << std::endl;
        // segments_.erase(segments_.begin(), segments_.begin() + seg_idx + 1);

        // 创建新 vector，包含 segments_[seg_idx] 到末尾的元素
        std::vector<std::shared_ptr<Segment>> new_segments(
            segments_.begin() + seg_idx,
            segments_.end());
        segments_ = std::move(new_segments); // 替换原 segments_

        // // 删除旧 Segment
        // if (seg_idx + 1 < segments_.size())
        // {
        //     segments_.erase(segments_.begin(), segments_.begin() + seg_idx + 1);
        // }
        // else
        // {
        //     // 处理删除全部 Segment 的情况
        //     segments_.clear();
        //     segments_.push_back(std::make_shared<Segment>());
        //     segments_[0]->path = new_path.string(); // 新文件路径
        //     segments_[0]->index = index;
        //     segments_[0]->epos = std::move(epos);
        //     segments_[0]->ebuf = std::move(ebuf);
        // }

        // 更新 first_index_
        first_index_ = index;
        ClearCacheInternal();
    }
    catch (...)
    {
        corrupt_ = true;
        throw std::runtime_error("log corrupt");
    }
}

void WAL::TruncateBackInternal(uint64_t index)
{
    std::cout << "Truncating back to index: " << index << std::endl;
    if (index == 0 || last_index_ == 0 || index < first_index_ || index > last_index_)
    {
        throw std::runtime_error("out of range");
    }
    if (index == last_index_)
    {
        return;
    }

    int seg_idx = FindSegment(index);
    std::cout << "Segment index found: " << seg_idx << std::endl;
    auto seg = LoadSegment(index);
    std::cout << "Loaded segment: " << seg->path << std::endl;
    auto epos = std::vector<std::pair<size_t, size_t>>(
        seg->epos.begin(),
        seg->epos.begin() + (index - seg->index + 1));
    std::cout << "epos content: [";
    for (const auto &entry : epos)
    {
        std::cout << "(" << entry.first << ", " << entry.second << "), ";
    }
    std::cout << "]" << std::endl;

    std::cout << ">>>>>>>>>>>>>>>>>>>>> seg->index: " << seg->index << std::endl;
    std::cout << "index - seg->index + 1: " << index - seg->index + 1 << std::endl;

    std::vector<uint8_t> ebuf(
        seg->ebuf.begin(),
        seg->ebuf.begin() + epos.back().second);

    std::string str(ebuf.begin(), ebuf.end());
    std::cout << "ebuf Content: " << str << std::endl; // 输出: Hello

    // Create temp file
    std::cout << "Creating temp file for truncation..." << std::endl;
    fs::path temp_path = fs::path(path_) / "TEMP";
    {
        std::ofstream temp_file(temp_path, std::ios::binary | std::ios::trunc);
        if (!temp_file.write(reinterpret_cast<const char *>(ebuf.data()), ebuf.size()))
        {
            throw std::runtime_error("failed to write temp file");
        }

        temp_file.close();
    }

    // Rename to END file
    fs::path end_path = fs::path(path_) / (SegmentName(seg->index) + ".END");
    fs::rename(temp_path, end_path);

    try
    {
        // Close tail segment
        sfile_->close();

        // Delete truncated segments
        for (int i = seg_idx; i < static_cast<int>(segments_.size()); i++)
        {
            fs::remove(segments_[i]->path);
        }

        // Rename END to final name
        fs::path new_path = fs::path(path_) / SegmentName(seg->index);
        fs::rename(end_path, new_path);

        // Reopen tail segment
        sfile_ = std::make_unique<std::fstream>(
            new_path,
            std::ios::binary | std::ios::out | std::ios::in);
        if (!*sfile_)
        {
            throw std::runtime_error("failed to reopen segment file");
        }

        sfile_->seekp(0, std::ios::end);
        if (sfile_->tellp() != static_cast<std::streampos>(ebuf.size()))
        {
            throw std::runtime_error("invalid seek");
        }

        seg->path = new_path.string();
        // segments_.erase(segments_.begin() + seg_idx + 1, segments_.end());

        // 创建新 vector，包含 segments_[0] 到 segments_[seg_idx] 的元素
        std::vector<std::shared_ptr<Segment>> new_segments(
            segments_.begin(),
            segments_.begin() + seg_idx + 1);
        segments_ = std::move(new_segments); // 替换原 segments_

        // 向后删除：移除 seg_idx + 1 之后的所有 Segments
        // if (seg_idx + 1 < segments_.size())
        // {
        //     segments_.erase(segments_.begin() + seg_idx + 1, segments_.end());
        // }
        // else
        // {
        //     // 处理 seg_idx 指向最后一个 Segment 的情况（无需删除）
        //     // 可选：更新当前 Segment 的元数据（如需要）
        //     segments_.back()->epos = std::move(epos);
        //     segments_.back()->ebuf = std::move(ebuf);
        //     segments_.back()->index = index; // 如果 index 变化
        // }

        last_index_ = index;
        ClearCacheInternal();
        LoadSegmentEntries(seg);
    }
    catch (...)
    {
        corrupt_ = true;
        throw std::runtime_error("log corrupt");
    }
}

void WAL::PushCache(int seg_idx)
{
    if (seg_idx < 0 || seg_idx >= static_cast<int>(segments_.size()))
    {
        return;
    }

    // Simple LRU cache implementation
    if (scache_.size() >= options_.segment_cache_size)
    {
        // Find least recently used
        int lru_idx = lru_order_.front();
        scache_.erase(lru_idx);
        lru_order_.erase(lru_order_.begin());
    }

    scache_[seg_idx] = segments_[seg_idx];
    lru_order_.push_back(seg_idx);
}

void WAL::ClearCacheInternal()
{
    scache_.clear();
    lru_order_.clear();
}

std::string WAL::SegmentName(uint64_t index)
{
    std::ostringstream oss;
    oss << std::setw(20) << std::setfill('0') << index;
    return oss.str();
}

std::pair<std::vector<uint8_t>, std::pair<size_t, size_t>>
WAL::AppendEntry(const std::vector<uint8_t> &dst, uint64_t index,
                 const std::vector<uint8_t> &data, LogFormat format)
{
    std::vector<uint8_t> out = dst;
    size_t pos = out.size();

    if (format == LogFormat::JSON)
    {
        // {"index":"number","data":"base64encoded"}
        std::string json = "{\"index\":\"" + std::to_string(index) + "\",\"data\":\"";

        // Check if data is valid UTF-8
        bool is_utf8 = true;
        const char *str = reinterpret_cast<const char *>(data.data());
        size_t len = data.size();
        for (size_t i = 0; i < len;)
        {
            unsigned char c = str[i];
            if (c <= 0x7F)
            {
                i++;
            }
            else if ((c & 0xE0) == 0xC0)
            {
                if (i + 1 >= len || (str[i + 1] & 0xC0) != 0x80)
                {
                    is_utf8 = false;
                    break;
                }
                i += 2;
            }
            else if ((c & 0xF0) == 0xE0)
            {
                if (i + 2 >= len || (str[i + 1] & 0xC0) != 0x80 ||
                    (str[i + 2] & 0xC0) != 0x80)
                {
                    is_utf8 = false;
                    break;
                }
                i += 3;
            }
            else if ((c & 0xF8) == 0xF0)
            {
                if (i + 3 >= len || (str[i + 1] & 0xC0) != 0x80 ||
                    (str[i + 2] & 0xC0) != 0x80 ||
                    (str[i + 3] & 0xC0) != 0x80)
                {
                    is_utf8 = false;
                    break;
                }
                i += 4;
            }
            else
            {
                is_utf8 = false;
                break;
            }
        }

        if (is_utf8)
        {
            json += "+";
            json.append(str, len);
        }
        else
        {
            json += "$";
            json += base64_encode(data.data(), data.size(), false);
        }
        json += "\"}\n";

        out.insert(out.end(), json.begin(), json.end());
    }
    else
    {
        // Binary format: varint length + data
        std::vector<uint8_t> varint;
        WriteVarint(data.size(), varint);
        out.insert(out.end(), varint.begin(), varint.end());
        out.insert(out.end(), data.begin(), data.end());
    }

    return {out, {pos, out.size()}};
}

std::vector<uint8_t> WAL::ReadJSON(const std::vector<uint8_t> &edata)
{
    try
    {
        std::string json_str(edata.begin(), edata.end());
        size_t data_pos = json_str.find("\"data\":\"");
        if (data_pos == std::string::npos)
        {
            throw std::runtime_error("invalid JSON format");
        }
        data_pos += 8; // length of "\"data\":\""

        if (data_pos >= json_str.size())
        {
            throw std::runtime_error("invalid JSON format");
        }

        char prefix = json_str[data_pos];
        if (prefix != '+' && prefix != '$')
        {
            throw std::runtime_error("invalid data prefix");
        }

        size_t end_pos = json_str.find('"', data_pos + 1);
        if (end_pos == std::string::npos)
        {
            throw std::runtime_error("invalid JSON format");
        }

        std::string data_str = json_str.substr(data_pos + 1, end_pos - data_pos - 1);
        if (prefix == '+')
        {
            return std::vector<uint8_t>(data_str.begin(), data_str.end());
        }
        else
        {
            return base64_decode(data_str);
        }
    }
    catch (...)
    {
        throw std::runtime_error("log corrupt");
    }
}

std::vector<uint8_t> WAL::ReadBinary(const std::vector<uint8_t> &edata, bool no_copy)
{
    uint64_t size;
    size_t n = ReadVarint(edata.data(), edata.size(), &size);
    if (n == 0 || edata.size() - n < size)
    {
        throw std::runtime_error("log corrupt");
    }

    if (no_copy)
    {
        return std::vector<uint8_t>(
            edata.begin() + n,
            edata.begin() + n + size);
    }
    else
    {
        std::vector<uint8_t> data(size);
        std::copy(
            edata.begin() + n,
            edata.begin() + n + size,
            data.begin());
        return data;
    }
}

void WAL::Batch::Write(uint64_t index, const std::vector<uint8_t> &data)
{
    entries.push_back({index, data.size()});
    datas.insert(datas.end(), data.begin(), data.end());
}

void WAL::Batch::Clear()
{
    entries.clear();
    datas.clear();
}