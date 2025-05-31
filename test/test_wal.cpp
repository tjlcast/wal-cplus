#include "wal.h"
#include "utils.h"
#include <iostream>
#include <cassert>

void TestBasicOperations()
{
    std::string path = "test_wal";
    fs::remove_all(path);

    {
        WAL wal(path);

        std::cout << "WAL load path: " << path << std::endl;

        // Test empty log
        assert(wal.FirstIndex() == 0);
        assert(wal.LastIndex() == 0);

        std::cout << "wal segments size: " << wal.segments_.size() << std::endl;

        // Test writing
        wal.Write(1, {'a', 'b', 'c'});
        wal.Write(2, {'d', 'e', 'f'});
        wal.Write(3, {'g', 'h', 'i'});

        assert(wal.FirstIndex() == 1);
        assert(wal.LastIndex() == 3);
        std::cout << "Successfully wrote entries 1 to 3\n";

        // Test reading
        auto data = wal.Read(1);
        assert(data == std::vector<uint8_t>({'a', 'b', 'c'}));
        std::cout << "Read entry 1: " << std::string(data.begin(), data.end()) << "\n";

        data = wal.Read(2);
        assert(data == std::vector<uint8_t>({'d', 'e', 'f'}));
        std::cout << "Read entry 2: " << std::string(data.begin(), data.end()) << "\n";

        data = wal.Read(3);
        assert(data == std::vector<uint8_t>({'g', 'h', 'i'}));
        std::cout << "Read entry 3: " << std::string(data.begin(), data.end()) << "\n";

        // Test batch writing
        WAL::Batch batch;
        batch.Write(4, {'j', 'k', 'l'});
        batch.Write(5, {'m', 'n', 'o'});
        wal.WriteBatch(&batch);
        std::cout << "Batch written entries 4 and 5\n";

        assert(wal.FirstIndex() == 1);
        assert(wal.LastIndex() == 5);
        wal.Close();
    }

    std::cout << "WAL closed, reopening to verify persistence...\n";
    // Reopen and verify persistence
    {
        WAL wal(path);
        std::cout << "Reopened WAL at path: " << path << "first index: "
                  << wal.FirstIndex() << ", last index: " << wal.LastIndex() << "\n";
        assert(wal.FirstIndex() == 1);
        assert(wal.LastIndex() == 5);

        auto data = wal.Read(1);
        assert(data == std::vector<uint8_t>({'a', 'b', 'c'}));

        data = wal.Read(5);
        assert(data == std::vector<uint8_t>({'m', 'n', 'o'}));
    }

    fs::remove_all(path);
    std::cout << "TestBasicOperations passed\n";
}

void TestTruncations()
{
    std::cout << "Running WAL truncation tests...\n";
    std::string path = "test_wal_trunc";
    fs::remove_all(path);

    {
        WAL wal(path);

        for (uint64_t i = 1; i <= 10; i++)
        {
            wal.Write(i, {static_cast<uint8_t>('a' + i - 1)});
        }
        std::cout << "Successfully wrote entries 1 to 10\n";

        assert(wal.FirstIndex() == 1);
        assert(wal.LastIndex() == 10);
        std::cout << "First index: " << wal.FirstIndex()
                  << ", Last index: " << wal.LastIndex() << "\n";

        // Truncate front
        wal.TruncateFront(4);
        std::cout << "After truncating front, First index: "
                  << wal.FirstIndex() << ", Last index: " << wal.LastIndex() << "\n";
        assert(wal.FirstIndex() == 4);
        assert(wal.LastIndex() == 10);

        // Truncate back
        wal.TruncateBack(7);
        std::cout << "After truncating back, First index: "
                  << wal.FirstIndex() << ", Last index: " << wal.LastIndex() << "\n";
        assert(wal.FirstIndex() == 4);
        assert(wal.LastIndex() == 7);

        // Verify remaining data
        auto data = wal.Read(4);
        assert(data == std::vector<uint8_t>({'d'}));

        data = wal.Read(7);
        assert(data == std::vector<uint8_t>({'g'}));
    }

    // Reopen and verify truncations persisted
    {
        WAL wal(path);
        std::cout << "Reopened WAL at path: " << path << "first index: "
                  << wal.FirstIndex() << ", last index: " << wal.LastIndex() << "\n";
        assert(wal.FirstIndex() == 4);
        assert(wal.LastIndex() == 7);

        auto data = wal.Read(5);
        assert(data == std::vector<uint8_t>({'e'}));
    }

    fs::remove_all(path);
    std::cout << "TestTruncations passed\n";
}

void TestJSONFormat()
{
    std::cout << "Running WAL JSON format tests...\n";
    std::string path = "test_wal_json";
    fs::remove_all(path);

    WAL::Options opts;
    opts.log_format = WAL::LogFormat::JSON;

    {
        WAL wal(path, opts);

        wal.Write(1, {'a', 'b', 'c'});
        wal.Write(2, {0x80, 0x81, 0x82}); // non-UTF8 data

        assert(wal.FirstIndex() == 1);
        assert(wal.LastIndex() == 2);

        auto data = wal.Read(1);
        assert(data == std::vector<uint8_t>({'a', 'b', 'c'}));

        data = wal.Read(2);
        assert(data == std::vector<uint8_t>({0x80, 0x81, 0x82}));
    }

    fs::remove_all(path);
    std::cout << "TestJSONFormat passed\n";
}

int main()
{
    try
    {
        std::cout << "Running WAL tests...\n";
        TestBasicOperations();
        TestTruncations();
        TestJSONFormat();
        std::cout << "All tests passed\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "Test failed: " << e.what() << "\n";
        return 1;
    }
    return 0;
}