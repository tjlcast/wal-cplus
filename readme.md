## Usage

### Json Example
``` c++
void TestStringWithJSONFormat()
{
    std::cout << "Running WAL string JSON format tests...\n";
    std::string path = "test_wal_string_json";
    fs::remove_all(path);

    WAL::Options opts;
    opts.log_format = WAL::LogFormat::JSON;

    {
        WAL wal(path, opts);

        // 准备测试字符串
        std::string test_str1 = "Hello, WAL!";
        std::string test_str2 = "你好，世界！";  // 包含非ASCII字符

        // 写入字符串
        wal.Write(1, std::vector<uint8_t>(test_str1.begin(), test_str1.end()));
        wal.Write(2, std::vector<uint8_t>(test_str2.begin(), test_str2.end()));

        assert(wal.FirstIndex() == 1);
        assert(wal.LastIndex() == 2);

        // 读取并验证字符串
        auto data1 = wal.Read(1);
        std::string read_str1(data1.begin(), data1.end());
        assert(read_str1 == test_str1);

        auto data2 = wal.Read(2);
        std::string read_str2(data2.begin(), data2.end());
        assert(read_str2 == test_str2);

        std::cout << "Read string1: " << read_str1 << "\n";
        std::cout << "Read string2: " << read_str2 << "\n";
    }

    fs::remove_all(path);
    std::cout << "TestStringWithJSONFormat passed\n";
}
```

### build
``` bash
make clean; 
make -j$(nproc) # or make -j16
```


### test
Follow `build`, you can run
``` bash
make test
```