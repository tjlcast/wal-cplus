CXX := g++
# 修改点1：添加第三方头文件路径
CXXFLAGS := -std=c++17 -fsanitize=address -Wall -Wextra -Iinclude -Ithird_party/tinyLRU-cplus -O2 -fPIC

SRC_DIR := src
TEST_DIR := test
BUILD_DIR := build
LIB_DIR := lib
THIRD_PARTY_DIR := third_party/tinyLRU-cplus

# 安装目录配置
PREFIX ?= /usr/local
INCLUDE_DIR ?= $(PREFIX)/include/wal
LIB_INSTALL_DIR ?= $(PREFIX)/lib

SRCS := $(wildcard $(SRC_DIR)/*.cpp)
OBJS := $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SRCS))

TEST_SRCS := $(wildcard $(TEST_DIR)/*.cpp)
TEST_OBJS := $(patsubst $(TEST_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(TEST_SRCS))

LIB_NAME := libwal.a
TARGET := $(BUILD_DIR)/wal_test

all: $(LIB_NAME) $(TARGET)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(LIB_DIR):
	mkdir -p $(LIB_DIR)

# 修改点2：确保第三方目录存在
$(THIRD_PARTY_DIR):
	mkdir -p $(THIRD_PARTY_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR)/%.o: $(TEST_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(LIB_NAME): $(OBJS) | $(LIB_DIR)
	ar rcs $(LIB_DIR)/$@ $^

$(TARGET): $(TEST_OBJS) $(LIB_NAME)
	$(CXX) $(CXXFLAGS) $(TEST_OBJS) -L$(LIB_DIR) -lwal -o $@ $(LDFLAGS)

clean:
	rm -rf $(BUILD_DIR) $(LIB_DIR)

test: $(TARGET)
	./$(TARGET)

install: $(LIB_NAME)
	@echo "Installing to $(PREFIX)"
	install -d $(DESTDIR)$(INCLUDE_DIR)
	install -d $(DESTDIR)$(LIB_INSTALL_DIR)
	install -m 644 include/*.h $(DESTDIR)$(INCLUDE_DIR)
	install -m 644 $(LIB_DIR)/$(LIB_NAME) $(DESTDIR)$(LIB_INSTALL_DIR)
	# 可选：安装第三方头文件
	install -d $(DESTDIR)$(INCLUDE_DIR)/third_party
	install -m 644 $(THIRD_PARTY_DIR)/include/*.hpp $(DESTDIR)$(INCLUDE_DIR)/third_party/

uninstall:
	@echo "Removing from $(PREFIX)"
	rm -rf $(DESTDIR)$(INCLUDE_DIR)
	rm -f $(DESTDIR)$(LIB_INSTALL_DIR)/$(LIB_NAME)

.PHONY: all clean test install uninstall