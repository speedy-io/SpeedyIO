CXX=g++
CC=gcc

#Check gcc/g++ version. Error otherwise
REQUIRED_GCC_VERSION := 11
REQUIRED_GPP_VERSION := 11

# detect host architecture and pick CPU flags
ARCH := $(shell uname -m)
ifeq ($(ARCH),aarch64)
  # ARCH_FLAGS := -march=armv8-a           # safe default for Graviton; or use -mcpu=neoverse-v2 for Graviton4
  ARCH_FLAGS := -mcpu=neoverse-v2          # safe default for Graviton; or use -mcpu=neoverse-v2 for Graviton4
else
  ARCH_FLAGS := -march=native
endif

# Add -latomic for AArch64 (if required by atomics)
ifeq ($(ARCH),aarch64)
  LIBS += -latomic
endif

# Output directory
LIB_DIR := lib
TARGET  := $(LIB_DIR)/lib_speedyio_release.so

LIBS=-lpthread -lrt -ldl -lm
FLAGS=-fPIC -shared -std=c++14 -O3 $(ARCH_FLAGS)

INCLUDE=-I. -I./utils -I./src

BOOK_KEEPING=-DMAINTAIN_INODE -DPER_FD_DS -DPER_THREAD_DS
SYSTEM_INFO=-DENABLE_SYSTEM_INFO
EVICTION_FLAGS_LRU=-DENABLE_EVICTION -DEVICTION_LRU -DENABLE_PVT_HEAP -DENABLE_POSIX_FADV_RANDOM_FOR_WHITELISTED_FILES

# SOURCES = \
#     interface.cpp \
#     inode.cpp \
#     prefetch_evict.cpp \
# 	utils/bitmap/bitmap.c \
# 	utils/filename_helper/filename_helper.cpp \
# 	utils/hashtable/hashtable.c \
# 	utils/heaps/binary_heap/heap.cpp \
# 	utils/latency_tracking/latency_tracking.cpp \
# 	utils/parse_config/get_config.cpp \
# 	utils/r_w_lock/readers_writers_lock.cpp \
#     utils/shim/shim.cpp \
# 	utils/start_stop/start_stop_speedyio.cpp \
# 	utils/system_info/system_info.cpp \
# 	utils/thpool/simple/thpool-simple.c \
# 	utils/thpool/simple/fsck_lock.c \
# 	utils/trigger/trigger.cpp \
#     utils/whitelist/whitelist.cpp \
#     utils/events_logger/events_logger.cpp

SRC_DIR := src

VPATH := $(SRC_DIR) \
         $(SRC_DIR)/utils/bitmap \
         $(SRC_DIR)/utils/filename_helper \
         $(SRC_DIR)/utils/hashtable \
         $(SRC_DIR)/utils/heaps/binary_heap \
         $(SRC_DIR)/utils/latency_tracking \
         $(SRC_DIR)/utils/parse_config \
         $(SRC_DIR)/utils/r_w_lock \
         $(SRC_DIR)/utils/shim \
         $(SRC_DIR)/utils/start_stop \
         $(SRC_DIR)/utils/system_info \
         $(SRC_DIR)/utils/thpool/simple \
         $(SRC_DIR)/utils/trigger \
         $(SRC_DIR)/utils/whitelist \
         $(SRC_DIR)/utils/events_logger


all: SPEEDYIO_RELEASE 

SPEEDYIO_RELEASE: check_gcc_version $(LIB_DIR) $(TARGET)

$(LIB_DIR):
	mkdir -p $(LIB_DIR)

$(TARGET): $(SOURCES)
	$(CXX) $(INCLUDE) $(FLAGS) -o $@ $^ \
	$(LIBS) -DGHEAP_TRIGGER -DNOSYNC_BEFORE_RANGE_EVICT \
	-DEVICTOR_OUTSIDE_LOCK $(BOOK_KEEPING) $(SYSTEM_INFO) \
	$(EVICTION_FLAGS_LRU) -DSET_PVT_MIN_IN_GHEAP \
	-DENABLE_START_STOP -DENABLE_FADV_DONT_NEED -DENABLE_SEQ_ON_DONTNEED


clean:
	sudo rm -f /usr/lib/lib_speedyio_*.so
	rm -rf $(LIB_DIR)

check_gcc_version:
	@if ! command -v gcc >/dev/null 2>&1; then \
		echo "Error: gcc not found."; exit 1; \
	fi
	@if ! command -v g++ >/dev/null 2>&1; then \
		echo "Error: g++ not found."; exit 1; \
	fi
	@GCC_VERSION=$$(gcc -dumpversion | cut -d. -f1); \
	if [ "$$GCC_VERSION" -lt "$(REQUIRED_GCC_VERSION)" ]; then \
		echo "Error: GCC version must be >= $(REQUIRED_GCC_VERSION). Found: $$GCC_VERSION"; \
		exit 1; \
	fi
	@GPP_VERSION=$$(g++ -dumpversion | cut -d. -f1); \
	if [ "$$GPP_VERSION" -lt "$(REQUIRED_GPP_VERSION)" ]; then \
		echo "Error: G++ version must be >= $(REQUIRED_GPP_VERSION). Found: $$GPP_VERSION"; \
		exit 1; \
	fi