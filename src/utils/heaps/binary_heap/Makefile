# Debug option (uncomment to enable)
#DEBUG=-DDEBUG

# Variables for Catch2
CATCH2_INCLUDE := -I../../../../external/catch2
CATCH2_BINARY  := ../../../../external/catch2/catch_amalgamated.o

# Compiler and flags
CXX := g++
CXXFLAGS := -g -std=c++14 -pthread $(DEBUG) -I. $(CATCH2_INCLUDE)

# Sources and objects
HEAP_SRC   := heap.cpp
TEST_SRC   := tests/test_heap.cpp tests/test_heap_utils.cpp tests/test_heap_multithreaded.cpp
OBJS       := heap.o tests/test_heap.o tests/test_heap_utils.o tests/test_heap_multithreaded.o

# The output binaries
TARGETS := test_heap test_heap_multithreaded

# Common test flags
TEST_FLAGS := --reporter console --durations yes

# Default target: build everything
all: $(TARGETS)

# Build the single-threaded test binary
test_heap: heap.o tests/test_heap.o tests/test_heap_utils.o $(CATCH2_BINARY)
	$(CXX) $(CXXFLAGS) heap.o tests/test_heap.o tests/test_heap_utils.o $(CATCH2_BINARY) -o test_heap

# Build the multithreaded test binary
test_heap_multithreaded: heap.o tests/test_heap_multithreaded.o tests/test_heap_utils.o $(CATCH2_BINARY)
	$(CXX) $(CXXFLAGS) heap.o tests/test_heap_multithreaded.o tests/test_heap_utils.o $(CATCH2_BINARY) -o test_heap_multithreaded

# Compile individual source files into object files
heap.o: heap.cpp heap.hpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

tests/test_heap.o: tests/test_heap.cpp heap.hpp tests/test_heap_utils.hpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

tests/test_heap_utils.o: tests/test_heap_utils.cpp tests/test_heap_utils.hpp heap.hpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

tests/test_heap_multithreaded.o: tests/test_heap_multithreaded.cpp heap.hpp tests/test_heap_utils.hpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Run single-threaded tests
run-st: test_heap
	@./test_heap $(TEST_FLAGS)

# Run multi-threaded tests
run-mt: test_heap_multithreaded
	@./test_heap_multithreaded $(TEST_FLAGS)

# Run all tests (both single-threaded and multi-threaded)
run: test_heap test_heap_multithreaded
	@echo "Running single-threaded tests..."
	@./test_heap $(TEST_FLAGS)
	@echo ""
	@echo "Running multi-threaded tests..."
	@./test_heap_multithreaded $(TEST_FLAGS)

# Clean up generated files
clean:
	rm -f heap.o tests/test_heap.o tests/test_heap_utils.o tests/test_heap_multithreaded.o $(TARGETS)
