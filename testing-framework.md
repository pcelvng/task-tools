# Testing Framework Documentation

This document outlines the testing patterns and frameworks used in the task-tools repository, focusing on unit test patterns, the hydronica/trial framework, and conversion strategies for testify-based tests.

## Overview

The repository uses a consistent testing approach with the following key components:

1. **hydronica/trial** - Primary testing framework for table-driven tests
2. **Go's built-in testing** - Standard Go testing patterns
3. **testify/assert** - Legacy testing assertions (to be converted)
4. **Example functions** - Documentation-style tests

## Testing Patterns

### 1. hydronica/trial Framework

The `hydronica/trial` framework is the primary testing tool used throughout the repository. It provides a clean, type-safe way to write table-driven tests.

#### Basic Pattern

```go
func TestFunctionName(t *testing.T) {
    fn := func(input InputType) (OutputType, error) {
        // Test logic here
        return result, err
    }
    
    cases := trial.Cases[InputType, OutputType]{
        "test case name": {
            Input:    inputValue,
            Expected: expectedValue,
        },
        "error case": {
            Input:     errorInput,
            ShouldErr: true,
        },
    }
    
    trial.New(fn, cases).Test(t)
}
```

#### Advanced Features

**Comparers**: Custom comparison logic for complex types
```go
trial.New(fn, cases).Comparer(trial.Contains).Test(t)
trial.New(fn, cases).Comparer(trial.EqualOpt(trial.IgnoreAllUnexported)).Test(t)
```

**SubTests**: For complex test scenarios
```go
trial.New(fn, cases).SubTest(t)
```

**Timeouts**: For tests that might hang
```go
trial.New(fn, cases).Timeout(time.Second).SubTest(t)
```

#### Time Handling

The repository uses trial's time utilities instead of literal `time.Date()` calls:

```go
// Preferred (using trial utilities)
trial.TimeDay("2023-01-01")
trial.TimeHour("2023-01-01T12")
trial.Time(time.RFC3339, "2023-01-01T00:00:00Z")

// Avoid (literal time.Date calls)
time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
```

### 2. Standard Go Testing

For simple tests that don't require table-driven patterns:

```go
func TestSimpleFunction(t *testing.T) {
    result := functionUnderTest()
    if result != expected {
        t.Errorf("got %v, expected %v", result, expected)
    }
}
```

### 3. Example Functions

Used for documentation and demonstrating API usage:

```go
func ExampleFunctionName() {
    // Example code
    fmt.Println("output")
    
    // Output:
    // output
}
```

Example functions are found in files like `file/writebyhour_test.go` and serve as both tests and documentation.

### 4. TestMain Pattern

For setup and teardown across multiple tests:

```go
func TestMain(m *testing.M) {
    // Setup code
    code := m.Run()
    // Cleanup code
    os.Exit(code)
}
```

## Current Test File Analysis

### Files Using hydronica/trial (41 files)

The following files use the trial framework:

- `apps/flowlord/taskmaster_test.go`
- `apps/flowlord/handler_test.go`
- `apps/flowlord/files_test.go`
- `apps/flowlord/batch_test.go`
- `apps/flowlord/cache/cache_test.go`
- `file/file_test.go`
- `file/util/util_test.go`
- `file/nop/nop_test.go`
- `file/minio/client_test.go`
- `file/minio/read_test.go`
- `file/minio/write_test.go`
- `file/local/read_test.go`
- `file/local/write_test.go`
- `file/local/local_test.go`
- `file/buf/buf_test.go`
- `file/stat/stat_test.go`
- `file/scanner_test.go`
- `workflow/workflow_test.go`
- `tmpl/tmpl_test.go`
- `db/prep_test.go`
- `db/batch/batch_test.go`
- `db/batch/stat_test.go`
- `consumer/discover_test.go`
- `bootstrap/bootstrap_test.go`
- `apps/workers/*/worker_test.go` (multiple worker test files)
- `apps/tm-archive/*/app_test.go` (multiple archive test files)
- `apps/utils/*/logger_test.go`, `stats_test.go`, `recap_test.go`, `filewatcher_test.go`

### Files Using testify/assert (2 files)

These files need conversion to trial or standard Go testing:

- `apps/tm-archive/http/http_test.go`
- `apps/utils/filewatcher/watcher_test.go`

### Example Function Usage

Files with extensive example functions:
- `file/writebyhour_test.go` (13 example functions)

## Conversion Strategies

### From testify/assert to trial

**Current testify pattern:**
```go
func TestFunction(t *testing.T) {
    result := functionUnderTest()
    assert.Equal(t, expected, result)
    assert.NotNil(t, err)
}
```

**Convert to trial pattern:**
```go
func TestFunction(t *testing.T) {
    fn := func(input InputType) (OutputType, error) {
        return functionUnderTest(input)
    }
    
    cases := trial.Cases[InputType, OutputType]{
        "success case": {
            Input:    testInput,
            Expected: expectedOutput,
        },
        "error case": {
            Input:     errorInput,
            ShouldErr: true,
        },
    }
    
    trial.New(fn, cases).Test(t)
}
```

### From testify/assert to standard Go testing

For simple cases that don't benefit from table-driven tests:

```go
func TestFunction(t *testing.T) {
    result := functionUnderTest()
    if result != expected {
        t.Errorf("got %v, expected %v", result, expected)
    }
}
```

## Best Practices

### 1. Test Structure

- Use descriptive test case names
- Group related test cases logically
- Use table-driven tests for multiple scenarios
- Keep test functions focused and single-purpose

### 2. Error Testing

```go
cases := trial.Cases[InputType, OutputType]{
    "error case": {
        Input:     errorInput,
        ShouldErr: true,
    },
}
```

### 3. Time Testing

Always use trial time utilities:
```go
// Good
trial.TimeDay("2023-01-01")
trial.TimeHour("2023-01-01T12")

// Avoid
time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
```

### 4. Complex Comparisons

Use appropriate comparers for complex types:
```go
trial.New(fn, cases).Comparer(trial.EqualOpt(trial.IgnoreAllUnexported)).Test(t)
```

### 5. Test Organization

- Place tests in `*_test.go` files
- Use `TestMain` for setup/teardown when needed
- Use example functions for API documentation
- Keep test data in separate files when appropriate

## Migration Plan

### Phase 1: Convert testify/assert usage

1. **apps/tm-archive/http/http_test.go**
   - Convert `assert.Equal` calls to trial cases
   - Convert `assert.Contains` to appropriate trial comparers

2. **apps/utils/filewatcher/watcher_test.go**
   - Convert `assert.Equal` and `assert.NotNil` calls
   - Create table-driven test cases

### Phase 2: Standardize patterns

1. Ensure all new tests use trial framework
2. Convert any remaining standard Go tests to trial when beneficial
3. Maintain example functions for documentation

### Phase 3: Documentation and training

1. Update this document as patterns evolve
2. Provide examples for common testing scenarios
3. Establish coding standards for test writing

## Common Test Patterns

### Testing with external dependencies

```go
func TestWithDependencies(t *testing.T) {
    fn := func(input InputType) (OutputType, error) {
        // Setup mocks or test doubles
        mockDep := &MockDependency{}
        service := NewService(mockDep)
        return service.Process(input)
    }
    
    cases := trial.Cases[InputType, OutputType]{
        "success": {
            Input:    validInput,
            Expected: expectedOutput,
        },
    }
    
    trial.New(fn, cases).Test(t)
}
```

### Testing async operations

```go
func TestAsyncOperation(t *testing.T) {
    fn := func(input InputType) (OutputType, error) {
        result := make(chan OutputType, 1)
        err := make(chan error, 1)
        
        go func() {
            output, e := asyncOperation(input)
            result <- output
            err <- e
        }()
        
        return <-result, <-err
    }
    
    cases := trial.Cases[InputType, OutputType]{
        "async success": {
            Input:    testInput,
            Expected: expectedOutput,
        },
    }
    
    trial.New(fn, cases).Timeout(5 * time.Second).Test(t)
}
```

This testing framework provides a consistent, maintainable approach to testing across the entire task-tools repository.
