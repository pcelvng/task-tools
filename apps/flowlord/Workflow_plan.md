# Workflow SQLite Persistence Plan
Purpose: Detailed specification for converting the in-memory workflow.Cache system to use SQLite persistence while maintaining exact interface compatibility.

## Current Status
**❌ NOT IMPLEMENTED** - The workflow system currently uses in-memory `workflow.Cache` implementation.

## Overview
This plan outlines the conversion of the existing in-memory workflow cache to a SQLite-based persistent system while maintaining 100% interface compatibility. The goal is zero breaking changes - all existing code continues to work unchanged.

## Database Schema Design

### Workflow File Tracking
```sql
-- Workflow file tracking (replaces in-memory workflow file cache)
CREATE TABLE workflow_files (
    file_path TEXT PRIMARY KEY,
    file_hash TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP,
);

-- Workflow phases (matches Phase struct exactly)
CREATE TABLE workflow_phases (
    file_path TEXT NOT NULL,
    task TEXT NOT NULL,           -- topic:job format (e.g., "data-load:hourly")
    depends_on TEXT,
    rule TEXT,                    -- URI query parameters (e.g., "cron=0 0 * * *&offset=1h")
    template TEXT,
    retry INTEGER DEFAULT 0,      -- threshold of times to retry
    status TEXT,                  -- phase status info (warnings, errors, validation messages)
    PRIMARY KEY (file_path, task)
);

-- Task relationships are generated dynamically from workflow_phases
-- No separate table needed - relationships are derived from depends_on field
-- This approach is simpler, more maintainable, and always up-to-date

-- Indexes for performance
CREATE INDEX idx_workflow_phases_task ON workflow_phases (task);
CREATE INDEX idx_workflow_phases_depends_on ON workflow_phases (depends_on);
CREATE INDEX idx_workflow_phases_status ON workflow_phases (status);
```

## Benefits of Human-Readable Keys

### 1. Direct File Path References
- `workflow_files.file_path` is the primary key (e.g., "workflows/data-load.toml")
- No need to join tables to see which file a phase belongs to
- Easy to identify and debug workflow issues

### 2. Composite Primary Keys for Phases
- `(file_path, task_name, job_name)` uniquely identifies each phase
- Directly readable: `("workflows/data-load.toml", "data-load", "hourly")`
- No surrogate IDs to remember or map

### 3. Dynamic Task Relationships
- Task relationships are generated from `depends_on` field in workflow_phases
- No separate table to maintain or keep in sync
- Always up-to-date with current workflow configuration
- Simpler schema with fewer tables and foreign keys

### 4. Phase Status Tracking
- `status` field stores validation messages, warnings, and errors for each phase
- Replaces console logging with persistent database storage
- Enables querying and filtering phases by status
- Provides better debugging and monitoring capabilities

**Status Field Usage Examples:**
- `"invalid phase: rule and dependsOn are blank"`
- `"no valid rule found: cron=invalid"`
- `"parent task not found: data-load"`
- `"ignored rule: cron=0 0 * * *"`
- `"warning: retry count exceeds recommended limit"`
- `""` (empty string for phases with no issues)

## Example Queries

```sql
-- Find all phases in a specific workflow file
SELECT task, depends_on, rule, status
FROM workflow_phases 
WHERE file_path = 'workflows/data-load.toml';

-- Find phases that depend on a specific task type
SELECT file_path, task, rule, status
FROM workflow_phases 
WHERE depends_on = 'data-load';

-- Find phases by topic (using LIKE for topic:job matching)
SELECT file_path, task, depends_on, rule, status
FROM workflow_phases 
WHERE task LIKE 'data-load:%';

-- Find phases with warnings or errors
SELECT file_path, task, status
FROM workflow_phases 
WHERE status IS NOT NULL AND status != '';

-- Find phases with specific status messages
SELECT file_path, task, status
FROM workflow_phases 
WHERE status LIKE '%warning%' OR status LIKE '%error%';

-- Generate task relationships dynamically (parent -> child)
SELECT 
    parent.depends_on as parent_task,
    parent.task as child_task,
    parent.file_path,
    parent.rule as child_rule,
    parent.status as child_status
FROM workflow_phases parent
WHERE parent.depends_on IS NOT NULL AND parent.depends_on != '';

-- Find all children of a specific task
SELECT 
    child.task as child_task,
    child.file_path,
    child.rule as child_rule,
    child.status as child_status
FROM workflow_phases child
WHERE child.depends_on = 'data-load';

-- Find all parents of a specific task
SELECT 
    parent.depends_on as parent_task,
    parent.file_path,
    parent.rule as parent_rule,
    parent.status as parent_status
FROM workflow_phases parent
WHERE parent.task = 'data-load:hourly';

-- Get workflow file info with phase count and status summary
SELECT 
    wf.file_path, 
    wf.file_hash, 
    wf.loaded_at, 
    COUNT(wp.task) as phase_count,
    COUNT(CASE WHEN wp.status IS NOT NULL AND wp.status != '' THEN 1 END) as phases_with_status
FROM workflow_files wf
LEFT JOIN workflow_phases wp ON wf.file_path = wp.file_path
GROUP BY wf.file_path;
```

## Maintained Interface Design

The new SQLite-based implementation will maintain the exact same interface as the current `workflow.Cache`:

```go
// Keep existing workflow.Cache interface unchanged
type Cache interface {
    // Existing methods remain exactly the same
    Search(task, job string) (path string, ph Phase)
    Get(t task.Task) Phase
    Children(t task.Task) []Phase
    Refresh() (changedFiles []string, err error)
    IsDir() bool
    Close() error
}

// Keep existing Phase struct unchanged
type Phase struct {
    Task      string // Should use Topic() and Job() for access
    Rule      string
    DependsOn string // Task that the previous workflow depends on
    Retry     int
    Template  string // template used to create the task
}

// Keep existing Workflow struct unchanged
type Workflow struct {
    Checksum string  // md5 hash for the file to check for changes
    Phases   []Phase `toml:"phase"`
}
```

## Implementation Strategy

- **Same Package**: Keep everything in `cache` package (not workflow package)
- **Same Structs**: Maintain `Phase` and `Workflow` structs exactly as they are
- **Same Methods**: All existing methods return the same types and behavior
- **SQLite Backend**: Add workflow methods directly to existing `SQLite` struct
- **Zero Breaking Changes**: All existing unit tests continue to work unchanged
- **No Interface Switching**: Do not create new interfaces or compatibility layers

## Key Benefits

1. **Persistence**: Workflow configurations survive restarts
2. **Historical Tracking**: Full audit trail of task relationships
3. **Performance**: Indexed queries for fast dependency resolution
4. **Scalability**: No memory limitations for large workflow sets
5. **Debugging**: Rich querying capabilities for troubleshooting
6. **Simplified Architecture**: Single SQLite instance replaces in-memory cache

## Implementation Plan

### Phase 1: Update Workflow Package
1. Modify existing `workflow.Cache` to use SQLite backend
2. Keep all existing interfaces, structs, and method signatures unchanged
3. Add SQLite persistence to workflow file loading
4. Implement task relationship tracking within existing structure

### Phase 2: Update Flowlord Integration
1. No changes needed to `taskmaster.go` - same interface
2. Update workflow loading to use SQLite persistence
3. Add task relationship recording to existing task processing
4. Maintain all existing method calls and behavior

### Phase 3: Handler Updates
1. Update handlers to query SQLite for workflow data
2. Add task relationship queries to existing endpoints
3. Enhance alert system with SQLite-based data
4. Maintain existing response formats

### Phase 4: Testing and Validation
1. All existing unit tests continue to work unchanged
2. Add SQLite-specific integration tests
3. Performance testing for SQLite queries
4. Migration testing from existing workflow files

## Migration Strategy

### Seamless Replacement Approach
- Keep existing `workflow.Cache` interface and structs
- Replace in-memory storage with SQLite persistence
- Zero breaking changes to existing code
- All unit tests continue to work without modification

### Key Implementation Details

```go
// Add workflow methods directly to existing SQLite struct
type SQLite struct {
    LocalPath  string
    BackupPath string
    TaskTTL   time.Duration
    Retention time.Duration
    db    *sql.DB
    fOpts file.Options
    mu sync.Mutex
    
    // Add workflow-specific fields
    workflowPath string
    isDir bool
}

// Add workflow methods to existing SQLite struct
func (s *SQLite) Search(task, job string) (path string, ph Phase) {
    // Query: SELECT file_path, task, depends_on, rule, template, retry, status
    //        FROM workflow_phases WHERE task = ? OR task LIKE ?
    //        (where ? is either exact match or topic:job format)
    // Return same results as before, with status info available
}

func (s *SQLite) Get(t task.Task) Phase {
    // Query: SELECT file_path, task, depends_on, rule, template, retry, status
    //        FROM workflow_phases WHERE task = ? OR task LIKE ?
    //        (where ? is either exact match or topic:job format)
    // Return same Phase struct with status info
}

func (s *SQLite) Children(t task.Task) []Phase {
    // Query: SELECT file_path, task, depends_on, rule, template, retry, status
    //        FROM workflow_phases WHERE depends_on = ? OR depends_on LIKE ?
    //        (where ? matches the task type or topic:job format)
    // Return same []Phase slice with status info
}

func (s *SQLite) Refresh() (changedFiles []string, err error) {
    // Check file hashes against workflow_files table using file_path as key
    // Load changed files into SQLite using file_path as primary key
    // Return same changedFiles list
}

func (s *SQLite) IsDir() bool {
    return s.isDir
}

func (s *SQLite) Close() error {
    // Existing close logic + any workflow cleanup
}
```

### Database Schema Migration
- Add new tables alongside existing ones
- Migrate existing workflow data if needed
- Remove old tables after successful migration
- Maintain data integrity throughout process

## Required Code Changes

### 1. Package Imports
```go
// Keep existing import - no changes needed
import "github.com/pcelvng/task-tools/workflow"
```

### 2. TaskMaster Struct Update
```go
// Keep existing struct - no changes needed
type taskMaster struct {
    // ... other fields
    *workflow.Cache  // Same as before
    // ... other fields
}
```

### 3. Workflow Loading Changes
```go
// Update to use SQLite struct directly instead of workflow.New()
// The SQLite struct will implement the workflow.Cache interface
if tm.Cache, err = tm.taskCache; err != nil {
    return fmt.Errorf("workflow setup %w", err)
}
// Or assign directly since SQLite now implements the interface
tm.Cache = tm.taskCache
```

### 4. Dependency Resolution Updates
```go
// Keep existing code - no changes needed
phase := tm.Cache.Get(task)
children := tm.Cache.Children(task)
```

### 5. Handler Updates
```go
// Keep existing code - no changes needed
// All existing method calls work the same
// Only internal implementation changes to use SQLite
```

## Technical Considerations

### Performance
- Indexed queries for fast dependency resolution
- Prepared statements for security and performance
- Connection pooling for concurrent access
- WAL mode for better concurrent access

### Data Integrity
- Foreign key constraints for data integrity
- Transaction support for atomic operations
- Proper error handling and rollback
- Data validation on insert/update

### Monitoring
- Query performance tracking
- Database size monitoring
- Connection pool metrics
- Error rate tracking

## Future Enhancements

### Advanced Features
- Workflow versioning and history
- Phase execution statistics
- Dependency visualization
- Workflow validation and testing
- Hot reloading of workflow changes

### Integration
- REST API endpoints for workflow management
- Web UI for workflow editing
- Workflow import/export functionality
- Integration with external workflow tools

