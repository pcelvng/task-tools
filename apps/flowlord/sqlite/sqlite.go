package sqlite

import (
	"database/sql"
	_ "embed"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/pcelvng/task-tools/file"
)

//go:embed schema.sql
var schema string

const (
	// DefaultPageSize is the default number of items per page for paginated queries
	DefaultPageSize = 100
)

// currentSchemaVersion is the current version of the database schema.
// Increment this when making schema changes that require migration.
// Version 1: Initial schema
// Version 2: Added date_index table for performance optimization
const currentSchemaVersion = 2

type SQLite struct {
	LocalPath  string
	BackupPath string

	TaskTTL   time.Duration `toml:"task-ttl" comment:"time that tasks are expected to have completed in. This values tells the cache how long to keep track of items and alerts if items haven't completed when the cache is cleared"`
	Retention time.Duration // 90 days

	db    *sql.DB
	fOpts *file.Options
	mu    sync.Mutex

	// Workflow-specific fields
	workflowPath string
	isDir        bool
}

// Open the sqlite DB. If localPath doesn't exist then check if BackupPath exists and copy it to localPath
// Also initializes workflow path and determines if it's a directory
func (o *SQLite) Open(workflowPath string, fOpts *file.Options) error {
	o.fOpts = fOpts
	o.workflowPath = workflowPath
	if o.TaskTTL < time.Hour {
		o.TaskTTL = time.Hour
	}
	if o.db == nil {
		if err := o.initDB(); err != nil {
			return err
		}
	}

	// Determine if workflow path is a directory
	sts, err := file.Stat(workflowPath, fOpts)
	if err != nil {
		return fmt.Errorf("problem with workflow path %s %w", workflowPath, err)
	}
	o.isDir = sts.IsDir
	_, err = o.Refresh()

	return err
}

func (o *SQLite) initDB() error {
	backupSts, _ := file.Stat(o.BackupPath, o.fOpts)
	localSts, _ := file.Stat(o.LocalPath, o.fOpts)

	if localSts.Size == 0 && backupSts.Size > 0 {
		log.Printf("Restoring local DB from backup %s", o.BackupPath)
		// no local file but backup exists so copy it down
		if err := copyFiles(o.BackupPath, o.LocalPath, o.fOpts); err != nil {
			log.Println(err) // TODO: should this be fatal?
		}
	}

	// Open the database
	db, err := sql.Open("sqlite", o.LocalPath)
	if err != nil {
		return err
	}

	// Enable WAL mode for safer concurrent reads and writes
	// WAL mode allows multiple readers and one writer simultaneously
	_, err = db.Exec("PRAGMA journal_mode = WAL;")
	if err != nil {
		return fmt.Errorf("failed to set WAL mode: %w", err)
	}

	// Set busy timeout to handle concurrent access gracefully
	// This prevents immediate lock failures by retrying for up to 30 seconds
	_, err = db.Exec("PRAGMA busy_timeout = 30000;")
	if err != nil {
		return fmt.Errorf("failed to set busy timeout: %w", err)
	}

	// Set a smaller page size to reduce DB file size
	_, err = db.Exec("PRAGMA page_size = 4096;")
	if err != nil {
		return fmt.Errorf("failed to set page size: %w", err)
	}

	// Enable auto vacuum to reclaim space when records are deleted
	_, err = db.Exec("PRAGMA auto_vacuum = INCREMENTAL;")
	if err != nil {
		return fmt.Errorf("failed to set auto vacuum: %w", err)
	}

	o.db = db

	// Check and migrate schema if needed
	// This will handle initial schema creation for new databases (version 0)
	// and apply incremental migrations for existing databases
	if err := o.migrateIfNeeded(); err != nil {
		return fmt.Errorf("schema migration failed: %w", err)
	}

	return nil
}

func copyFiles(src, dst string, fOpts *file.Options) error {
	r, err := file.NewReader(src, fOpts)
	if err != nil {
		return fmt.Errorf("init reader err: %w", err)
	}
	w, err := file.NewWriter(dst, fOpts)
	if err != nil {
		return fmt.Errorf("init writer err: %w", err)
	}
	_, err = io.Copy(w, r)
	if err != nil {
		return fmt.Errorf("copy err: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer err: %w", err)
	}
	return r.Close()
}

// migrateIfNeeded checks the current schema version and applies migrations if needed
func (o *SQLite) migrateIfNeeded() error {
	currentVersion := o.GetSchemaVersion()

	if currentVersion < currentSchemaVersion {
		log.Printf("Migrating database schema from version %d to %d", currentVersion, currentSchemaVersion)
		if err := o.migrateSchema(currentVersion); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
		if err := o.setVersion(currentSchemaVersion); err != nil {
			return fmt.Errorf("failed to update schema version: %w", err)
		}
		log.Printf("Schema migration completed successfully")
	}

	return nil
}

// GetSchemaVersion returns the current schema version from the database.
// Returns 0 if the schema_version table doesn't exist or is empty (new database).
func (o *SQLite) GetSchemaVersion() int {
	var version int
	err := o.db.QueryRow("SELECT version FROM schema_version LIMIT 1").Scan(&version)
	if err != nil {
		// Table doesn't exist or other error - treat as version 0 (new database)
		// schema.sql will create the table when applied
		return 0
	}

	return version
}

// setVersion updates the schema version in the database
func (o *SQLite) setVersion(version int) error {
	// Delete all existing records to ensure we only have one row
	_, err := o.db.Exec("DELETE FROM schema_version")
	if err != nil {
		return fmt.Errorf("failed to clear schema_version: %w", err)
	}

	// Insert the new version
	_, err = o.db.Exec("INSERT INTO schema_version (version) VALUES (?)", version)
	if err != nil {
		return fmt.Errorf("failed to insert schema version: %w", err)
	}

	return nil
}

// migrateSchema applies version-specific migrations based on the current version
func (o *SQLite) migrateSchema(currentVersion int) error {
	// Version 0 → 1: Initial schema creation for new databases
	if currentVersion < 1 {
		log.Println("Creating initial schema (version 1)")

		// Drop workflow tables if they exist (they may have been created before versioning)
		_, err := o.db.Exec(`
			DROP TABLE IF EXISTS workflow_files;
			DROP TABLE IF EXISTS workflow_phases;
		`)
		if err != nil {
			return fmt.Errorf("failed to drop old workflow tables: %w", err)
		}

		_, err = o.db.Exec(schema)
		if err != nil {
			return fmt.Errorf("failed to create initial schema: %w", err)
		}
	}

	// Version 1 → 2: Add date_index table for performance optimization
	if currentVersion < 2 {
		log.Println("Migrating schema from version 1 to 2 (adding date_index table)")

		// Re-apply schema.sql - it has IF NOT EXISTS so it's safe and will add new tables
		_, err := o.db.Exec(schema)
		if err != nil {
			return fmt.Errorf("failed to apply schema for version 2: %w", err)
		}

		// Populate the date_index from existing data
		if err := o.RebuildDateIndex(); err != nil {
			return fmt.Errorf("failed to populate date_index: %w", err)
		}

		log.Println("Successfully migrated to schema version 2")
	}

	// Add future migrations here as needed:
	// Example:
	// if currentVersion < 3 {
	//     db := o.db
	//     // Drop column by recreating table (since data loss is OK)
	//     db.Exec("DROP TABLE IF EXISTS task_records")
	//     // schema.sql will recreate it with correct structure
	// }

	return nil
}

// Close the DB connection and copy the current file to the backup location
func (o *SQLite) Close() error {
	var errs []error
	if err := o.db.Close(); err != nil {
		errs = append(errs, err)
	}
	if o.BackupPath != "" {
		log.Printf("Backing up DB to %s", o.BackupPath)
		if err := o.Sync(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

// Sync the local DB to the backup location
func (o *SQLite) Sync() error {
	return copyFiles(o.LocalPath, o.BackupPath, o.fOpts)
}



