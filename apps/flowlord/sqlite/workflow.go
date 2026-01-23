package sqlite

import (
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/hydronica/toml"
	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/pcelvng/task"
	"github.com/robfig/cron/v3"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/workflow"
)

// Phase represents a workflow phase (same as workflow.Phase)
type Phase struct {
	Task      string // Should use Topic() and Job() for access
	Rule      string
	DependsOn string // Task that the previous workflow depends on
	Retry     int
	Template  string // template used to create the task
}

type PhaseDB struct {
	Phase
	FilePath string // workflow file path
	Status   string // status of the phase (e.g. valid, invalid, warning)
}

func (p PhaseDB) Topic() string {
	return p.Phase.Topic()
}
func (p PhaseDB) Job() string {
	return p.Phase.Job()
}

func (ph Phase) IsEmpty() bool {
	return ph.Task == "" && ph.Rule == "" && ph.DependsOn == "" && ph.Template == ""
}

// Job portion of the Task
func (ph Phase) Job() string {
	s := strings.Split(ph.Task, ":")
	if len(s) > 1 {
		return s[1]
	}

	r, _ := url.ParseQuery(ph.Rule)
	if j := r.Get("job"); j != "" {
		return j
	}
	return ""
}

// Topic portion of the Task
func (ph Phase) Topic() string {
	s := strings.Split(ph.Task, ":")
	return s[0]
}

// Deprecated:
// ToWorkflowPhase converts cache.Phase to workflow.Phase
func (ph Phase) ToWorkflowPhase() workflow.Phase {
	return workflow.Phase{
		Task:      ph.Task,
		Rule:      ph.Rule,
		DependsOn: ph.DependsOn,
		Retry:     ph.Retry,
		Template:  ph.Template,
	}
}

// Workflow represents a workflow file with phases
type Workflow struct {
	Checksum string  // md5 hash for the file to check for changes
	Phases   []Phase `toml:"phase"`
}

// Workflow Cache Methods - implementing workflow.Cache interface

// IsDir returns true if the original workflow path is a folder rather than a file
func (s *SQLite) IsDir() bool {
	return s.isDir
}

// Search the all workflows within the cache and return the first
// matching phase with the specific task and job (optional)
func (s *SQLite) Search(taskType, job string) PhaseDB {
	return s.Get(task.Task{Type: taskType, Job: job})
}

// Get the Phase associated with the task
// looks for matching phases within a workflow defined in meta
// that matches the task Type and job.
func (s *SQLite) Get(t task.Task) PhaseDB {
	s.mu.Lock()
	defer s.mu.Unlock()

	values, _ := url.ParseQuery(t.Meta)
	//key := values.Get("workflow")
	job := t.Job
	if job == "" {
		job = values.Get("job")
	}
	key := t.Type
	if job != "" {
		key += ":" + job
	}

	query := `
		SELECT file_path, task, depends_on, rule, template, retry
		FROM workflow_phases 
		WHERE task = ?
		ORDER BY file_path, task
		LIMIT 1
	`
	rows, err := s.db.Query(query, key)
	if err != nil {
		return PhaseDB{Status: err.Error()}
	}
	defer rows.Close()

	if rows.Next() {
		ph := PhaseDB{}

		err := rows.Scan(&ph.FilePath, &ph.Task, &ph.DependsOn, &ph.Rule, &ph.Template, &ph.Retry)
		if err != nil {
			return PhaseDB{Status: err.Error()}
		}
		// Compute validation status on read instead of storing it
		ph.Status = ph.Phase.Validate()
		return ph
	}
	return PhaseDB{Status: "not found"}
}

// Children of the given task t, a child phase is one that dependsOn another task
// Empty slice will be returned if no children are found.
// A task without a type or metadata containing the workflow info
// will result in an error
func (s *SQLite) Children(t task.Task) []Phase {
	s.mu.Lock()
	defer s.mu.Unlock()

	if t.Type == "" {
		return nil
	}

	values, _ := url.ParseQuery(t.Meta)
	key := values.Get("workflow")
	job := t.Job
	if job == "" {
		job = values.Get("job")
	}

	if key == "" {
		return nil
	}

	// Find phases that depend on this task
	query := `
		SELECT task, depends_on, rule, template, retry
		FROM workflow_phases 
		WHERE file_path = ? AND (depends_on LIKE ? OR depends_on = ?)
		ORDER BY task
	`

	rows, err := s.db.Query(query, key, t.Type+":%", t.Type)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []Phase
	for rows.Next() {
		var taskStr, dependsOn, rule, template string
		var retry int

		err := rows.Scan(&taskStr, &dependsOn, &rule, &template, &retry)
		if err != nil {
			continue
		}

		// Parse depends_on to check if it matches the task
		v := strings.Split(dependsOn, ":")
		depends := v[0]
		var j string
		if len(v) > 1 {
			j = v[1]
		}

		if depends == t.Type {
			if j == "" || j == job {
				result = append(result, Phase{
					Task:      taskStr,
					Rule:      rule,
					DependsOn: dependsOn,
					Retry:     retry,
					Template:  template,
				})
			}
		}
	}

	return result
}

// Refresh checks the cache and reloads any files if the checksum has changed.
func (s *SQLite) Refresh() (changedFiles []string, err error) {
	if !s.isDir {
		f, err := s.loadFile(s.workflowPath, s.fOpts)
		if len(f) > 0 {
			changedFiles = append(changedFiles, f)
		}
		return changedFiles, err
	}

	// List and read all files
	allFiles, err := listAllFiles(s.workflowPath, s.fOpts)
	if err != nil {
		return changedFiles, err
	}

	errs := appenderr.New()
	for _, filePath := range allFiles {
		f, err := s.loadFile(filePath, s.fOpts)
		if err != nil {
			errs.Add(err)
		}
		if len(f) > 0 {
			changedFiles = append(changedFiles, f)
		}
	}

	// Remove deleted workflows from database
	for _, key := range s.GetWorkflowFiles() {
		found := false
		for _, v := range allFiles {
			f := s.filePath(v)
			if f == key {
				found = true
				break
			}
		}
		if !found {
			errs.Add(s.removeWorkflow(key))
			changedFiles = append(changedFiles, "-"+key)
		}
	}

	return changedFiles, errs.ErrOrNil()
}

// Helper methods for workflow operations

// listAllFiles recursively lists all files in a folder and sub-folders
func listAllFiles(p string, opts *file.Options) ([]string, error) {
	files := make([]string, 0)
	sts, err := file.List(p, opts)
	if err != nil {
		return nil, err
	}
	for _, f := range sts {
		if f.IsDir {
			s, err := listAllFiles(f.Path, opts)
			if err != nil {
				return nil, err
			}
			files = append(files, s...)
			continue
		}
		files = append(files, f.Path)
	}
	return files, nil
}

// loadFile checks a files checksum and updates database if required
// loaded file name is returned
func (s *SQLite) loadFile(path string, opts *file.Options) (f string, err error) {
	f = s.filePath(path)
	sts, err := file.Stat(path, opts)
	// permission issues
	if err != nil {
		return "", fmt.Errorf("stats %s %w", path, err)
	}
	// We can't process a directory here
	if sts.IsDir {
		return "", fmt.Errorf("can not read directory %s", path)
	}

	// Check if file has changed by comparing checksum
	existingHash := s.getFileHash(f)
	if existingHash == sts.Checksum {
		return "", nil // No changes
	}

	// Read and parse the workflow file
	r, err := file.NewReader(path, opts)
	if err != nil {
		return "", fmt.Errorf("new reader %s %w", path, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("read-all: %s %w", path, err)
	}

	var workflow Workflow
	if _, err := toml.Decode(string(b), &workflow); err != nil {
		return "", fmt.Errorf("decode: %s %w", string(b), err)
	}

	// Update database with new workflow data
	err = s.updateWorkflowInDB(f, sts.Checksum, workflow.Phases)
	if err != nil {
		return "", fmt.Errorf("update workflow in db: %w", err)
	}

	return f, nil
}

// filePath returns a filePath consist of all unique part
// after the path set in the cache
func (s *SQLite) filePath(p string) (filePath string) {
	path := strings.TrimLeft(s.workflowPath, ".")
	if i := strings.LastIndex(p, path); i != -1 {
		filePath = strings.TrimLeft(p[i+len(path):], "/")
	}
	if filePath == "" {
		_, filePath = filepath.Split(p)
	}
	return filePath
}

// getFileHash retrieves the current hash for a workflow file
func (s *SQLite) getFileHash(filePath string) string {
	path := s.filePath(filePath)
	var hash string
	err := s.db.QueryRow("SELECT file_hash FROM workflow_files WHERE file_path = ?", path).Scan(&hash)
	if err != nil {
		return ""
	}
	return hash
}

// GetWorkflowFiles returns a map of all workflow files in the database
func (s *SQLite) GetWorkflowFiles() []string {
	files := make([]string, 0)
	rows, err := s.db.Query("SELECT file_path FROM workflow_files")
	if err != nil {
		return files
	}
	defer rows.Close()

	for rows.Next() {
		var filePath string
		if err := rows.Scan(&filePath); err == nil {
			files = append(files, filePath)
		}
	}
	return files
}

// GetAllPhasesGrouped returns all phases grouped by workflow file
func (s *SQLite) GetAllPhasesGrouped() map[string][]PhaseDB {
	result := make(map[string][]PhaseDB)

	workflowFiles := s.GetWorkflowFiles()
	for _, filePath := range workflowFiles {
		phases, err := s.GetPhasesForWorkflow(filePath)
		if err != nil {
			continue
		}
		result[filePath] = phases
	}

	return result
}

// GetPhasesForWorkflow returns all phases for a specific workflow file
func (s *SQLite) GetPhasesForWorkflow(filePath string) ([]PhaseDB, error) {
	rows, err := s.db.Query(`
		SELECT file_path, task, depends_on, rule, template, retry
		FROM workflow_phases 
		WHERE file_path = ?
		ORDER BY task
	`, filePath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var phases []PhaseDB
	for rows.Next() {
		ph := PhaseDB{}

		err := rows.Scan(&ph.FilePath, &ph.Task, &ph.DependsOn, &ph.Rule, &ph.Template, &ph.Retry)
		if err != nil {
			continue
		}

		// Compute validation status on read instead of storing it
		ph.Status = ph.Phase.Validate()

		phases = append(phases, ph)
	}

	return phases, nil
}

// updateWorkflowInDB updates the workflow data in the database
func (s *SQLite) updateWorkflowInDB(filePath, checksum string, phases []Phase) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update or insert workflow file record
	_, err := s.db.Exec(`
		INSERT INTO workflow_files (file_path, file_hash, loaded_at, last_modified)
		VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (file_path) DO UPDATE SET
			file_hash = excluded.file_hash,
			loaded_at = CURRENT_TIMESTAMP,
			last_modified = CURRENT_TIMESTAMP
	`, filePath, checksum)
	if err != nil {
		return err
	}

	// Remove existing phases for this workflow
	_, err = s.db.Exec("DELETE FROM workflow_phases WHERE file_path = ?", filePath)
	if err != nil {
		return err
	}

	// Insert new phases
	for _, phase := range phases {
		task := phase.Task
		if !strings.Contains(task, ":") && phase.Job() != "" {
			task = task + ":" + phase.Job()
		}
		phase.Task = task

		_, err = s.db.Exec(`
			INSERT INTO workflow_phases (file_path, task, depends_on, rule, template, retry)
			VALUES (?, ?, ?, ?, ?, ?)
		`, filePath, phase.Task, phase.DependsOn, phase.Rule, phase.Template, phase.Retry)
		if err != nil {
			return err
		}
	}

	return nil
}

// removeWorkflow removes a workflow and its phases from the database
func (s *SQLite) removeWorkflow(filePath string) error {
	// Start transaction
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Remove phases first
	_, err = tx.Exec("DELETE FROM workflow_phases WHERE file_path = ?", filePath)
	if err != nil {
		return err
	}

	// Remove workflow file record
	_, err = tx.Exec("DELETE FROM workflow_files WHERE file_path = ?", filePath)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Validate a phase and returns status message
func (ph Phase) Validate() string {

	values, err := url.ParseQuery(ph.Rule)
	if err != nil {
		return fmt.Sprintf("invalid rule format: %s", ph.Rule)
	}

	// Basic validation logic
	if ph.DependsOn == "" && values.Get("cron") == "" && values.Get("files") == "" {
		return "non-scheduled phase: use depends_on, cron or files"
	}

	// Check for valid cron rule

	if c := values.Get("cron"); c != "" {

		if _, err := cron.NewParser(
			cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
		).Parse(c); err != nil {
			return fmt.Sprintf("invalid cron: %s %v", c, err)
		}

	}

	return "" // No issues
}
