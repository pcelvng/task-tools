package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/pcelvng/task"
)

var rerunMetaDrop = []string{"retry", "retried", "delayed"}

func sanitizeRerunMeta(meta string) (string, error) {
	vals, err := url.ParseQuery(meta)
	if err != nil {
		return "", err
	}
	for _, k := range rerunMetaDrop {
		vals.Del(k)
	}
	vals.Set("rerun", "manual")
	s, err := url.QueryUnescape(vals.Encode())
	if err != nil {
		return "", err
	}
	return s, nil
}

func taskForRerun(orig task.Task) (*task.Task, error) {
	meta, err := sanitizeRerunMeta(orig.Meta)
	if err != nil {
		return nil, err
	}
	t := task.NewWithID(orig.Type, orig.Info, orig.ID)
	t.Job = orig.Job
	t.Meta = meta
	return t, nil
}

type rerunRequest struct {
	Type    string `json:"type"`
	Job     string `json:"job"`
	ID      string `json:"id"`
	Created string `json:"created"`
}

type rerunResponse struct {
	Status string    `json:"Status"`
	Task   task.Task `json:"Task"`
}

func (tm *taskMaster) rerunHandler(w http.ResponseWriter, r *http.Request) {
	var req rerunRequest
	b, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Type == "" || req.ID == "" || req.Created == "" {
		http.Error(w, "type, id, and created are required", http.StatusBadRequest)
		return
	}

	orig, err := tm.taskCache.GetTaskRecord(req.Type, req.Job, req.ID, req.Created)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "task not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	t, err := taskForRerun(orig)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tm.taskCache.Add(*t)
	if err := tm.producer.Send(t.Type, t.JSONBytes()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(rerunResponse{
		Status: "Rerun queued",
		Task:   *t,
	})
}
