package main

import (
	"context"
	"fmt"
	"net/http"
)

type taskmaster struct {
}

func (tm *taskmaster) Run(ctx context.Context) error {
	// register endpoints
	//http.HandleFunc("/", registry)
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/ping/", pingHandler)

	srvErr := make(chan error)
	go func() {
		srvErr <- http.ListenAndServe(":9090", nil)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-srvErr:
		return err
	}
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ok\n")
}
