package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	jobSuccessMetric *prometheus.CounterVec
	jobFailureMetric *prometheus.CounterVec
	jobRuntimeMetric *prometheus.HistogramVec
)

func init() {
	jobSuccessMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "job_success",
		Help: "The number of successful jobs",
	}, []string{"task_type", "job_id"})
	jobFailureMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "job_failure",
		Help: "The number of failed jobs",
	}, []string{"task_type", "job_id"})
	jobRuntimeMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "job_runtime",
		Help: "The runtime of the job in seconds",
	}, []string{"task_type", "job_id"})
}

func registerMetrics() {
	prometheus.MustRegister(jobSuccessMetric)
	prometheus.MustRegister(jobFailureMetric)
	prometheus.MustRegister(jobRuntimeMetric)
}

func metricsHandler() http.Handler {
	return promhttp.Handler()
}
