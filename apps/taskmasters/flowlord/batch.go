package main

type batchJob struct {
	cronJob

	FilePath string `uri:"origin"`
}

func (j *batchJob) Run() {
	// process batch details
	// create time run range
	// pull details from origin file
	// get details of child jobs to run
}
