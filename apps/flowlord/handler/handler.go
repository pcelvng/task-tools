package handler

import _ "embed"

//go:embed alert.tmpl
var AlertTemplate string

//go:embed files.tmpl
var FilesTemplate string

//go:embed task.tmpl
var TaskTemplate string
