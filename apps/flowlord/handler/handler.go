package handler

import _ "embed"

//go:embed alert.tmpl
var AlertTemplate string

//go:embed files.tmpl
var FilesTemplate string

//go:embed task.tmpl
var TaskTemplate string

//go:embed header.tmpl
var HeaderTemplate string

//go:embed about.tmpl
var AboutTemplate string
