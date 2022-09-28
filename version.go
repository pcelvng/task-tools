package tools

import (
	"fmt"
	"os"
	"runtime"
)

var (
	// specify version, BuildTimeUTC, AppName at build time with `-ldflags "-X path.to.package.Version x.x.x"` etc...
	Version      = "-"
	BuildTimeUTC = "-"

// AppName      = "-"
)

func ShowVersion(show bool) {
	if show {
		fmt.Println(String())
		os.Exit(0)
	}
}

func String() string {
	return fmt.Sprintf(
		"%s (built w/%s)\nUTC Build Time: %v",
		//		AppName,
		Version,
		runtime.Version(),
		BuildTimeUTC,
	)
}
