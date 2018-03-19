package tools

import (
	"fmt"
	"runtime"
)

var (
	// specifiy version at build time with `-ldflags "-X path.to.package.Version x.x.x"` etc...
	Version = "-"
)

func String() string {
	return fmt.Sprintf(
		"%s (built w/%s)",
		Version,
		runtime.Version(),
	)
}
