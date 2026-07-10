package benchmeta

import (
	"runtime"
	"time"
)

const MachineTag = "Intel MacBook Pro, quad-core i5-1038NG7 (8 threads), 16GB RAM, Ubuntu 20.04, native Docker"

type Environment struct {
	GoVersion  string `json:"go_version"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	RealInfra  bool   `json:"real_infra"`
	MachineTag string `json:"machine_tag"`
	Timestamp  string `json:"timestamp"`
}

func CurrentEnvironment(realInfra bool) Environment {
	return Environment{
		GoVersion:  runtime.Version(),
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		OS:         runtime.GOOS,
		Arch:       runtime.GOARCH,
		RealInfra:  realInfra,
		MachineTag: MachineTag,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
	}
}
