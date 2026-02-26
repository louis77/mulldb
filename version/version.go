package version

import "runtime/debug"

// These vars are set at build time via:
//
//	go build -ldflags "-X mulldb/version.Tag=v1.0.0 -X mulldb/version.GitCommit=abc1234 -X mulldb/version.BuildTime=2026-02-26T00:00:00Z"
var (
	Tag       = "dev"
	GitCommit = "" // empty = auto-detect from build info
	BuildTime = "" // empty = auto-detect from build info
)

func String() string {
	commit, buildTime := GitCommit, BuildTime
	if commit == "" || buildTime == "" {
		if info, ok := debug.ReadBuildInfo(); ok {
			for _, s := range info.Settings {
				switch s.Key {
				case "vcs.revision":
					if commit == "" && len(s.Value) >= 8 {
						commit = s.Value[:8]
					}
				case "vcs.time":
					if buildTime == "" {
						buildTime = s.Value
					}
				}
			}
		}
	}
	if commit == "" {
		commit = "unknown"
	}
	if buildTime == "" {
		buildTime = "unknown"
	}
	return "PostgreSQL 15.0 (mulldb " + Tag + ", commit " + commit + ", built " + buildTime + ")"
}
