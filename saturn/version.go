package saturn

import "ergo.services/ergo/gen"

const (
	clientName    string = "Saturn ESRD Client" // Ergo Service Registration and Discovery
	clientRelease string = "R1"                 // (Rev.1)

)

var (
	Version = gen.Version{
		Name:    clientName,
		Release: clientRelease,
		License: gen.LicenseBSL1,
	}
)
