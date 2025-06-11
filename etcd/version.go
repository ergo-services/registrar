package etcd

import "ergo.services/ergo/gen"

const (
	clientName    string = "ETCD Client" //
	clientRelease string = "R1"          // (Rev.1)

)

var (
	version = gen.Version{
		Name:    clientName,
		Release: clientRelease,
		License: gen.LicenseMIT,
	}
)
