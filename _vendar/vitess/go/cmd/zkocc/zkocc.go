// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"

	"github.com/yudppp/goworker/_vendar/vitess/go/relog"
	rpc "github.com/yudppp/goworker/_vendar/vitess/go/rpcplus"
	"github.com/yudppp/goworker/_vendar/vitess/go/rpcwrap/bsonrpc"
	"github.com/yudppp/goworker/_vendar/vitess/go/rpcwrap/jsonrpc"
	_ "github.com/yudppp/goworker/_vendar/vitess/go/snitch"
	"github.com/yudppp/goworker/_vendar/vitess/go/umgmt"
	"github.com/yudppp/goworker/_vendar/vitess/go/vt/servenv"
	"github.com/yudppp/goworker/_vendar/vitess/go/zk"
	"github.com/yudppp/goworker/_vendar/vitess/go/zk/zkocc"
)

const (
	DefaultLameDuckPeriod = 30.0
	DefaultRebindDelay    = 0.01
)

var usage = `Cache open zookeeper connections and allow cheap read requests
through a lightweight RPC interface.  The optional parameters are cell
names to try to connect to at startup, versus waiting for the first
request to connect.`

var (
	resolveLocal   = flag.Bool("resolve-local", false, "if specified, will try to resolve /zk/local/ paths. If not set, they will fail.")
	port           = flag.Int("port", 14850, "port for the server")
	lameDuckPeriod = flag.Float64("lame-duck-period", DefaultLameDuckPeriod, "how long to give in-flight transactions to finish")
	rebindDelay    = flag.Float64("rebind-delay", DefaultRebindDelay, "artificial delay before rebinding a hijacked listener")
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, usage)
	}
}

// zkocc: a proxy for zk
func main() {
	flag.Parse()
	if err := servenv.Init("zkocc"); err != nil {
		relog.Fatal("Error in servenv.Init: %v", err)
	}

	rpc.HandleHTTP()
	jsonrpc.ServeHTTP()
	jsonrpc.ServeRPC()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()

	zk.RegisterZkReader(zkocc.NewZkReader(*resolveLocal, flag.Args()))

	// we delegate out startup to the micromanagement server so these actions
	// will occur after we have obtained our socket.
	umgmt.SetLameDuckPeriod(float32(*lameDuckPeriod))
	umgmt.SetRebindDelay(float32(*rebindDelay))
	umgmt.AddStartupCallback(func() {
		umgmt.StartHttpServer(fmt.Sprintf(":%v", *port))
	})

	relog.Info("started zkocc %v", *port)
	umgmtSocket := fmt.Sprintf("/tmp/zkocc-%08x-umgmt.sock", *port)
	if umgmtErr := umgmt.ListenAndServe(umgmtSocket); umgmtErr != nil {
		relog.Error("umgmt.ListenAndServe err: %v", umgmtErr)
	}
	relog.Info("done")
}
