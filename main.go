package main

import (
	"fmt"
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/codegangsta/cli"
	"github.com/scascketta/capmetricsd/daemon"
	"github.com/scascketta/capmetricsd/tools"
	"log"
	"os"
)

const (
	DB_ENV      = "CAPMETRICSDB"
	GET_USAGE   = "USAGE: capmetricsd get db dest min max"
	START_USAGE = "USAGE: capmetricsd start -t target-url --db db-path [--cronitor cronitor-url]"
)

var (
	elog = log.New(os.Stderr, "[ERR] ", log.LstdFlags|log.Lshortfile)
)

func main() {
	app := cli.NewApp()

	app.Name = "capmetricsd"
	app.Usage = "a tool to start the capmetricsd daemon or view captured data."

	app.Commands = []cli.Command{
		{
			Name:  "start",
			Usage: "start the capmetrics daemon (in the foreground)",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "target-url, t",
					Usage: "URL to a GTFS-realtime Vehicle Positions feed",
				},
				cli.StringFlag{
					Name:  "db-path, db",
					Usage: "Path to a BoltDB database.",
				},
				cli.StringFlag{
					Name:  "cronitor-url, cron",
					Usage: "(OPTIONAL) URL to send requests to notify Cronitor (or comparable monitoring service)",
				},
			},
			Action: func(ctx *cli.Context) {
				target := ctx.String("target-url")
				db := ctx.String("db-path")

				if target == "" || db == "" {
					fmt.Println(START_USAGE)
					return
				}

				cronitor := ctx.String("cronitor-url")

				log.Printf("Starting capmetrics daemon -- target: %s, dbPath: %s, cronitor URL: %s\n", target, db, cronitor)
				daemon.Start(target, cronitor, db)
			},
		},
		{
			Name:  "get",
			Usage: "get all data between two POSIX timestamps",
			Action: func(ctx *cli.Context) {
				if len(ctx.Args()) < 4 {
					log.Fatal("Missing command arguments\n", GET_USAGE)
				}

				db := ctx.Args()[0]
				dest := ctx.Args()[1]
				min := ctx.Args()[2]
				max := ctx.Args()[3]

				err := tools.GetData(db, dest, min, max)
				if err != nil {
					elog.Println(err)
				}
			},
		},
		{
			Name:  "ingest",
			Usage: "ingest historical CSV data",
			Action: func(ctx *cli.Context) {
				if len(ctx.Args()) < 1 {
					log.Fatal("Missing pattern location to CSV data")
				}
				tools.Ingest(ctx.Args()[0])
			},
		},
		{
			Name:  "stats",
			Usage: "stats on a Bolt database",
			Action: func(ctx *cli.Context) {
				db := os.Getenv(DB_ENV)
				if db == "" {
					if len(ctx.Args()) == 0 {
						log.Fatalf("missing path to Bolt database (either an env var - %s) or arg\n", DB_ENV)
					}
					db = ctx.Args()[0]
				}
				err := tools.PrintBoltStats(db)
				if err != nil {
					log.Fatal(err)
				}
			},
		},
	}

	app.Run(os.Args)
}
