package main

import (
	"github.com/scascketta/capmetricsd/Godeps/_workspace/src/github.com/codegangsta/cli"
	"github.com/scascketta/capmetricsd/daemon"
	"github.com/scascketta/capmetricsd/tools"
	"log"
	"os"
)

const (
	DB_ENV = "CAPMETRICSDB"
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
			Action: func(ctx *cli.Context) {
				log.Println("Launching capmetrics daemon")
				daemon.Start()
			},
		},
		{
			Name:  "get",
			Usage: "get all data between two POSIX timestamps",
			Action: func(ctx *cli.Context) {
				dbPath := os.Getenv(DB_ENV)
				if dbPath == "" {
					log.Println("Missing env var:", DB_ENV)
					return
				}

				if len(ctx.Args()) < 3 {
					log.Fatal("Missing command arguments")
				}

				dest := ctx.Args()[0]
				min := ctx.Args()[1]
				max := ctx.Args()[2]

				err := tools.GetData(dbPath, dest, min, max)
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
