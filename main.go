package main

import (
	"github.com/codegangsta/cli"
	"github.com/scascketta/capmetricsd/daemon"
	"github.com/scascketta/capmetricsd/tools"
	"log"
	"os"
	"strconv"
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
			Usage: "get all data between two unix timestamps",
			Action: func(ctx *cli.Context) {
				dest := ctx.Args()[0]

				errMsg := "Error parsing time %s: %s.\n"
				minUnix, maxUnix := ctx.Args()[1], ctx.Args()[2]

				minStr, err := strconv.ParseInt(minUnix, 10, 64)
				if err != nil {
					log.Printf(errMsg, minUnix, err)
					return
				}
				maxStr, err := strconv.ParseInt(maxUnix, 10, 64)
				if err != nil {
					log.Printf(errMsg, maxUnix, err)
					return
				}

				err = tools.GetData("./capmetrics.db", dest, minStr, maxStr)
				if err != nil {
					log.Println(err)
				}
			},
		},
		{
			Name:  "ingest",
			Usage: "ingest historical CSV data",
			Action: func(ctx *cli.Context) {
				tools.Ingest(ctx.Args()[0])
			},
		},
	}

	app.Run(os.Args)
}
