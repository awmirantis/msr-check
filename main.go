package main

import (
	"os"

	"github.com/awmirantis/dtr_check/missingblobs"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Commands = []cli.Command{
		{
			Name:   "missing_blobs",
			Usage:  "Detect missing blobs",
			Action: missingblobs.Run,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     "replica_id",
					Usage:    "Replica ID of the rethinkdb",
					Required: true,
				},
				cli.StringFlag{
					Name:     "o",
					Usage:    "Filename to output results to. Requires the volume :/out",
					Required: true,
				},
			},
		},
	}
	app.Name = "MSRCheck"
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
