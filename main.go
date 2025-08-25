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
				cli.StringFlag{
					Name:  "org",
					Usage: "Organization name of blobs to process",
				},
				cli.StringFlag{
					Name:  "repo",
					Usage: "Repository name of blobs to process",
				},
				cli.StringFlag{
					Name:  "tag",
					Usage: "Tag name of blobs to process",
				},
				cli.BoolFlag{
					Name:  "v",
					Usage: "Verbose logging written to file msr-check.log",
				},
				cli.BoolFlag{
					Name:  "json",
					Usage: "Produces JSON output instead of a text list",
				},
			},
		},
	}
	app.Name = "MSRCheck"
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
