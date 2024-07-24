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
					Usage:    "replica_id of the rethinkdb",
					Required: true,
				},
			},
		},
	}
	app.Name = "DTRCheck"
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

// REPLICA_ID=$(docker ps --filter name=dtr-rethinkdb --format "{{ .Names }}" | cut -d"-" -f3)
// docker run --rm -i --net dtr-ol -e DTR_REPLICA_ID=${REPLICA_ID} -v dtr-ca-$REPLICA_ID:/tls awmirantis
