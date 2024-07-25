package missingblobs

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/awmirantis/dtr_check/util"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

type BlobManager struct {
	session      *r.Session
	shaToPK      map[string][]string
	replicaID    string
	missingBlobs []Blob
}

type ManifestLayer struct {
	Digest         string   `gorethink:"digest"`
	Size           int64    `gorethink:"size"`
	MediaType      string   `gorethink:"mediaType"`
	DockerfileLine string   `gorethink:"dockerfileLine"`
	URLs           []string `gorethink:"urls,omitempty"`
}

type Manifest struct {
	PK         string           `gorethink:"pk"`
	Digest     string           `gorethink:"digest"`
	Repository string           `gorethink:"repository"`
	Layers     []*ManifestLayer `gorethink:"layers"`
}

type Blob struct {
	Sha256sum    string `gorethink:"sha256sum"`
	ID           string `gorethink:"id"`
	Repositories []string
}

type Tags struct {
	Repository string `gorethink:"repository"`
	Name       string `gorethink:"name"`
}

func Run(cliContext *cli.Context) error {
	replicaID := cliContext.String("replica_id")
	outputFile := cliContext.String("o")
	session, err := util.GetSession(replicaID)
	if err != nil {
		return fmt.Errorf("could not get conenct to rethinkdb: %s", err)
	}
	bm := BlobManager{
		session:   session,
		replicaID: replicaID,
		shaToPK:   make(map[string][]string),
	}

	err = bm.findMissingBlobs()
	if err != nil {
		return fmt.Errorf("failed to retrieve missing blobs: %s", err)
	}
	if len(bm.missingBlobs) == 0 {
		log.Errorf("No missing blobs found")
		return nil
	}

	err = bm.makeShaMap()
	if err != nil {
		return fmt.Errorf("failed to make SHA mapping table: %s", err)
	}
	for idx := range bm.missingBlobs {
		repos, ok := bm.shaToPK[bm.missingBlobs[idx].Sha256sum]
		if !ok {
			log.Errorf("could not find repo for blob: %s", bm.missingBlobs[idx].ID)
		} else {
			bm.missingBlobs[idx].Repositories = repos
		}
	}
	jsonData, err := json.Marshal(bm.missingBlobs)
	if err != nil {
		return fmt.Errorf("could not marshal json object: %s", err)
	}
	if outputFile == "" {
		fmt.Printf("%s", string(jsonData))
	} else {
		if outputFile != "" {
			filePath := fmt.Sprintf("/out/%s", outputFile)
			err = os.WriteFile(filePath, jsonData, 0666)
			if err != nil {
				return fmt.Errorf("failed to write to file: '%s' err: %s", filePath, err)
			}
		}
	}
	return nil
}

func (bm *BlobManager) findMissingBlobs() error {
	q := r.DB("dtr2").Table("blobs").Pluck("sha256sum", "id")
	cursor, err := q.Run(bm.session)
	if err != nil {
		return fmt.Errorf("failed to query blobs table: %s", err)
	}
	defer cursor.Close()
	blobs := []Blob{}
	err = cursor.All(&blobs)
	if err != nil {
		return fmt.Errorf("failed to read blobs table: %s", err)
	}
	for _, blob := range blobs {
		blobPath := fmt.Sprintf("/storage/docker/registry/v2/blobs/id/%s/%s/data", blob.ID[0:2], blob.ID)
		_, err := os.Stat(blobPath)
		if err != nil {
			bm.missingBlobs = append(bm.missingBlobs, blob)
		}
	}
	return nil
}

func (bm *BlobManager) makeShaMap() error {
	q := r.DB("dtr2").Table("manifests").Pluck("pk", "digest", "repository", "layers")
	cursor, err := q.Run(bm.session)
	if err != nil {
		return fmt.Errorf("failed to query manifests table: %s", err)
	}
	defer cursor.Close()
	manifests := []Manifest{}
	err = cursor.All(&manifests)
	if err != nil {
		return fmt.Errorf("failed to read manifests table: %s", err)
	}
	for _, manifest := range manifests {
		repo, err := bm.getRepoFromPK(manifest.PK)
		if err != nil {
			return fmt.Errorf("failed to get repo from SHA key: %s", err)
		}
		for _, layer := range manifest.Layers {
			bm.shaToPK[layer.Digest] = append(bm.shaToPK[layer.Digest], repo)
		}
	}
	return nil
}

func (bm *BlobManager) getRepoFromPK(pk string) (string, error) {
	q := r.DB("dtr2").Table("tags").Filter(map[string]interface{}{"digestPK": pk}).Pluck("name", "repository")
	cursor, err := q.Run(bm.session)
	if err != nil {
		return "", fmt.Errorf("could not find tag: %s", err)
	}
	repo := Tags{}
	err = cursor.One(&repo)
	if err != nil {
		return "", fmt.Errorf("failed to find tag: %s", err)
	}
	return fmt.Sprintf("%s:%s", repo.Repository, repo.Name), nil
}
