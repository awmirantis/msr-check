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
	session, err := util.GetSession(replicaID)
	if err != nil {
		return fmt.Errorf("could not get conenct to rethinkdb: %s", err)
	}
	mb := BlobManager{
		session:   session,
		replicaID: replicaID,
		shaToPK:   make(map[string][]string),
	}
	err = mb.makeShaMap()
	if err != nil {
		return fmt.Errorf("failed to make SHA mapping table: %s", err)
	}
	err = mb.findMissingBlobs()
	if err != nil {
		return fmt.Errorf("failed to find missing blobs: %s", err)
	}
	for idx := range mb.missingBlobs {
		repos, ok := mb.shaToPK[mb.missingBlobs[idx].Sha256sum]
		if !ok {
			log.Errorf("could not find repo for blob: %s", mb.missingBlobs[idx].ID)
		} else {
			mb.missingBlobs[idx].Repositories = repos
		}
		jsonData, err := json.Marshal(mb.missingBlobs)
		if err != nil {
			return fmt.Errorf("could no marshal json object: %s", err)
		}
		fmt.Printf("%s", string(jsonData))
	}
	return nil
}

func (mb *BlobManager) findMissingBlobs() error {
	q := r.DB("dtr2").Table("blobs").Pluck("sha256sum", "id")
	cursor, err := q.Run(mb.session)
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
			mb.missingBlobs = append(mb.missingBlobs, blob)
		}
	}
	return nil
}

func (mb *BlobManager) makeShaMap() error {
	q := r.DB("dtr2").Table("manifests").Pluck("pk", "digest", "repository", "layers")
	cursor, err := q.Run(mb.session)
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
		repo, err := mb.getRepoFromPK(manifest.PK)
		if err != nil {
			return fmt.Errorf("failed to get repo from SHA key: %s", err)
		}
		for _, layer := range manifest.Layers {
			mb.shaToPK[layer.Digest] = append(mb.shaToPK[layer.Digest], repo)
		}
	}
	return nil
}

func (mb *BlobManager) getRepoFromPK(pk string) (string, error) {
	q := r.DB("dtr2").Table("tags").Filter(map[string]interface{}{"digestPK": pk}).Pluck("name", "repository")
	cursor, err := q.Run(mb.session)
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
