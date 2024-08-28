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
	orgName      string
	repoName     string
	blobs        []Blob
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
	PK           string           `gorethink:"pk"`
	Digest       string           `gorethink:"digest"`
	Repository   string           `gorethink:"repository"`
	Layers       []*ManifestLayer `gorethink:"layers"`
	ConfigDigest string           `gorethink:"configDigest"`
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
		replicaID: cliContext.String("replica_id"),
		orgName:   cliContext.String("org"),
		repoName:  cliContext.String("repo"),
		shaToPK:   make(map[string][]string),
		blobs:     []Blob{},
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
			log.Infof("could not find repo for blob: %s sha: %s", bm.missingBlobs[idx].ID, bm.missingBlobs[idx].Sha256sum)
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

func (bm *BlobManager) getAllBlobs() error {
	q := r.DB("dtr2").Table("blobs").Pluck("sha256sum", "id")
	cursor, err := q.Run(bm.session)
	if err != nil {
		return fmt.Errorf("failed to query blobs table: %s", err)
	}
	defer cursor.Close()
	err = cursor.All(&bm.blobs)
	if err != nil {
		return fmt.Errorf("failed to read blobs table: %s", err)
	}
	return nil
}

func (bm *BlobManager) getBlobsByOrgRepo() error {
	q := r.DB("dtr2").Table("blob_links").Filter(
		func(term r.Term) r.Term {
			if bm.orgName != "" && bm.repoName != "" {
				return term.Field("namespace").Eq(bm.orgName).And(term.Field("repository").Eq(bm.repoName))
			} else if bm.orgName != "" {
				return term.Field("namespace").Eq(bm.orgName)
			}
			return term.Field("repository").Eq(bm.repoName)
		}).EqJoin("digest", r.DB("dtr2").Table("blobs")).Zip().Pluck("sha256sum", "id")
	cursor, err := q.Run(bm.session)
	if err != nil {
		return fmt.Errorf("failed to query blob_links table: %s", err)
	}
	defer cursor.Close()
	err = cursor.All(&bm.blobs)
	if err != nil {
		return fmt.Errorf("failed to read blobs table: %s", err)
	}
	return nil
}

func (bm *BlobManager) findMissingBlobs() error {
	log.Error("Reading blobs table.")
	var err error
	if bm.orgName != "" || bm.repoName != "" {
		err = bm.getBlobsByOrgRepo()
	} else {
		err = bm.getAllBlobs()
	}
	if err != nil {
		return fmt.Errorf("failed to read blobs table: %s", err)
	}

	blobCount := len(bm.blobs)
	blobIncrement := max(blobCount/10, 1)
	log.Error("Scanning file system for blobs.")
	for idx, blob := range bm.blobs {
		if idx%blobIncrement == 0 {
			log.Errorf("Checking if a blob is missing: %d of %d", idx+1, blobCount)
		}
		if len(blob.ID) > 1 {
			blobPath := fmt.Sprintf("/storage/docker/registry/v2/blobs/id/%s/%s/data", blob.ID[0:2], blob.ID)
			_, err := os.Stat(blobPath)
			if err != nil {
				bm.missingBlobs = append(bm.missingBlobs, blob)
			}
		}
	}
	log.Errorf("Missing blobs found: %d", len(bm.missingBlobs))
	return nil
}

func (bm *BlobManager) makeShaMap() error {
	log.Error("Reading manifests table.")
	q := r.DB("dtr2").Table("manifests").Pluck("pk", "digest", "repository", "layers", "configDigest")
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
	log.Error("Processing manifests.")
	for _, manifest := range manifests {
		repo, err := bm.getRepoFromPK(manifest.PK)
		if err != nil {
			return fmt.Errorf("failed to get repo from SHA key: %s", err)
		}
		if manifest.ConfigDigest != "" {
			bm.shaToPK[manifest.ConfigDigest] = append(bm.shaToPK[manifest.ConfigDigest], repo)
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
		log.Warnf("Failed to find tag for PK: %s error: %s", pk, err)
		return "", nil
	}
	return fmt.Sprintf("%s:%s", repo.Repository, repo.Name), nil
}
