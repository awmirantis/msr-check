package missingblobs

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

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
	tagName      string
	blobs        []Blob
	missingBlobs []Blob
	verbose      bool
	json         bool
	logFile      *os.File
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
	MediaType    string `gorethink:"mediaType"`
	Repositories []string
}

type Tags struct {
	Repository string `gorethink:"repository"`
	Name       string `gorethink:"name"`
	Digest     string `gorethink:"digest"`
}

func Run(cliContext *cli.Context) error {
	replicaID := cliContext.String("replica_id")
	outputFile := cliContext.String("o")
	session, err := util.GetSession(replicaID)
	if err != nil {
		return fmt.Errorf("could not get connect to rethinkdb: %s", err)
	}
	bm := BlobManager{
		session:      session,
		replicaID:    cliContext.String("replica_id"),
		orgName:      cliContext.String("org"),
		repoName:     cliContext.String("repo"),
		tagName:      cliContext.String("tag"),
		shaToPK:      make(map[string][]string),
		blobs:        []Blob{},
		missingBlobs: []Blob{},
		verbose:      cliContext.Bool("v"),
		json:         cliContext.Bool("json"),
	}
	if bm.verbose {
		bm.logFile, err = os.OpenFile("/out/msr-check.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			log.Error("Could not create log file.")
		} else {
			defer bm.logFile.Close()
		}
	}

	if bm.tagName != "" {
		if bm.repoName == "" || bm.orgName == "" {
			return fmt.Errorf("when specifying a tag the repo and org must also be specified")
		}
		return bm.checkByTag()
	}
	err = bm.findMissingBlobs()
	if err != nil {
		return fmt.Errorf("failed to retrieve missing blobs: %s", err)
	}
	if len(bm.missingBlobs) == 0 {
		return nil
	}

	err = bm.makeShaMap()
	if err != nil {
		return fmt.Errorf("failed to make SHA mapping table: %s", err)
	}
	for idx := range bm.missingBlobs {
		repos, ok := bm.shaToPK[bm.missingBlobs[idx].Sha256sum]
		if !ok {
			log.Errorf("could not find repo for blob: %s sha: %s mediaType: %s", bm.missingBlobs[idx].ID, bm.missingBlobs[idx].Sha256sum, bm.missingBlobs[idx].MediaType)
		} else {
			bm.missingBlobs[idx].Repositories = repos
		}
	}

	if bm.json {
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
	} else {
		file := os.Stdout
		if outputFile != "" {
			filePath := fmt.Sprintf("/out/%s", outputFile)
			file, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
			if err != nil {
				return fmt.Errorf("failed to write to file: '%s' err: %s", filePath, err)
			}
			defer file.Close()
		}
		var repos []string
		for _, blob := range bm.missingBlobs {
			repos = append(repos, blob.Repositories...)
		}
		slices.Sort(repos)
		repos = slices.Compact(repos)
		for _, repo := range repos {
			file.WriteString(repo + "\n")
		}
	}
	return nil
}

func (bm *BlobManager) getAllBlobs() error {
	q := r.DB("dtr2").Table("blobs").Pluck("sha256sum", "id", "mediaType")
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
	filter := make(map[string]interface{})
	if bm.orgName != "" {
		filter["namespace"] = bm.orgName
	}
	if bm.repoName != "" {
		filter["repository"] = bm.repoName
	}
	q := r.DB("dtr2").Table("blob_links").Filter(filter).
		EqJoin("digest", r.DB("dtr2").Table("blobs"), r.EqJoinOpts{Index: "sha256sum"}).
		Zip().
		Pluck("sha256sum", "id", "mediaType").
		Distinct()

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
	log.Errorf("Reading blobs table.")
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
	log.Errorf("Scanning file system for blobs (%d).", blobCount)
	for idx, blob := range bm.blobs {
		if idx%blobIncrement == 0 && !bm.verbose {
			log.Errorf("Checking if a blob is missing: %d of %d", idx+1, blobCount)
		}
		if len(blob.ID) > 1 {
			blobPath := fmt.Sprintf("/storage/docker/registry/v2/blobs/id/%s/%s/data", blob.ID[0:2], blob.ID)
			stat, err := os.Stat(blobPath)
			if err != nil || stat.Size() == 0 {
				bm.log("Blob %s not found at %s error: %v\n", blob.ID, blobPath, err)
				bm.missingBlobs = append(bm.missingBlobs, blob)
			} else {
				bm.log("Blob %s found at %s\n", blob.ID, blobPath)
			}
		}
	}
	log.Errorf("Missing blobs found: %d", len(bm.missingBlobs))
	return nil
}

func (bm *BlobManager) makeShaMap() error {
	log.Errorf("Reading manifests table.")
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
	log.Errorf("Processing manifests.")
	for _, manifest := range manifests {
		if manifest.Repository != "" {
			sha := strings.Split(manifest.PK, "@")
			manifest.PK = fmt.Sprintf("%s@%s", manifest.Repository, sha[1])
		}
		repo, err := bm.getRepoFromPK(manifest.PK)
		if err != nil {
			bm.log("failed to get repo from SHA key: %s\n", err)
			continue
		}
		if manifest.ConfigDigest != "" {
			bm.shaToPK[manifest.ConfigDigest] = append(bm.shaToPK[manifest.ConfigDigest], repo)
		}
		for _, layer := range manifest.Layers {
			if layer != nil && layer.Digest != "" {
				bm.shaToPK[layer.Digest] = append(bm.shaToPK[layer.Digest], repo)
			}
		}
	}
	bm.log("Digest count: %d\n", len(bm.shaToPK))
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
		bm.log("Failed to find tag for PK: %s error: %s\n", pk, err)
		return "", nil
	}
	return fmt.Sprintf("%s:%s", repo.Repository, repo.Name), nil
}

func (bm *BlobManager) checkByTag() error {
	image := fmt.Sprintf("%s/%s:%s", bm.orgName, bm.repoName, bm.tagName)
	q := r.DB("dtr2").Table("tags").Filter(map[string]interface{}{"pk": image}).Pluck("repository", "name", "digest")
	cursor, err := q.Run(bm.session)
	if err != nil {
		bm.log("could not read tags for image: %v\n", err)
		return fmt.Errorf("could not read tags for image: %v", err)
	}
	tags := Tags{}
	err = cursor.One(&tags)
	if err != nil {
		bm.log("could not get tags for image: %v\n", err)
		return fmt.Errorf("could not get tags for image: %v", err)
	}
	bm.log("Tags: %+v\n", tags)
	q = r.DB("dtr2").Table("manifests").Filter(map[string]interface{}{"digest": tags.Digest})
	cursor, err = q.Run(bm.session)
	if err != nil {
		bm.log("could get digest for image: %v\n", err)
		return fmt.Errorf("could get digest for image: %v", err)
	}
	manifest := Manifest{}
	err = cursor.One(&manifest)
	if err != nil {
		bm.log("could not get layers for image: %v\n", err)
		return fmt.Errorf("could not get layers for image: %v", err)
	}
	bm.log("Manifest: %+v\n", manifest)
	var ids []string
	for cnt, layer := range manifest.Layers {
		q = r.DB("dtr2").Table("blobs").Filter(map[string]interface{}{"sha256sum": layer.Digest}).Pluck("id")
		cursor, err = q.Run(bm.session)
		if err != nil {
			bm.log("could get blob %s for layer: %v\n", layer.Digest, err)
			return fmt.Errorf("could get blob for layer: %v", err)
		}
		blobs := Blob{}
		err = cursor.One(&blobs)
		if err != nil {
			bm.log("could not get blob id for layer: %v\n", err)
			return fmt.Errorf("could not get blob id for layer: %v", err)
		}
		bm.log("Layer %d blob: %+v", cnt, blobs)
		ids = append(ids, blobs.ID)
	}

	if manifest.ConfigDigest != "" {
		// TODO: refactor into a function
		q = r.DB("dtr2").Table("blobs").Filter(map[string]interface{}{"sha256sum": manifest.ConfigDigest}).Pluck("id")
		cursor, err = q.Run(bm.session)
		if err != nil {
			bm.log("could get blob for manifest config: %v\n", err)
			return fmt.Errorf("could get blob for manifest config: %v", err)
		}
		blobs := Blob{}
		err = cursor.One(&blobs)
		if err != nil {
			return fmt.Errorf("could not get blob id for manifest config: %v", err)
		}
		bm.log("Mnaifest blob: %+v", blobs)
		ids = append(ids, blobs.ID)
	}
	bm.log("ID list: %+v", ids)

	missingBlobCnt := 0
	for _, id := range ids {
		blobPath := fmt.Sprintf("/storage/docker/registry/v2/blobs/id/%s/%s/data", id[0:2], id)
		stat, err := os.Stat(blobPath)
		if err != nil {
			missingBlobCnt++
			bm.log("Blob %s not found at %s error: %v\n", id, blobPath, err)
			log.Errorf("Blob %s not found at %s error: %v", id, blobPath, err)
		} else {
			bm.log("Blob %s at %s stat: %+v\n", id, blobPath, stat)
			log.Errorf("Blob %s found at %s", id, blobPath)
		}
	}
	log.Errorf("Missing blob count: %d", missingBlobCnt)
	return nil
}

func (bm *BlobManager) log(pattern string, args ...interface{}) {
	if bm.logFile != nil {
		bm.logFile.Write([]byte(fmt.Sprintf(pattern, args...)))
	}
}
