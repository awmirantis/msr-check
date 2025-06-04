# msr-check

## Building
From the command prompt:
```bash
./build.sh
```
This will build the image `awmirantis/msr-check`.

## Finding unknown blobs and the repos that are afilliated to them
The `msr-check` command `missing_blobs` will find the blobs that are no longer in the storage system and return the repositories that use that blob.
### Find the replica ID
This command requires the REPLICA_ID of rethinkdb and also the storage location of the blobs.
The replica ID can be retrieved with the following command:
```bash
export REPLICA_ID=$(docker ps --filter name=dtr-rethinkdb --format "{{ .Names }}" | cut -d"-" -f3)
```
### Find the location the blobs are stored
The storage location is typically at `/var/lib/docker/volumes/dtr-registry-54170f2efd1a/_data` but can vary depending on the customers configuration this will be passed as a volume to the msr-check container
### Execute the missing_blobs command
```bash
docker run --rm --net dtr-ol -v <PATH_TO_STORAGE>:/storage -v dtr-ca-$REPLICA_ID:/ca awmirantis/msr-check missing_blobs  --replica_id  $REPLICA_ID
```
#### Optional parameters
- `-o` Specify the filename to output the results to.  This requires adding the volume `/out`:
- `--org` Specify the org (namespace) of the repo to check for missing blobs
- `--repo` Specify the repository name to check for missing blobs
- `-v` Verbose logging, warning: execessive logging if scanning entire repo.
```bash
docker run --rm --net dtr-ol -v ~:/out -v <PATH_TO_STORAGE>:/storage -v dtr-ca-$REPLICA_ID:/ca awmirantis/msr-check missing_blobs  --replica_id  $REPLICA_ID -o foo.json --org mirantis --repo msr
