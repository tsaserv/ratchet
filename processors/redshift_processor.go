package processors

import (
	"database/sql"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
)

type redshiftManifest struct {
	Entries []redshiftManifestEntry `json:"entries"`
}

type redshiftManifestEntry struct {
	URL       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

// RedshiftProcessor gets data into a Redshift table by first uploading data batches to S3.
// Once all data is uploaded to S3, the appropriate "COPY" command is executed against the
// database to import the data files.
//
// This processor is not set up to do any fancy merging; rather, it writes every row received
// to the table defined. An ideal use case is writing data to a temporary table that is later
// merged into your production dataset.
type RedshiftProcessor struct {
	awsID           string
	awsSecret       string
	awsRegion       string
	bucket          string
	config          *aws.Config
	db              *sql.DB
	prefix          string
	tableName       string
	manifestEntries []redshiftManifestEntry
	data            []string
	BatchSize       int
	Compress        bool
	manifestPath    string
}

// NewRedshiftProcessor returns a reference to a new Redshift Processor
func NewRedshiftProcessor(db *sql.DB, tableName, awsID, awsSecret, awsRegion, bucket, prefix string) *RedshiftProcessor {
	p := RedshiftProcessor{
		awsID:     awsID,
		awsSecret: awsSecret,
		awsRegion: awsRegion,
		bucket:    bucket,
		db:        db,
		prefix:    prefix,
		tableName: tableName,
		BatchSize: 1000,
		Compress:  true,
	}

	creds := credentials.NewStaticCredentials(awsID, awsSecret, "")
	p.config = aws.NewConfig().WithRegion(awsRegion).WithDisableSSL(true).WithCredentials(creds)

	return &p
}

// ProcessData stores incoming data in a local var. Once enough data has been received (as defined
// by r.BatchSize), it will write a file out to S3 and reset the local var
func (r *RedshiftProcessor) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	var objects []interface{}
	err := data.ParseJSON(d, &objects)
	util.KillPipelineIfErr(err, killChan)

	for _, obj := range objects {
		dd, err := data.NewJSON(obj)
		util.KillPipelineIfErr(err, killChan)
		r.data = append(r.data, string(dd))

		// Flush the data if we've hit the threshold of records
		if r.BatchSize > 0 && len(r.data) >= r.BatchSize {
			r.flushFiles(killChan)
		}
	}
}

// Finish writes any remaining records to a file on S3, creates the manifest file, and then
// kicks off the query to import the S3 files into the Redshift table
func (r *RedshiftProcessor) Finish(outputChan chan data.JSON, killChan chan error) {
	r.flushFiles(killChan)
	r.createManifest(killChan)
	r.copyToRedshift(killChan)
}

func (r *RedshiftProcessor) flushFiles(killChan chan error) {
	fileName := fmt.Sprintf("%vfile.%v", r.prefix, len(r.manifestEntries))
	_, err := util.WriteS3Object(r.data, r.config, r.bucket, fileName, "\n", r.Compress)
	util.KillPipelineIfErr(err, killChan)

	if r.Compress {
		fileName += ".gz"
	}

	entry := redshiftManifestEntry{
		URL:       fmt.Sprintf("s3://%v/%v", r.bucket, fileName),
		Mandatory: true,
	}
	r.manifestEntries = append(r.manifestEntries, entry)

	r.data = nil
}

func (r *RedshiftProcessor) createManifest(killChan chan error) {
	manifest := redshiftManifest{Entries: r.manifestEntries}
	manifestData, err := data.NewJSON(manifest)
	util.KillPipelineIfErr(err, killChan)

	dd := []string{string(manifestData)}
	r.manifestPath = fmt.Sprintf("%vfile.manifest", r.prefix)
	_, err = util.WriteS3Object(dd, r.config, r.bucket, r.manifestPath, "\n", false)
	util.KillPipelineIfErr(err, killChan)
}

func (r *RedshiftProcessor) copyToRedshift(killChan chan error) {
	err := util.ExecuteSQLQuery(r.db, r.copyQuery())
	util.KillPipelineIfErr(err, killChan)
}

func (r *RedshiftProcessor) copyQuery() string {
	compression := ""
	if r.Compress {
		compression = "GZIP"
	}

	query := fmt.Sprintf(`
                COPY %v
                FROM 's3://%v/%v'
                REGION '%v'
                CREDENTIALS 'aws_access_key_id=%v;aws_secret_access_key=%v'
                MANIFEST
                JSON 'auto'
                %v
        `, r.tableName, r.bucket, r.manifestPath, r.awsRegion, r.awsID, r.awsSecret, compression)

	return query
}

func (r *RedshiftProcessor) String() string {
	return "RedshiftProcessor"
}
