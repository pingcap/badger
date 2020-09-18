package s3util

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Client struct {
	endPoint  string
	bucket    string
	keyID     string
	secretKey string
	cli       *s3.S3
}

func NewS3Client(endPoint, bucket, keyID, secretKey string) *S3Client {
	s3c := &S3Client{
		endPoint:  endPoint,
		bucket:    bucket,
		keyID:     keyID,
		secretKey: secretKey,
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{Transport: tr}
	cred := credentials.NewStaticCredentials(keyID, secretKey, "")
	sess := session.Must(session.NewSession(aws.NewConfig().WithEndpoint(endPoint).WithS3ForcePathStyle(true).WithCredentials(cred).WithHTTPClient(client)))
	s3c.cli = s3.New(sess)
	return s3c
}

func (c *S3Client) Get(key string, offset, length int64) ([]byte, error) {
	input := &s3.GetObjectInput{}
	input.Bucket = &c.bucket
	input.Key = &key
	rangeStr := fmt.Sprintf("bytes:%d-%d", offset, offset+length)
	input.Range =&rangeStr
	out, err := c.cli.GetObject(input)
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	result := make([]byte, length)
	_, err = io.ReadFull(out.Body, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *S3Client) Put(key string, data []byte) error {
	input := &s3.PutObjectInput{}
	input.SetContentLength(int64(len(data)))
	input.Bucket = &c.bucket
	input.Key = &key
	input.Body = bytes.NewReader(data)
	_, err := c.cli.PutObject(input)
	return err
}

