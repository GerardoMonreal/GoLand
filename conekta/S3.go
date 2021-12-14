package conekta

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io/ioutil"
	"log"
	"strings"
)

type bucket struct {
	Name    string
	Profile string
	client  *s3.Client
	log     *log.Logger
}

type Bucket interface {
	Download(ctx context.Context, file string, path string) ([]byte, error)
	Upload(ctx context.Context, name string, path string, file []byte) error
	Delete(ctx context.Context, file string, path string) error
}

func NewBucket(name string, profile string, logger *log.Logger) Bucket {
	return &bucket{
		Name:    name,
		Profile: profile,
		client:  nil,
		log:     logger,
	}
}

func (b *bucket) Download(ctx context.Context, file string, path string) ([]byte, error) {
	err := b.initS3Client(ctx)
	if err != nil {
		b.log.Fatal(ctx, err.Error())
		return nil, err
	}
	b.log.Println(ctx, "Getting file")
	file = b.getFilePath(file, path)
	b.log.Println(ctx, fmt.Sprintf("Getting file: %v", file))
	object, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.Name),
		Key:    aws.String(file),
	})
	if err != nil {
		b.log.Println(ctx, err.Error())
		return nil, err
	}
	b.log.Println(ctx, "File downloaded successfully")
	b.log.Println(ctx, "Reading file")
	fileBytes, err := ioutil.ReadAll(object.Body)
	if err != nil {
		return nil, err
	}
	return fileBytes, nil
}

func (b *bucket) Upload(ctx context.Context, file string, path string, fileBytes []byte) error {
	err := b.initS3Client(ctx)
	if err != nil {
		b.log.Fatal(ctx, err.Error())
		return err
	}

	file = b.getFilePath(file, path)

	fileReader := bytes.NewReader(fileBytes)

	object, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.Name),
		Key:    aws.String(file),
		Body:   fileReader,
	})
	if err != nil {
		return err
	}
	b.log.Println(object.VersionId)
	return nil
}

func (b *bucket) Delete(ctx context.Context, file string, path string) error {
	err := b.initS3Client(ctx)
	if err != nil {
		b.log.Fatal(ctx, err.Error())
		return err
	}

	file = b.getFilePath(file, path)

	_, err = b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.Name),
		Key:    aws.String(file),
	})
	if err != nil {
		return err
	}
	b.log.Println("delete was succeeded")
	return nil
}

func (b *bucket) initS3Client(ctx context.Context) error {
	if b.client != nil {
		return nil
	}
	b.log.Println(ctx, fmt.Sprintf("configure s3 bucket: %v with profile: %v", b.Name, b.Profile))
	cfg, err := config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile(b.Profile))
	if err != nil {
		return err
	}
	b.client = s3.NewFromConfig(cfg)
	return nil
}

func (b *bucket) getFilePath(file string, path string) string {
	if !isEmptyOrWitheSpace(path) {
		path = strings.TrimSuffix(path, "/")
		return fmt.Sprintf("%v/%v", path, file)
	}
	return file
}

func isEmptyOrWitheSpace(s string) bool {
	return s == "" || len(s) == 0 || strings.TrimSpace(s) == ""
}
