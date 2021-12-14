package main

import (
	"aws_example/conekta"
	"context"
	"fmt"
	"log"
	"os"
)

func main() {
	bucket := conekta.NewBucket("s3-payments-sb", "conektasb", log.New(os.Stdout, "Logger:", log.Ldate|log.Ltime|log.Lshortfile))
	delete(bucket)
}

func upload(bucket conekta.Bucket) {
	file, err := os.ReadFile("/Users/gerardomonreal/go/src/aws_example/main/testing-payments.txt")
	if err != nil {
		fmt.Print(err.Error())
		return
	}
	os.Stdout.Write(file)
	err = bucket.Upload(context.TODO(), "upload_testing.txt", "testing", file)
	if err != nil {
		fmt.Print(err.Error())
	}
}

func download(bucket conekta.Bucket) {
	bytes, err := bucket.Download(context.TODO(), "upload_testing.txt", "testing/")
	if err != nil {
		fmt.Print(err.Error())
	}
	os.Stdout.Write(bytes)
}

func delete(bucket conekta.Bucket) {
	err := bucket.Delete(context.TODO(), "upload_testing.txt", "testing/")
	if err != nil {
		fmt.Print(err.Error())
	}
}
