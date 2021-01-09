package main

import (
	"fmt"
	"strings"

	"elastic/destream"

	"github.com/olivere/elastic"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var awsSession = session.Must(session.NewSession(&aws.Config{}))
var dynamoSvc = dynamodb.New(awsSession)
var esclient = new(destream.Elasticsearch)

func handler(e events.DynamoDBEvent) error {
	var item map[string]events.DynamoDBAttributeValue
	fmt.Println("Beginning ES Sync")
	for _, v := range e.Records {
		switch v.EventName {
		case "INSERT":
			fallthrough
		case "MODIFY":
			tableName := strings.Split(v.EventSourceArn, "/")[1]
			item = v.Change.NewImage
			details, err := (&destream.DynamoDetails{
				DynamoDBAPI: dynamoSvc,
			}).Get(tableName)

			if err != nil {
				return err
			}
			fmt.Println("Before ES Sync")
			svc, err := elastic.NewClient(
				elastic.SetSniff(false),
				elastic.SetURL("https://search-dynamodbto-elasticsearch-kto6hxg5b3t2um4pvypqggqm4i.ap-south-1.es.amazonaws.com/"),
			)
			if err != nil {
				return err
			}
			fmt.Println("Before ES Sync")
			esclient.Client = svc
			resp, err := esclient.Update(details, item)
			if err != nil {
				return err
			}
			fmt.Println(resp.Result)
		default:
		}
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
