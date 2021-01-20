package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Item has fields for the DynamoDB
type Item struct {
	//db class map
	FileKey    string `json:"FileKey" dynamodbav:"FileKey"`
	Month      string `json:"Month" dynamodbav:"Month"`
	Cupcake    string `json:"Cupcake" dynamodbav:"Cupcake"`
	Updatetime string `json:"updatetime" dynamodbav:"updatetime"`
}

//Response type
type Response struct {
	Response []Item `json:"response"`
}

func main() {
	lambda.Start(Handler)
}

//Sessioninit function
func Sessioninit() (*dynamodb.DynamoDB, error) {
	region := os.Getenv("ap-south-1")
	// Initialize a session
	if session, err := session.NewSession(&aws.Config{
		Region: &region,
	}); err != nil {
		fmt.Println(fmt.Sprintf("Failed to initialize a session to AWS: %s", err.Error()))
		return nil, err
		// Create DynamoDB client
	} else {
		return dynamodb.New(session), nil
	}
}

//Handler function to get event
func Handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var (
		err       error
		svc       *dynamodb.DynamoDB
		tableName = aws.String("MyDataFromS3")
		items     []Item
	)
	// Intitalize a client session to dynamodb

	svc, err = Sessioninit()
	if err != nil {
		return events.APIGatewayProxyResponse{Body: "Session init erro " + err.Error(), StatusCode: 500}, nil
	}
	params := &dynamodb.ScanInput{
		TableName: tableName,
	}
	result, err := svc.Scan(params)
	if err != nil {
		// Status Bad Request
		myHeader := make(map[string]string)
		myHeader["Access-Control-Allow-Origin"] = "*"
		myHeader["Content-Type"] = "application/json"
		return events.APIGatewayProxyResponse{
			Headers:    myHeader,
			Body:       "DynamoDB Query API call failed: " + err.Error(),
			StatusCode: 500,
		}, nil
	}

	for _, i := range result.Items {
		userdata := Item{}
		if err := dynamodbattribute.UnmarshalMap(i, &userdata); err != nil {
			myHeader := make(map[string]string)
			myHeader["Access-Control-Allow-Origin"] = "*"
			myHeader["Content-Type"] = "application/json"
			return events.APIGatewayProxyResponse{
				Headers:    myHeader,
				Body:       "Got error unmarshalling:" + err.Error(),
				StatusCode: 500,
			}, nil
		}
		//fmt.Println(result)
		items = append(items, userdata)
	}

	body, _ := json.Marshal(&Response{
		Response: items,
	})

	//fmt.Println(result)
	myHeader := make(map[string]string)
	myHeader["Access-Control-Allow-Origin"] = "*"
	myHeader["Content-Type"] = "application/json"
	return events.APIGatewayProxyResponse{
		Headers:    myHeader,
		Body:       string(body),
		StatusCode: 200,
	}, nil

}
