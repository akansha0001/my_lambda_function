package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func main() {
	lambda.Start(handler)
}

func handler() {
	fmt.Println("in handler")
	updatessinglevalue()
}

func updatessinglevalue() {

	type MyDataFromS3Key struct {
		FileKey string
	}

	type MyDataFromS3KeyUpdate struct {
		Updatetime string `json:":time"`
	}

	config := &aws.Config{
		Region: aws.String("ap-south-1"),
	}

	sess := session.Must(session.NewSession(config))

	svc := dynamodb.New(sess)

	// input := &dynamodb.DescribeTableInput{
	// 	TableName: aws.String("MyDataFromS3"),
	// }

	// result, err := svc.DescribeTable(input)
	// if err != nil {
	// 	if aerr, ok := err.(awserr.Error); ok {
	// 		switch aerr.Code() {
	// 		case dynamodb.ErrCodeResourceNotFoundException:
	// 			fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
	// 		case dynamodb.ErrCodeInternalServerError:
	// 			fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
	// 		default:
	// 			fmt.Println(aerr.Error())
	// 		}
	// 	} else {
	// 		// Print the error, cast err to awserr.Error to get the Code and
	// 		// Message from an error.
	// 		fmt.Println(err.Error())
	// 	}
	// 	return
	// }

	// noofitem := int(*result.Table.ItemCount)
	// print(noofitem)

	noofitem := 204
	arr := make([]int, 100)
	var r int
	for r = 0; r < 100; r++ {
		arr[r] = 1 + rand.Intn(noofitem)
	}

	for r := 0; r < len(arr); r++ {
		key, err := dynamodbattribute.MarshalMap(MyDataFromS3Key{
			FileKey: strconv.Itoa(arr[r]),
		})

		if err != nil {
			fmt.Println(err.Error())
			return
		}
		t := time.Now().String()
		updatevalue, err := dynamodbattribute.MarshalMap(MyDataFromS3KeyUpdate{
			Updatetime: t,
		})
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		input := &dynamodb.UpdateItemInput{
			Key:                       key,
			TableName:                 aws.String("MyDataFromS3"),
			UpdateExpression:          aws.String("set updatetime= :time"),
			ExpressionAttributeValues: updatevalue,
			ReturnValues:              aws.String("UPDATED_NEW"),
		}

		_, err = svc.UpdateItem(input)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("Successfully updated record no %v \n", arr[r])
	}

}
