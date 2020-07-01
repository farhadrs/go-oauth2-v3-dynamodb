package go_oauth2_v3_dynamodb

import (
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"gopkg.in/oauth2.v3"
	"gopkg.in/oauth2.v3/models"
)

func insert(item interface{}, tableName string, db *dynamodb.DynamoDB) (err error) {
	var av map[string]*dynamodb.AttributeValue
	av, err = dynamodbattribute.MarshalMap(item)
	if err != nil {
		return
	}
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}
	_, err = db.PutItem(input)
	return
}

func remove(id string, tableName string, db *dynamodb.DynamoDB) (err error) {
	cId := clientId{ID: id}
	var av map[string]*dynamodb.AttributeValue
	av, err = dynamodbattribute.MarshalMap(cId)
	if err != nil {
		return
	}
	input := &dynamodb.DeleteItemInput{
		Key:       av,
		TableName: aws.String(tableName),
	}

	_, err = db.DeleteItem(input)
	return
}

func getClient(id string, tableName string, db *dynamodb.DynamoDB) (info oauth2.ClientInfo, err error) {
	var io *dynamodb.GetItemOutput
	io, err = getRaw(id, tableName, db)
	if err != nil {
		return
	}

	item := &client{}
	err = dynamodbattribute.UnmarshalMap(io.Item, &item)

	if err != nil {
		return
	}
	if item.ClientId == "" {
		err = errors.New("not found")
		return
	}
	info = &models.Client{
		ID:     item.ClientId,
		Secret: item.ClientSecret,
		UserID: item.UserID,
	}
	return
}

func getRaw(id string, tableName string, db *dynamodb.DynamoDB) (io *dynamodb.GetItemOutput, err error) {
	io, err = db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"client_id": {
				S: aws.String(id),
			},
		},
	})
	if err != nil {
		return
	}
	return
}

func getToStruct(id string, tableName string, db *dynamodb.DynamoDB, strct interface{}) (err error) {
	var io *dynamodb.GetItemOutput
	io, err = db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"client_id": {
				S: aws.String(id),
			},
		},
	})
	if err != nil {
		return
	}
	err = dynamodbattribute.UnmarshalMap(io.Item, &strct)
	return
}

func getToken(id string, tableName string, db *dynamodb.DynamoDB) (ti oauth2.TokenInfo, err error) {
	var io *dynamodb.GetItemOutput
	io, err = getRaw(id, tableName, db)
	if err != nil {
		return
	}
	var bd basicData
	err = dynamodbattribute.UnmarshalMap(io.Item, &bd)

	if err != nil {
		return
	}
	if bd.ID == "" {
		err = errors.New("not found")
		return
	}

	var tm models.Token
	err = json.Unmarshal(bd.Data, &tm)
	if err != nil {
		return
	}
	ti = &tm
	return
}

func createTable(tableName string, definitions, keySchemas map[string]string, secondIndex *[]*dynamodb.GlobalSecondaryIndex, db *dynamodb.DynamoDB) (err error) {
	var dAttributeDefinitions []*dynamodb.AttributeDefinition
	var dKeySchemas []*dynamodb.KeySchemaElement

	for key, val := range definitions {
		dAttributeDefinitions = append(dAttributeDefinitions, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(key),
			AttributeType: aws.String(val),
		})
	}
	for key, val := range keySchemas {
		dKeySchemas = append(dKeySchemas, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(key),
			KeyType:       aws.String(val),
		})
	}

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: dAttributeDefinitions,
		KeySchema:            dKeySchemas,
		BillingMode:          aws.String("PAY_PER_REQUEST"),
		TableName:            aws.String(tableName),
	}
	if secondIndex != nil {
		input.GlobalSecondaryIndexes = *secondIndex
	}
	_, err = db.CreateTable(input)
	return
}
