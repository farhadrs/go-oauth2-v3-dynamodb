package go_oauth2_v3_dynamodb

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Migration struct {
	ClientsCName string
	BasicCName   string
	AccessCName  string
	RefreshCName string
}

func (m Migration) newDefault() *Migration {
	return &Migration{
		ClientsCName: "oauth2_clients",
		BasicCName:   "oauth2_basic",
		AccessCName:  "oauth2_access",
		RefreshCName: "oauth2_refresh",
	}
}

// pass nil to nm to migrate with default table names
func (m Migration) Migrate(cfg *aws.Config, nm *Migration) (err error) {
	dnm := m.newDefault()
	if nm != nil {
		if nm.AccessCName == "" {
			nm.AccessCName = dnm.AccessCName
		}
		if nm.BasicCName == "" {
			nm.BasicCName = dnm.BasicCName
		}
		if nm.ClientsCName == "" {
			nm.ClientsCName = dnm.ClientsCName
		}
	} else {
		nm = dnm
	}

	var sess *session.Session
	sess, err = session.NewSession(cfg)
	if err != nil {
		return
	}
	svc := dynamodb.New(sess)
	secIndex := &[]*dynamodb.GlobalSecondaryIndex{
		{
			IndexName: aws.String("user_id-index"),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					KeyType:       aws.String("HASH"),
					AttributeName: aws.String("user_id"),
				},
			},
			Projection: &dynamodb.Projection{ProjectionType: aws.String("ALL")},
		},
		{
			IndexName: aws.String("valid_until-index"),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					KeyType:       aws.String("HASH"),
					AttributeName: aws.String("valid_until"),
				},
			},
			Projection: &dynamodb.Projection{ProjectionType: aws.String("ALL")},
		},
	}
	err = createTable(nm.ClientsCName, map[string]string{
		"client_id":   "S",
		"user_id":     "S",
		"valid_until": "S",
	}, map[string]string{
		"client_id": "HASH",
	}, secIndex, svc)
	if err != nil && !strings.Contains(err.Error(), "preexisting") && !strings.Contains(err.Error(), "Table already exists") {
		return
	}
	err = nil
	err = createTable(nm.BasicCName, map[string]string{
		"id": "S",
	}, map[string]string{
		"id": "HASH",
	}, nil, svc)
	if err != nil && !strings.Contains(err.Error(), "preexisting") && !strings.Contains(err.Error(), "Table already exists") {
		return
	}
	err = nil
	err = createTable(nm.AccessCName, map[string]string{
		"id": "S",
	}, map[string]string{
		"id": "HASH",
	}, nil, svc)
	if err != nil && !strings.Contains(err.Error(), "preexisting") && !strings.Contains(err.Error(), "Table already exists") {
		return
	}
	err = nil
	err = createTable(nm.RefreshCName, map[string]string{
		"id": "S",
	}, map[string]string{
		"id": "HASH",
	}, nil, svc)
	if err != nil && !strings.Contains(err.Error(), "preexisting") && !strings.Contains(err.Error(), "Table already exists") {
		return
	}
	err = nil
	return
}
