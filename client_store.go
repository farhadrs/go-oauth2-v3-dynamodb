package go_oauth2_v3_dynamodb

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"gopkg.in/oauth2.v3"
)

// ClientConfig client configuration parameters
type ClientConfig struct {
	// store clients data collection name(The default is oauth2_clients)
	ClientsCName string
}

// NewDefaultClientConfig create a default client configuration
func NewDefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		ClientsCName: "oauth2_clients",
	}
}

// NewClientStore create a client store instance based on mongodb
func NewClientStore(cfg *aws.Config, ccfgs ...*ClientConfig) *ClientStore {
	sess := session.Must(session.NewSession(cfg))
	svc := dynamodb.New(sess)
	return NewClientStoreWithSession(sess, svc, ccfgs...)
}

// NewClientStoreWithSession create a client store instance based on mongodb
func NewClientStoreWithSession(session *session.Session, db *dynamodb.DynamoDB, ccfgs ...*ClientConfig) *ClientStore {
	cs := &ClientStore{
		session: session,
		ccfg:    NewDefaultClientConfig(),
		db:      db,
	}
	if len(ccfgs) > 0 {
		cs.ccfg = ccfgs[0]
	}

	return cs
}

// ClientStore MongoDB storage for OAuth 2.0
type ClientStore struct {
	ccfg    *ClientConfig
	session *session.Session
	db      *dynamodb.DynamoDB
}

// Set set client information
func (cs *ClientStore) Set(clientId, clientSecret, userId, userType, redirectUri, name string, isConfidential bool, createTime, updateTime, validUntil time.Time) (err error) {
	entity := &client{
		ClientId:       clientId,
		ClientSecret:   clientSecret,
		RedirectUri:    redirectUri,
		Name:           name,
		IsConfidential: isConfidential,
		UserID:         userId,
		UserType:       userType,
		ValidUntil:     validUntil,
		CreateTime:     createTime,
		UpdateTime:     updateTime,
	}
	err = insert(entity, cs.ccfg.ClientsCName, cs.db)
	return
}

// GetByID according to the ID for the client information
func (cs *ClientStore) GetByID(id string) (info oauth2.ClientInfo, err error) {
	info, err = getClient(id, cs.ccfg.ClientsCName, cs.db)
	return
}

// RemoveByID use the client id to delete the client information
func (cs *ClientStore) RemoveByID(id string) (err error) {
	err = remove(id, cs.ccfg.ClientsCName, cs.db)
	return
}

type clientId struct {
	ID string `json:"id"`
}

type client struct {
	ClientId       string    `json:"client_id"`
	ClientSecret   string    `json:"client_secret"`
	IsConfidential bool      `json:"is_confidential"`
	RedirectUri    string    `json:"redirect_uri"`
	Name           string    `json:"name"`
	UserID         string    `json:"user_id"`
	UserType       string    `json:"user_type"`
	ValidUntil     time.Time `json:"valid_until"`
	CreateTime     time.Time `json:"create_time"`
	UpdateTime     time.Time `json:"update_time"`
}
