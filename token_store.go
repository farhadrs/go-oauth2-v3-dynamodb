package go_oauth2_v3_dynamodb

import (
	"encoding/json"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/aws/aws-sdk-go/aws"
	"gopkg.in/oauth2.v3"
)

// TokenConfig token configuration parameters
type TokenConfig struct {
	// store txn collection name(The default is oauth2)
	TxnCName string
	// store token based data collection name(The default is oauth2_basic)
	BasicCName string
	// store access token data collection name(The default is oauth2_access)
	AccessCName string
	// store refresh token data collection name(The default is oauth2_refresh)
	RefreshCName string
}

// NewDefaultTokenConfig create a default token configuration
func NewDefaultTokenConfig() *TokenConfig {
	return &TokenConfig{
		TxnCName:     "oauth2_txn",
		BasicCName:   "oauth2_basic",
		AccessCName:  "oauth2_access",
		RefreshCName: "oauth2_refresh",
	}
}

// NewTokenStore create a token store instance based on mongodb
func NewTokenStore(cfg *aws.Config, tcfgs ...*TokenConfig) (store *TokenStore) {
	sess := session.Must(session.NewSession(cfg))
	svc := dynamodb.New(sess)

	return NewTokenStoreWithSession(sess, svc, tcfgs...)
}

// NewTokenStoreWithSession create a token store instance based on mongodb
func NewTokenStoreWithSession(session *session.Session, db *dynamodb.DynamoDB, tcfgs ...*TokenConfig) (store *TokenStore) {
	ts := &TokenStore{
		db:      db,
		session: session,
		tcfg:    NewDefaultTokenConfig(),
	}
	if len(tcfgs) > 0 {
		ts.tcfg = tcfgs[0]
	}

	store = ts
	return
}

// TokenStore MongoDB storage for OAuth 2.0
type TokenStore struct {
	tcfg    *TokenConfig
	db      *dynamodb.DynamoDB
	session *session.Session
}

// Create create and store the new token information
func (ts *TokenStore) Create(info oauth2.TokenInfo) (err error) {
	var jv []byte
	jv, err = json.Marshal(info)
	if err != nil {
		return
	}

	if code := info.GetCode(); code != "" {
		err = insert(basicData{
			ID:        code,
			Data:      jv,
			ExpiredAt: info.GetCodeCreateAt().Add(info.GetCodeExpiresIn()),
		}, ts.tcfg.BasicCName, ts.db)
		return
	}

	aexp := info.GetAccessCreateAt().Add(info.GetAccessExpiresIn())
	rexp := aexp
	if refresh := info.GetRefresh(); refresh != "" {
		rexp = info.GetRefreshCreateAt().Add(info.GetRefreshExpiresIn())
		if aexp.Second() > rexp.Second() {
			aexp = rexp
		}
	}

	id := primitive.NewObjectID().Hex()

	var basicAv, accessAv, refreshAv map[string]*dynamodb.AttributeValue
	var ops []*dynamodb.TransactWriteItem

	oauthBasic := basicData{
		ID:        id,
		Data:      jv,
		ExpiredAt: rexp,
	}

	basicAv, err = dynamodbattribute.MarshalMap(oauthBasic)
	if err != nil {
		return
	}

	oauthAccess := tokenData{
		ID:        info.GetAccess(),
		BasicID:   id,
		ExpiredAt: aexp,
	}

	accessAv, err = dynamodbattribute.MarshalMap(oauthAccess)
	if err != nil {
		return
	}

	ops = []*dynamodb.TransactWriteItem{
		{
			Put: &dynamodb.Put{
				TableName: aws.String(ts.tcfg.BasicCName),
				Item:      basicAv,
			},
		},
		{
			Put: &dynamodb.Put{
				TableName: aws.String(ts.tcfg.AccessCName),
				Item:      accessAv,
			},
		},
	}

	if refresh := info.GetRefresh(); refresh != "" {
		oauthRefresh := tokenData{
			ID:        refresh,
			BasicID:   id,
			ExpiredAt: rexp,
		}
		refreshAv, err = dynamodbattribute.MarshalMap(oauthRefresh)
		if err != nil {
			return
		}
		ops = append(ops, &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				TableName: aws.String(ts.tcfg.RefreshCName),
				Item:      refreshAv,
			},
		})
	}
	_, err = ts.db.TransactWriteItems(&dynamodb.TransactWriteItemsInput{
		TransactItems: ops,
	})
	return
}

// RemoveByCode use the authorization code to delete the token information
func (ts *TokenStore) RemoveByCode(code string) (err error) {
	err = remove(code, ts.tcfg.BasicCName, ts.db)
	return
}

// RemoveByAccess use the access token to delete the token information
func (ts *TokenStore) RemoveByAccess(access string) (err error) {
	err = remove(access, ts.tcfg.AccessCName, ts.db)
	return
}

// RemoveByRefresh use the refresh token to delete the token information
func (ts *TokenStore) RemoveByRefresh(refresh string) (err error) {
	err = remove(refresh, ts.tcfg.RefreshCName, ts.db)
	return
}

func (ts *TokenStore) getData(basicID string) (ti oauth2.TokenInfo, err error) {
	ti, err = getToken(basicID, ts.tcfg.BasicCName, ts.db)
	return
}

func (ts *TokenStore) getBasicID(cname, token string) (basicID string, err error) {
	var td *tokenData
	err = getToStruct(token, cname, ts.db, &td)
	if err != nil {
		return
	}
	basicID = td.BasicID
	return
}

// GetByCode use the authorization code for token information data
func (ts *TokenStore) GetByCode(code string) (ti oauth2.TokenInfo, err error) {
	ti, err = ts.getData(code)
	return
}

// GetByAccess use the access token for token information data
func (ts *TokenStore) GetByAccess(access string) (ti oauth2.TokenInfo, err error) {
	basicID, err := ts.getBasicID(ts.tcfg.AccessCName, access)
	if err != nil && basicID == "" {
		return
	}
	ti, err = ts.getData(basicID)
	return
}

// GetByRefresh use the refresh token for token information data
func (ts *TokenStore) GetByRefresh(refresh string) (ti oauth2.TokenInfo, err error) {
	basicID, err := ts.getBasicID(ts.tcfg.RefreshCName, refresh)
	if err != nil && basicID == "" {
		return
	}
	ti, err = ts.getData(basicID)
	return
}

type basicData struct {
	ID        string    `json:"id"`
	Data      []byte    `json:"Data"`
	ExpiredAt time.Time `json:"ExpiredAt"`
}

type tokenData struct {
	ID        string    `json:"id"`
	BasicID   string    `json:"BasicID"`
	ExpiredAt time.Time `json:"ExpiredAt"`
}
