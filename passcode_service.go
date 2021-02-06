package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/esutil"
	"time"
)

type PasscodeService struct {
	client        *elasticsearch.Client
	indexName     string
	idName        string
	passcodeName  string
	expiredAtName string
}

func NewPasscodeService(db *elasticsearch.Client, tableName string, options ...string) *PasscodeService {
	var keyName, passcodeName, expiredAtName string
	if len(options) >= 1 && len(options[0]) > 0 {
		keyName = options[0]
	} else {
		keyName = "_id"
	}
	if len(options) >= 2 && len(options[1]) > 0 {
		passcodeName = options[1]
	} else {
		passcodeName = "passcode"
	}
	if len(options) >= 3 && len(options[2]) > 0 {
		expiredAtName = options[2]
	} else {
		expiredAtName = "expiredAt"
	}
	return &PasscodeService{db, tableName, keyName, passcodeName, expiredAtName}
}

func (s *PasscodeService) Save(ctx context.Context, id string, passcode string, expiredAt time.Time) (int64, error) {
	pass := make(map[string]interface{})
	pass[s.passcodeName] = passcode
	pass[s.expiredAtName] = expiredAt
	req := esapi.UpdateRequest{
		Index:      s.indexName,
		DocumentID: id,
		Body:       esutil.NewJSONReader(pass),
		Refresh:    "true",
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return -1, fmt.Errorf("document ID not exists in the index")
	}

	var temp map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&temp)
	if err != nil {
		return -1, err
	}

	successful := int64(temp["_shards"].(map[string]interface{})["successful"].(float64))
	return successful, nil
}

func (s *PasscodeService) Load(ctx context.Context, id string) (string, time.Time, error) {
	result := make(map[string]interface{})
	ok, err := FindOneByIdAndDecode(ctx, s.client, s.indexName, id, &result)
	if err != nil || !ok {
		return "", time.Now(), err
	}
	return result[s.passcodeName].(string), result[s.passcodeName].(time.Time), nil
}

func (s *PasscodeService) Delete(ctx context.Context, id string) (int64, error) {
	return DeleteOne(ctx, s.client, s.indexName, id)
}
