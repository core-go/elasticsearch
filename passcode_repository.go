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

type PasscodeRepository struct {
	client        *elasticsearch.Client
	indexName     string
	idName        string
	passcodeName  string
	expiredAtName string
}

func NewPasscodeRepository(db *elasticsearch.Client, tableName string, options ...string) *PasscodeRepository {
	var keyName, passcodeName, expiredAtName string
	if len(options) >= 1 && len(options[0]) > 0 {
		expiredAtName = options[0]
	} else {
		expiredAtName = "expiredAt"
	}
	if len(options) >= 2 && len(options[1]) > 0 {
		keyName = options[1]
	} else {
		keyName = "id"
	}
	if len(options) >= 3 && len(options[2]) > 0 {
		passcodeName = options[2]
	} else {
		passcodeName = "passcode"
	}
	return &PasscodeRepository{db, tableName, keyName, passcodeName, expiredAtName}
}

func (p *PasscodeRepository) Save(ctx context.Context, id string, passcode string, expiredAt time.Time) (int64, error) {
	pass := make(map[string]interface{})
	pass[p.passcodeName] = passcode
	pass[p.expiredAtName] = expiredAt
	req := esapi.UpdateRequest{
		Index:      p.indexName,
		DocumentID: id,
		Body:       esutil.NewJSONReader(pass),
		Refresh:    "true",
	}
	res, err := req.Do(ctx, p.client)
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

func (p *PasscodeRepository) Load(ctx context.Context, id string) (string, time.Time, error) {
	result := make(map[string]interface{})
	ok, err := FindOneByIdAndDecode(ctx, p.client, p.indexName, id, &result)
	if err != nil || !ok {
		return "", time.Now(), err
	}
	return result[p.passcodeName].(string), result[p.passcodeName].(time.Time), nil
}

func (p *PasscodeRepository) Delete(ctx context.Context, id string) (int64, error) {
	return DeleteOne(ctx, p.client, p.indexName, id)
}
