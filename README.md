# Elasticsearch
- Elasticsearch Client Utilities

## Installation

Please make sure to initialize a Go module before installing common-go/elasticsearch:

```shell
go get -u github.com/common-go/elasticsearch
```

Import:

```go
import "github.com/common-go/elasticsearch"
```

You can optimize the import by version:
- v0.0.1: Utilities to support query, find one by Id
- v0.0.4: Utilities to support insert, update, patch, upsert, delete
- v0.0.7: Utilities to support batch update
- v1.0.0: ViewService and GenericService
- v1.0.4: SearchService