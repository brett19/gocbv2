package gocb

type searchQueryData struct {
	Query interface{} `json:"query,omitempty"`
}

// SearchQuery represents a pending search query.
type SearchQuery struct {
	name string
	data searchQueryData
}

func (sq *SearchQuery) indexName() string {
	return sq.name
}

// NewSearchQuery creates a new SearchQuery object from an index name and query.
func NewSearchQuery(indexName string, query interface{}) *SearchQuery {
	q := &SearchQuery{
		name: indexName,
	}
	q.data.Query = query
	return q
}
