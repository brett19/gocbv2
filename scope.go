package gocb

type Scope struct {
	sb stateBlock
}

func newScope(bucket *Bucket, scopeName string) *Scope {
	scope := &Scope{
		sb: bucket.sb,
	}
	scope.sb.ScopeName = scopeName
	scope.sb.recacheClient()
	return scope
}

func (s *Scope) clone() *Scope {
	newS := *s
	return &newS
}

func (s *Scope) Collection(collectionName string) *Collection {
	return newCollection(s, collectionName)
}

func (s *Scope) DefaultCollection() *Collection {
	return s.Collection("_default")
}
