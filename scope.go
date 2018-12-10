package gocb

type Scope interface {
	Collection(collectionName string) Collection
	DefaultCollection() Collection
	stateBlock() stateBlock
}

type StdScope struct {
	sb stateBlock
}

func newScope(bucket Bucket, scopeName string) Scope {
	scope := &StdScope{
		sb: bucket.stateBlock(),
	}
	scope.sb.ScopeName = scopeName
	scope.sb.recacheClient()
	return scope
}

func (s *StdScope) clone() *StdScope {
	newS := *s
	return &newS
}

func (s *StdScope) Collection(collectionName string) Collection {
	return newCollection(s, collectionName)
}

func (s *StdScope) DefaultCollection() Collection {
	return s.Collection("_default")
}

func (s *StdScope) stateBlock() stateBlock {
	return s.sb
}
