package gocb

type Scope struct {
	sb stateBlock
}

func newScope(bucket *Bucket, scopeName string) *Scope {
	scope := &Scope{
		sb: bucket.stateBlock(),
	}
	scope.sb.ScopeName = scopeName
	scope.sb.recacheClient()
	return scope
}

func (s *Scope) clone() *Scope {
	newS := *s
	return &newS
}

func (s *Scope) Collection(collectionName string, opts *CollectionOptions) (*Collection, error) {
	return newCollection(s, collectionName, opts)
}

func (s *Scope) DefaultCollection(opts *CollectionOptions) (*Collection, error) {
	return s.Collection("_default", opts)
}

func (s *Scope) stateBlock() stateBlock {
	return s.sb
}
