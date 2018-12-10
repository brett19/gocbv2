package gocb

type Bucket interface {
	Name() string
	Scope(scopeName string) Scope
	DefaultCollection() Collection
	Collection(scopeName string, collectionName string) Collection
	// ViewQuery(designDoc string, viewName string, opts *ViewOptions) (ViewResult, error)
	// GetManager() *BucketManager
	stateBlock() stateBlock
}

type StdBucket struct {
	sb stateBlock
}

func newBucket(c Cluster, bucketName string) Bucket {
	return &StdBucket{
		sb: stateBlock{
			cluster: c,
			clientStateBlock: clientStateBlock{
				BucketName: bucketName,
			},
			tracer: c.Tracer(),
		},
	}
}

func (b *StdBucket) clone() *StdBucket {
	newB := *b
	return &newB
}

func (b *StdBucket) Name() string {
	return b.sb.BucketName
}

func (b *StdBucket) Scope(scopeName string) Scope {
	return newScope(b, scopeName)
}

func (b *StdBucket) DefaultScope() Scope {
	return b.Scope("_default")
}

func (b *StdBucket) Collection(scopeName string, collectionName string) Collection {
	return b.Scope(scopeName).Collection(collectionName)
}

func (b *StdBucket) DefaultCollection() Collection {
	return b.DefaultScope().DefaultCollection()
}

func (b *StdBucket) Views() *ViewsManager {
	return newViewsManager(b)
}

func (b *StdBucket) stateBlock() stateBlock {
	return b.sb
}
