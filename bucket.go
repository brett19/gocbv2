package gocb

// Bucket is an interface representing a single bucket within a cluster.
type Bucket struct {
	sb stateBlock
}

// BucketOptions are the options available when connecting to a Bucket.
type BucketOptions struct {
	UseMutationTokens bool
}

func newBucket(c *Cluster, bucketName string, opts BucketOptions) *Bucket {
	return &Bucket{
		sb: stateBlock{
			cluster: c,
			clientStateBlock: clientStateBlock{
				BucketName:        bucketName,
				UseMutationTokens: opts.UseMutationTokens,
			},
		},
	}
}

func (b *Bucket) connect() error {
	b.sb.recacheClient()
	cli := b.sb.getClient()
	return cli.connect()
}

func (b *Bucket) clone() *Bucket {
	newB := *b
	return &newB
}

// Name returns the name of the bucket.
func (b *Bucket) Name() string {
	return b.sb.BucketName
}

// Scope returns an instance of a Scope.
func (b *Bucket) Scope(scopeName string) *Scope {
	return newScope(b, scopeName)
}

func (b *Bucket) defaultScope() *Scope {
	return b.Scope("_default")
}

// Collection returns an instance of a collection.
func (b *Bucket) Collection(scopeName string, collectionName string) *Collection {
	return b.Scope(scopeName).Collection(collectionName)
}

// DefaultCollection returns an instance of the default collection.
func (b *Bucket) DefaultCollection() *Collection {
	return b.defaultScope().DefaultCollection()
}

func (b *Bucket) Views() *ViewsManager {
	return newViewsManager(b)
}

func (b *Bucket) stateBlock() stateBlock {
	return b.sb
}
