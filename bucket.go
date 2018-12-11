package gocb

type Bucket struct {
	sb stateBlock
}

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

func (b *Bucket) Name() string {
	return b.sb.BucketName
}

func (b *Bucket) Scope(scopeName string) *Scope {
	return newScope(b, scopeName)
}

func (b *Bucket) DefaultScope() *Scope {
	return b.Scope("_default")
}

func (b *Bucket) Collection(scopeName string, collectionName string) *Collection {
	return b.Scope(scopeName).Collection(collectionName)
}

func (b *Bucket) DefaultCollection() *Collection {
	return b.DefaultScope().DefaultCollection()
}

func (b *Bucket) Views() *ViewsManager {
	return newViewsManager(b)
}

func (b *Bucket) stateBlock() stateBlock {
	return b.sb
}
