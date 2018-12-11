package gocb

type Bucket struct {
	sb stateBlock
}

func newBucket(c *Cluster, bucketName string) (*Bucket, error) {
	b := &Bucket{
		sb: stateBlock{
			cluster: c,
			clientStateBlock: clientStateBlock{
				BucketName: bucketName,
			},
		},
	}

	b.sb.recacheClient()
	cli := b.sb.getClient()
	err := cli.connectAgent()
	if err != nil {
		return nil, err
	}

	return b, nil
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
