package gocb

type Bucket struct {
	sb stateBlock
}

func newBucket(c *Cluster, bucketName string) *Bucket {
	return &Bucket{
		sb: stateBlock{
			cluster: c,
			clientStateBlock: clientStateBlock{
				BucketName: bucketName,
			},
		},
	}
}

func (b *Bucket) clone() *Bucket {
	newB := *b
	return &newB
}

func (b *Bucket) WithDurability(persistTo, replicateTo uint) *Bucket {
	n := b.clone()
	n.sb.PersistTo = persistTo
	n.sb.ReplicateTo = replicateTo
	return n
}

func (b *Bucket) Scope(scopeName string) *Scope {
	return newScope(b, scopeName)
}

func (b *Bucket) DefaultScope() *Scope {
	return b.Scope("_default")
}

func (b *Bucket) Collection(collectionName string) *Collection {
	return b.DefaultScope().Collection(collectionName)
}

func (b *Bucket) DefaultCollection() *Collection {
	return b.DefaultScope().DefaultCollection()
}

func (b *Bucket) Views() *ViewsManager {
	return newViewsManager(b)
}
