package gocb

type ViewsManager struct {
	bucket Bucket
}

func newViewsManager(b Bucket) *ViewsManager {
	return &ViewsManager{
		bucket: b,
	}
}
