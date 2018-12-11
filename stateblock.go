package gocb

import (
	"fmt"
	"time"
)

type clientStateBlock struct {
	BucketName        string
	UseMutationTokens bool
}

func (sb *clientStateBlock) Hash() string {
	return fmt.Sprintf("%s-%t",
		sb.BucketName,
		sb.UseMutationTokens)
}

type collectionStateBlock struct {
	CollectionID          uint32
	ScopeID               uint32
	CollectionInitialized bool
	CollectionUnknown     bool
	ScopeUnknown          bool
}

type servicesStateBlock struct {
	n1qlTimeout time.Duration
}

type stateBlock struct {
	cluster      *Cluster
	cachedClient *client

	clientStateBlock

	ScopeName      string
	CollectionName string

	KvTimeout   time.Duration
	PersistTo   uint
	ReplicateTo uint

	N1qlRetryBehavior RetryBehavior
}

func (sb *stateBlock) getClient() *client {
	if sb.cachedClient == nil {
		panic("attempted to fetch client from incomplete state block")
	}

	return sb.cachedClient
}

func (sb *stateBlock) recacheClient() {
	if sb.cachedClient != nil && sb.cachedClient.Hash() == sb.Hash() {
		return
	}

	sb.cachedClient = sb.cluster.getClient(&sb.clientStateBlock)
}
