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
	n1qlTimeout      time.Duration
	analyticsTimeout time.Duration
	searchTimeout    time.Duration
}

type stateBlock struct {
	cachedClient client

	clientStateBlock

	ScopeName      string
	CollectionName string

	KvTimeout       time.Duration
	DuraTimeout     time.Duration
	DuraPollTimeout time.Duration
	PersistTo       uint
	ReplicateTo     uint

	N1qlRetryBehavior      RetryBehavior
	AnalyticsRetryBehavior RetryBehavior
	SearchRetryBehavior    RetryBehavior

	N1qlTimeout      func() time.Duration
	SearchTimeout    func() time.Duration
	AnalyticsTimeout func() time.Duration

	client func(*clientStateBlock) client
}

func (sb *stateBlock) getCachedClient() client {
	if sb.cachedClient == nil {
		panic("attempted to fetch client from incomplete state block")
	}

	return sb.cachedClient
}

// TODO: Is the check here redundant? We should only be changing hash if the bucket
// is different, which is a new stateblock anyway so does recaching ever
// make sense?
func (sb *stateBlock) recacheClient() {
	if sb.cachedClient != nil && sb.cachedClient.Hash() == sb.Hash() {
		return
	}

	sb.cachedClient = sb.client(&sb.clientStateBlock)
}
