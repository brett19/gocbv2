package gocb

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/couchbase/gocbcore.v7"
)

func (c *Collection) observeOnceCas(tracectx opentracing.SpanContext, key []byte, cas Cas, forDelete bool, replicaIdx int, commCh chan uint) (pendingOp, error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent.ObserveEx(gocbcore.ObserveOptions{
		Key:          key,
		ReplicaIdx:   replicaIdx,
		TraceContext: tracectx,
	}, func(res *gocbcore.ObserveResult, err error) {
		if err != nil || res == nil {
			commCh <- 0
			return
		}

		didReplicate := false
		didPersist := false

		if res.KeyState == gocbcore.KeyStatePersisted {
			if !forDelete {
				if Cas(res.Cas) == cas {
					if replicaIdx != 0 {
						didReplicate = true
					}
					didPersist = true
				}
			}
		} else if res.KeyState == gocbcore.KeyStateNotPersisted {
			if !forDelete {
				if Cas(res.Cas) == cas {
					if replicaIdx != 0 {
						didReplicate = true
					}
				}
			}
		} else if res.KeyState == gocbcore.KeyStateDeleted {
			if forDelete {
				didReplicate = true
			}
		} else {
			if forDelete {
				didReplicate = true
				didPersist = true
			}
		}

		var out uint
		if didReplicate {
			out |= 1
		}
		if didPersist {
			out |= 2
		}
		commCh <- out
	})
}

func (c *Collection) observeOnceSeqNo(tracectx opentracing.SpanContext, mt MutationToken, replicaIdx int, commCh chan uint) (pendingOp, error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent.ObserveVbEx(gocbcore.ObserveVbOptions{
		VbId:         mt.token.VbId,
		VbUuid:       mt.token.VbUuid,
		ReplicaIdx:   replicaIdx,
		TraceContext: tracectx,
	}, func(res *gocbcore.ObserveVbResult, err error) {
		if err != nil || res == nil {
			commCh <- 0
			return
		}

		didReplicate := res.CurrentSeqNo >= mt.token.SeqNo
		didPersist := res.PersistSeqNo >= mt.token.SeqNo

		var out uint
		if didReplicate {
			out |= 1
		}
		if didPersist {
			out |= 2
		}
		commCh <- out
	})
}

func (c *Collection) observeOne(ctx context.Context, tracectx opentracing.SpanContext, key []byte, mt MutationToken, cas Cas, forDelete bool, replicaIdx int, replicaCh, persistCh chan bool) {
	observeOnce := func(commCh chan uint) (pendingOp, error) {
		if mt.token.VbUuid != 0 && mt.token.SeqNo != 0 {
			return c.observeOnceSeqNo(tracectx, mt, replicaIdx, commCh)
		}
		return c.observeOnceCas(tracectx, key, cas, forDelete, replicaIdx, commCh)
	}

	sentReplicated := false
	sentPersisted := false

	failMe := func() {
		if !sentReplicated {
			replicaCh <- false
			sentReplicated = true
		}
		if !sentPersisted {
			persistCh <- false
			sentPersisted = true
		}
	}

	// Doing this will set the context deadline to whichever is shorter, what is already set or the timeout
	// value
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, c.sb.DuraTimeout)
	defer cancel()

	commCh := make(chan uint)
	for {
		op, err := observeOnce(commCh)
		if err != nil {
			failMe()
			return
		}

		select {
		case val := <-commCh:
			// Got Value
			if (val&1) != 0 && !sentReplicated {
				replicaCh <- true
				sentReplicated = true
			}
			if (val&2) != 0 && !sentPersisted {
				persistCh <- true
				sentPersisted = true
			}

			if sentReplicated && sentPersisted {
				return
			}

			waitTmr := gocbcore.AcquireTimer(c.sb.DuraPollTimeout)
			select {
			case <-waitTmr.C:
				gocbcore.ReleaseTimer(waitTmr, true)
				// Fall through to outside for loop
			case <-ctx.Done():
				gocbcore.ReleaseTimer(waitTmr, false)
				failMe()
				return
			}

		case <-ctx.Done():
			// Timed out
			op.Cancel()
			failMe()
			return
		}
	}
}

func (c *Collection) durability(ctx context.Context, tracectx opentracing.SpanContext, key string, cas Cas, mt MutationToken, replicaTo, persistTo uint, forDelete bool) error {
	if ctx == nil {
		ctx = context.Background()
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return err
	}

	numServers := agent.NumReplicas() + 1

	if replicaTo > uint(numServers-1) || persistTo > uint(numServers) {
		// return ErrNotEnoughReplicas TODO
	}

	keyBytes := []byte(key)

	replicaCh := make(chan bool, numServers)
	persistCh := make(chan bool, numServers)

	for replicaIdx := 0; replicaIdx < numServers; replicaIdx++ {
		go c.observeOne(ctx, tracectx, keyBytes, mt, cas, forDelete, replicaIdx, replicaCh, persistCh)
	}

	results := int(0)
	replicas := uint(0)
	persists := uint(0)

	for {
		select {
		case rV := <-replicaCh:
			if rV {
				replicas++
			}
			results++
		case pV := <-persistCh:
			if pV {
				persists++
			}
			results++
		}

		if replicas >= replicaTo && persists >= persistTo {
			return nil
		} else if results == (numServers * 2) {
			// return ErrDurabilityTimeout TODO
		}
	}
}
