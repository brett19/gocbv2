package gocb

import (
	"log"
	"time"

	gocbcore "gopkg.in/couchbase/gocbcore.v7"
)

// TODO: Need to handle timeouts here.  It might be neccessary to move
// timeout handling down into gocbcore, but that is still uncertain.

type SetOptions struct {
	expiry uint32
}

func (opts SetOptions) ExpireAt(expiry time.Time) SetOptions {
	opts.expiry = uint32(expiry.Unix())
	return opts
}

func (c *Collection) Insert(key string, doc Document, opts *SetOptions) (errOut error) {
	if opts == nil {
		opts = &SetOptions{}
	}

	log.Printf("Fetching Agent")

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return err
	}

	log.Printf("Transcoding")

	tdoc, err := c.transcodeDocument(doc)
	if err != nil {
		return err
	}

	waitCh := make(chan struct{})

	agent.AddEx(gocbcore.AddOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Value:        tdoc.bytes,
		Flags:        tdoc.flags,
		Expiry:       opts.expiry,
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

func (c *Collection) Upsert(key string, doc Document, opts *SetOptions) (errOut error) {
	if opts == nil {
		opts = &SetOptions{}
	}

	log.Printf("Fetching Agent")

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return err
	}

	log.Printf("Transcoding")

	tdoc, err := c.transcodeDocument(doc)
	if err != nil {
		return err
	}

	waitCh := make(chan struct{})

	agent.SetEx(gocbcore.SetOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Value:        tdoc.bytes,
		Flags:        tdoc.flags,
		Expiry:       opts.expiry,
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

type ReplaceOptions struct {
	expiry uint32
	cas    Cas
}

func (opts ReplaceOptions) ExpireAt(expiry time.Time) ReplaceOptions {
	opts.expiry = uint32(expiry.Unix())
	return opts
}

func (opts ReplaceOptions) Cas(cas Cas) ReplaceOptions {
	opts.cas = cas
	return opts
}

func (c *Collection) Replace(key string, doc Document, opts *ReplaceOptions) (errOut error) {
	if opts == nil {
		opts = &ReplaceOptions{}
	}

	log.Printf("Fetching Agent")

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return err
	}

	log.Printf("Transcoding")

	tdoc, err := c.transcodeDocument(doc)
	if err != nil {
		return err
	}

	waitCh := make(chan struct{})

	agent.ReplaceEx(gocbcore.ReplaceOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Value:        tdoc.bytes,
		Flags:        tdoc.flags,
		Expiry:       opts.expiry,
		Cas:          gocbcore.Cas(opts.cas),
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

type GetOptions struct{}

func (c *Collection) Get(key string, opts *GetOptions) (docOut *Document, errOut error) {
	if opts == nil {
		opts = &GetOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	agent.GetEx(gocbcore.GetOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
	}, func(res *gocbcore.GetResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		docOut = &Document{
			bytes: res.Value,
			flags: res.Flags,
			cas:   Cas(res.Cas),
		}
		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

type RemoveOptions struct {
	cas Cas
}

func (opts RemoveOptions) Cas(cas Cas) RemoveOptions {
	opts.cas = cas
	return opts
}

func (c *Collection) Remove(key string, opts *RemoveOptions) (errOut error) {
	if opts == nil {
		opts = &RemoveOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()

	if err != nil {
		return err
	}

	waitCh := make(chan struct{})

	agent.DeleteEx(gocbcore.DeleteOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Cas:          gocbcore.Cas(opts.cas),
	}, func(res *gocbcore.DeleteResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}
			errOut = err
			waitCh <- struct{}{}
			return
		}

		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

func (c *Collection) GetAndTouch(key string, opts *TouchOptions) (docOut *Document, errOut error) {
	if opts == nil {
		opts = &TouchOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	agent.GetAndTouchEx(gocbcore.GetAndTouchOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Expiry:       opts.expiry,
	}, func(res *gocbcore.GetAndTouchResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		docOut = &Document{
			bytes: res.Value,
			flags: res.Flags,
			cas:   Cas(res.Cas),
		}
		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

type GetAndLockOptions struct {
	lockTime uint32
}

func (opts GetAndLockOptions) LockTime(lockTime time.Time) GetAndLockOptions {
	opts.lockTime = uint32(lockTime.Unix())
	return opts
}

func (c *Collection) GetAndLock(key string, opts *GetAndLockOptions) (docOut *Document, errOut error) {
	if opts == nil {
		opts = &GetAndLockOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	agent.GetAndLockEx(gocbcore.GetAndLockOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		LockTime:     opts.lockTime,
	}, func(res *gocbcore.GetAndLockResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		docOut = &Document{
			bytes: res.Value,
			flags: res.Flags,
			cas:   Cas(res.Cas),
		}
		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

type UnlockOptions struct {
	cas Cas
}

func (opts UnlockOptions) Cas(cas Cas) UnlockOptions {
	opts.cas = cas
	return opts
}

func (c *Collection) Unlock(key string, opts *UnlockOptions) (errOut error) {
	if opts == nil {
		opts = &UnlockOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return err
	}

	waitCh := make(chan struct{})

	agent.UnlockEx(gocbcore.UnlockOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Cas:          gocbcore.Cas(opts.cas),
	}, func(res *gocbcore.UnlockResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

type GetReplicaOptions struct {
	replicaIdx int
}

func (opts GetReplicaOptions) ReplicaIndex(replicaIdx int) GetReplicaOptions {
	opts.replicaIdx = replicaIdx
	return opts
}

func (c *Collection) GetReplica(key string, opts *GetReplicaOptions) (docOut *Document, errOut error) {
	if opts == nil {
		opts = &GetReplicaOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	agent.GetReplicaEx(gocbcore.GetReplicaOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		ReplicaIdx:   opts.replicaIdx,
	}, func(res *gocbcore.GetReplicaResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		docOut = &Document{
			bytes: res.Value,
			flags: res.Flags,
			cas:   Cas(res.Cas),
		}
		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

type TouchOptions struct {
	expiry uint32
}

func (opts TouchOptions) ExpireAt(expiry time.Time) TouchOptions {
	opts.expiry = uint32(expiry.Unix())
	return opts
}

func (c *Collection) Touch(key string, opts *TouchOptions) (errOut error) {
	if opts == nil {
		opts = &TouchOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return err
	}

	waitCh := make(chan struct{})

	agent.TouchEx(gocbcore.TouchOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Expiry:       opts.expiry,
	}, func(res *gocbcore.TouchResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		waitCh <- struct{}{}
	})

	<-waitCh

	return
}
