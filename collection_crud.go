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
