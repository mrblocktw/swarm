// Copyright 2019 The Swarm Authors
// This file is part of the Swarm library.
//
// The Swarm library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Swarm library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the Swarm library. If not, see <http://www.gnu.org/licenses/>.

package newstream

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/log"
	"github.com/ethersphere/swarm/network"
	bv "github.com/ethersphere/swarm/network/bitvector"
	"github.com/ethersphere/swarm/p2p/protocols"
	"github.com/ethersphere/swarm/state"
	"github.com/ethersphere/swarm/storage"
)

var (
	// Compile time interface check
	_ node.Service = (*SlipStream)(nil)

	// Metrics
	processReceivedChunksCount = metrics.NewRegisteredCounter("network.stream.received_chunks.count", nil)
	streamSeenChunkDelivery    = metrics.NewRegisteredCounter("network.stream.seen_chunk_delivery.count", nil)
	streamEmptyWantedHashes    = metrics.NewRegisteredCounter("network.stream.empty_wanted_hashes.count", nil)
	streamWantedHashes         = metrics.NewRegisteredCounter("network.stream.wanted_hashes.count", nil)

	streamBatchFail               = metrics.NewRegisteredCounter("network.stream.batch_fail.count", nil)
	streamChunkDeliveryFail       = metrics.NewRegisteredCounter("network.stream.delivery_fail.count", nil)
	streamRequestNextIntervalFail = metrics.NewRegisteredCounter("network.stream.next_interval_fail.count", nil)
	lastReceivedChunksMsg         = metrics.GetOrRegisterGauge("network.stream.received_chunks", nil)

	// Protocol spec
	Spec = &protocols.Spec{
		Name:       "bzz-stream",
		Version:    8,
		MaxMsgSize: 10 * 1024 * 1024,
		Messages: []interface{}{
			StreamInfoReq{},
			StreamInfoRes{},
			GetRange{},
			OfferedHashes{},
			ChunkDelivery{},
			WantedHashes{},
		},
	}
)

// SlipStream is the base type that handles all client/server operations on a node
// it is instantiated once per stream protocol instance, that is, it should have
// one instance per node
type SlipStream struct {
	mtx            sync.RWMutex
	intervalsStore state.Store //every protocol would make use of this
	peers          map[enode.ID]*Peer

	providers map[string]StreamProvider

	spec    *protocols.Spec   //this protocol's spec
	balance protocols.Balance //implements protocols.Balance, for accounting
	prices  protocols.Prices  //implements protocols.Prices, provides prices to accounting

	quit chan struct{} // terminates registry goroutines
}

func NewSlipStream(intervalsStore state.Store, providers ...StreamProvider) *SlipStream {
	slipStream := &SlipStream{
		intervalsStore: intervalsStore,
		peers:          make(map[enode.ID]*Peer),
		providers:      make(map[string]StreamProvider),
		quit:           make(chan struct{}),
	}
	for _, p := range providers {
		slipStream.providers[p.StreamName()] = p
	}

	slipStream.spec = Spec

	return slipStream
}

func (s *SlipStream) getProvider(stream ID) StreamProvider {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.providers[stream.Name]
}

func (s *SlipStream) getPeer(id enode.ID) *Peer {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	p := s.peers[id]
	return p
}

func (s *SlipStream) addPeer(p *Peer) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.peers[p.ID()] = p
}

func (s *SlipStream) removePeer(p *Peer) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if _, found := s.peers[p.ID()]; found {
		p.logError("removing peer")
		delete(s.peers, p.ID())
		close(p.quit)
	} else {
		p.logWarn("peer was marked for removal but not found")
	}
}

// Run is being dispatched when 2 nodes connect
func (s *SlipStream) Run(p *p2p.Peer, rw p2p.MsgReadWriter) error {
	peer := protocols.NewPeer(p, rw, s.spec)
	bp := network.NewBzzPeer(peer)

	sp := NewPeer(bp, s.intervalsStore, s.providers)
	s.addPeer(sp)
	defer s.removePeer(sp)
	go sp.InitProviders()
	return peer.Run(s.HandleMsg(sp))
}

func (s *SlipStream) HandleMsg(p *Peer) func(context.Context, interface{}) error {
	return func(ctx context.Context, msg interface{}) error {
		switch msg := msg.(type) {
		case *StreamInfoReq:
			go s.handleStreamInfoReq(ctx, p, msg)
		case *StreamInfoRes:
			go s.handleStreamInfoRes(ctx, p, msg)
		case *GetRange:
			if msg.To == nil {
				// handle live
				go s.handleGetRangeHead(ctx, p, msg)
			} else {
				go s.handleGetRange(ctx, p, msg)
			}
		case *OfferedHashes:
			go s.handleOfferedHashes(ctx, p, msg)
		case *WantedHashes:
			go s.handleWantedHashes(ctx, p, msg)
		case *ChunkDelivery:
			go s.handleChunkDelivery(ctx, p, msg)
		}
		return nil
	}
}

// handleStreamInfoReq handles the StreamInfoReq message.
// this message is handled by the SERVER (*Peer is the client in this case)
func (s *SlipStream) handleStreamInfoReq(ctx context.Context, p *Peer, msg *StreamInfoReq) {
	p.logDebug("handleStreamInfoReq")
	streamRes := StreamInfoRes{}
	if len(msg.Streams) == 0 {
		p.logError("nil streams msg requested")
		p.Drop()
		return
	}
	for _, v := range msg.Streams {
		v := v
		provider := s.getProvider(v)
		if provider == nil {
			p.logError("unsupported provider", "stream", v)
			// tell the other peer we dont support this stream. this is non fatal
			// this might not be fatal as we might not support all providers.
			return
		}

		streamCursor, err := provider.CursorStr(v.Key)
		if err != nil {
			p.logError("error getting cursor for stream key", "name", v.Name, "key", v.Key, "err", err)
			panic(fmt.Errorf("provider cursor str %q: %v", v.Key, err))
			p.Drop()
			return
		}
		descriptor := StreamDescriptor{
			Stream:  v,
			Cursor:  streamCursor,
			Bounded: provider.Boundedness(),
		}
		streamRes.Streams = append(streamRes.Streams, descriptor)
	}
	if err := p.Send(ctx, streamRes); err != nil {
		p.logError("failed to send StreamInfoRes to client", "err", err)
	}
}

// TODO: provide this option value from StreamProvider?
var streamAutostart = true

// handleStreamInfoRes handles the StreamInfoRes message.
// this message is handled by the CLIENT (*Peer is the server in this case)
func (st *SlipStream) handleStreamInfoRes(ctx context.Context, p *Peer, msg *StreamInfoRes) {
	p.logDebug("handleStreamInfoRes")

	if len(msg.Streams) == 0 {
		p.logError("StreamInfo response is empty")
		p.Drop()
		return
	}

	for _, s := range msg.Streams {
		s := s
		provider := st.getProvider(s.Stream)
		if provider == nil {
			// at this point of the message exchange unsupported providers are illegal. drop peer
			p.logError("unsupported provider", "stream", s.Stream)
			p.Drop()
			return
		}

		p.logDebug("setting stream cursor", "stream", s.Stream, "cursor", s.Cursor)
		p.setCursor(s.Stream, s.Cursor)

		if streamAutostart {
			if s.Cursor > 0 {
				p.logDebug("requesting history stream", "stream", s.Stream, "cursor", s.Cursor)

				// fetch everything from beginning till s.Cursor
				go func() {
					err := st.requestStreamRange(ctx, p, s.Stream, s.Cursor)
					if err != nil {
						p.logError("had an error sending initial GetRange for historical stream", "stream", s.Stream, "err", err)
						p.Drop()
					}
				}()
			}

			// handle stream unboundedness
			if !s.Bounded {
				//constantly fetch the head of the stream
				go func() {
					p.logDebug("asking for live stream", "stream", s.Stream, "cursor", s.Cursor)

					// ask the tip (cursor + 1)
					err := st.requestStreamHead(ctx, p, s.Stream, s.Cursor+1)
					// https://github.com/golang/go/issues/4373 - use of closed network connection
					if err != nil && err != p2p.ErrShuttingDown && !strings.Contains(err.Error(), "use of closed network connection") {
						p.logError("had an error with initial stream head fetch", "stream", s.Stream, "cursor", s.Cursor+1, "err", err)
						p.Drop()
					}
				}()
			}
		}
	}
}

func (s *SlipStream) createSendWant(ctx context.Context, p *Peer, stream ID, from uint64, to *uint64, head bool) error {
	g := GetRange{
		Ruid:      uint(rand.Uint32()),
		Stream:    stream,
		From:      from,
		To:        to,
		BatchSize: BatchSize,
		Roundtrip: true,
	}

	p.logDebug("sending GetRange to peer", "ruid", g.Ruid, "stream", stream)

	w := &want{
		ruid:      g.Ruid,
		stream:    g.Stream,
		from:      g.From,
		to:        to,
		head:      head,
		hashes:    make(map[string]bool),
		requested: time.Now(),
	}
	p.mtx.Lock()
	p.openWants[w.ruid] = w
	p.mtx.Unlock()

	return p.Send(ctx, g)
}

func (s *SlipStream) requestStreamHead(ctx context.Context, p *Peer, stream ID, from uint64) error {
	p.logDebug("peer.requestStreamHead", "stream", stream, "from", from)
	return s.createSendWant(ctx, p, stream, from, nil, true)
}

func (s *SlipStream) requestStreamRange(ctx context.Context, p *Peer, stream ID, cursor uint64) error {
	p.logDebug("peer.requestStreamRange", "stream", stream, "cursor", cursor)
	provider := s.getProvider(stream)
	if provider == nil {
		// at this point of the message exchange unsupported providers are illegal. drop peer
		p.logError("unsupported provider", "stream", stream)
		p.Drop()
		return nil
	}
	from, _, empty, err := p.nextInterval(stream, 0)
	if err != nil {
		return err
	}
	p.logDebug("peer.requestStreamRange nextInterval", "stream", stream, "cursor", cursor, "from", from)
	if from > cursor || empty {
		p.logDebug("peer.requestStreamRange stream finished", "stream", stream, "cursor", cursor)
		// stream finished. quit
		return nil
	}
	return s.createSendWant(ctx, p, stream, from, &cursor, false)
}

func (s *SlipStream) handleGetRangeHead(ctx context.Context, p *Peer, msg *GetRange) {
	p.logDebug("peer.handleGetRangeHead", "ruid", msg.Ruid)
	provider := s.getProvider(msg.Stream)
	if provider == nil {
		// at this point of the message exchange unsupported providers are illegal. drop peer
		p.logError("unsupported provider", "stream", msg.Stream)
		p.Drop()
		return
	}

	key, err := provider.ParseKey(msg.Stream.Key)
	if err != nil {
		p.logError("erroring parsing stream key", "stream", msg.Stream, "err", err)
		p.Drop()
		return
	}
	log.Debug("peer.handleGetRangeHead collecting batch", "from", msg.From)
	h, f, t, e, err := s.collectBatch(ctx, p, provider, key, msg.From, 0)
	p.logDebug("peer.collectBatch", "stream", msg.Stream, "len(h)", len(h), "f", f, "t", t, "e", e, "err", err)
	if err != nil {
		p.logError("erroring getting live batch for stream", "stream", msg.Stream, "err", err)
		p.Drop()
		return
	}

	if e {
		select {
		case <-p.quit:
			p.logDebug("not sending batch due to shutdown")
			// prevent sending an empty batch that resulted from db shutdown
			return
		default:
			return
			offered := OfferedHashes{
				Ruid:      msg.Ruid,
				LastIndex: msg.From,
				Hashes:    []byte{},
			}
			if err := p.Send(ctx, offered); err != nil {
				p.logError("erroring sending empty live offered hashes", "ruid", msg.Ruid, "err", err)
			}
			return
		}
	}

	o := offer{
		ruid:      msg.Ruid,
		stream:    msg.Stream,
		hashes:    h,
		requested: time.Now(),
	}

	p.mtx.Lock()
	p.openOffers[msg.Ruid] = o
	p.mtx.Unlock()

	offered := OfferedHashes{
		Ruid:      msg.Ruid,
		LastIndex: t,
		Hashes:    h,
	}
	i := len(h) / HashSize
	p.logDebug("server offering batch", "ruid", msg.Ruid, "requestfrom", msg.From, "requestto", msg.To, "hashes", i)
	if err := p.Send(ctx, offered); err != nil {
		p.logError("erroring sending offered hashes", "ruid", msg.Ruid, "err", err)
		p.mtx.Lock()
		delete(p.openOffers, msg.Ruid)
		p.mtx.Unlock()
	}
}

// handleGetRange is handled by the SERVER and sends in response an OfferedHashes message
// in the case that for the specific interval no chunks exist - the server sends an empty OfferedHashes
// message so that the client could seal the interval and request the next
func (s *SlipStream) handleGetRange(ctx context.Context, p *Peer, msg *GetRange) {
	p.logDebug("peer.handleGetRange", "ruid", msg.Ruid)
	provider := s.getProvider(msg.Stream)
	if provider == nil {
		// at this point of the message exchange unsupported providers are illegal. drop peer
		p.logError("unsupported provider", "stream", msg.Stream)
		p.Drop()
		return
	}

	key, err := provider.ParseKey(msg.Stream.Key)
	if err != nil {
		p.logError("erroring parsing stream key", "err", err, "stream", msg.Stream)
		p.Drop()
		return
	}
	log.Debug("peer.handleGetRange collecting batch", "from", msg.From, "to", msg.To)
	h, f, t, e, err := s.collectBatch(ctx, p, provider, key, msg.From, *msg.To)
	// empty batch can be legit, TODO: check which errors should be handled, if any
	if err != nil {
		log.Error("erroring getting batch for stream", "peer", p.ID(), "stream", msg.Stream, "err", err)
		panic("for now")
		p.Drop()
		return
	}
	if e {
		p.logDebug("interval is empty for requested range", "empty?", e, "hashes", len(h)/HashSize, "ruid", msg.Ruid)
		select {
		case <-p.quit:
			// prevent sending an empty batch that resulted from db shutdown
			return
		default:
			offered := OfferedHashes{
				Ruid:      msg.Ruid,
				LastIndex: msg.From,
				Hashes:    []byte{},
			}
			if err := p.Send(ctx, offered); err != nil {
				p.logError("erroring sending empty offered hashes", "ruid", msg.Ruid, "err", err)
			}
			return
		}
	}
	p.logDebug("collected hashes for requested range", "hashes", len(h)/HashSize, "ruid", msg.Ruid)
	o := offer{
		ruid:      msg.Ruid,
		stream:    msg.Stream,
		hashes:    h,
		requested: time.Now(),
	}

	p.mtx.Lock()
	p.openOffers[msg.Ruid] = o
	p.mtx.Unlock()

	offered := OfferedHashes{
		Ruid:      msg.Ruid,
		LastIndex: t,
		Hashes:    h,
	}
	l := len(h) / HashSize
	p.logDebug("server offering batch", "ruid", msg.Ruid, "requestFrom", msg.From, "From", f, "requestTo", msg.To, "hashes", h, "hashes", l)
	if err := p.Send(ctx, offered); err != nil {
		p.logError("erroring sending offered hashes", "ruid", msg.Ruid, "err", err)
	}
}

// handleOfferedHashes handles the OfferedHashes wire protocol message.
// this message is handled by the CLIENT.
func (s *SlipStream) handleOfferedHashes(ctx context.Context, p *Peer, msg *OfferedHashes) {
	p.logDebug("peer.handleOfferedHashes", "ruid", msg.Ruid, "msg.lastIndex", msg.LastIndex)
	hashes := msg.Hashes
	lenHashes := len(hashes)
	if lenHashes%HashSize != 0 {
		p.logError("error invalid hashes length", "len", lenHashes, "ruid", msg.Ruid)
		p.Drop()
		return
	}

	p.mtx.RLock()
	w, ok := p.openWants[msg.Ruid]
	p.mtx.RUnlock()
	if !ok {
		p.logError("ruid not found, dropping peer")
		p.Drop()
		return
	}
	provider := s.getProvider(w.stream)
	if provider == nil {
		// at this point of the message exchange unsupported providers are illegal. drop peer
		p.logError("unsupported provider", "stream", w.stream)
		p.Drop()
		return
	}

	w.to = &msg.LastIndex
	if lenHashes == 0 {
		// received zero hashes count, just fill the intervals, remove open want and request next range
		err := p.addInterval(w.stream, w.from, *w.to)
		if err != nil {
			p.logError("error persisting interval", "stream", w.stream, "from", w.from, "to", w.to, "err", err)
			return
		}
		p.mtx.Lock()
		delete(p.openWants, msg.Ruid)
		p.mtx.Unlock()
		cur, ok := p.getCursor(w.stream)
		if !ok {
			metrics.NewRegisteredCounter("network.stream.quit_unwanted.count", nil).Inc(1)
			p.logDebug("no longer interested in stream. quitting", "stream", w.stream)
			return
		}
		if w.head {
			if err := s.requestStreamHead(ctx, p, w.stream, msg.LastIndex+1); err != nil {
				streamRequestNextIntervalFail.Inc(1)
				p.logError("error requesting next interval from peer", "err", err)
				p.Drop()
				return
			}

		} else {
			if err := s.requestStreamRange(ctx, p, w.stream, cur); err != nil {
				streamRequestNextIntervalFail.Inc(1)
				p.logError("error requesting next interval from peer", "err", err)
				p.Drop()
				return
			}
		}
		return
	}

	want, err := bv.New(lenHashes / HashSize)
	if err != nil {
		p.logError("error initiaising bitvector", "len", lenHashes/HashSize, "ruid", msg.Ruid, "err", err)
		p.Drop()
		return
	}

	var ctr uint64 = 0

	for i := 0; i < lenHashes; i += HashSize {
		hash := hashes[i : i+HashSize]
		p.logTrace("checking offered hash", "ref", fmt.Sprintf("%x", hash))
		c := chunk.Address(hash)

		if _, wait := provider.NeedData(ctx, hash); wait != nil {
			ctr++
			w.hashes[c.Hex()] = true
			// set the bit, so create a request
			want.Set(i / HashSize)
			p.logTrace("need data", "ref", fmt.Sprintf("%x", hash), "request", true)
		} else {
			w.hashes[c.Hex()] = false
		}
	}
	cc := make(chan chunk.Chunk)
	dc := make(chan error)

	atomic.AddUint64(&w.remaining, ctr)
	w.bv = want
	w.chunks = cc
	w.done = dc

	var wantedHashesMsg WantedHashes

	errc := s.sealBatch(p, provider, w)

	if ctr == 0 && lenHashes == 0 {
		streamEmptyWantedHashes.Inc(1)
		wantedHashesMsg = WantedHashes{
			Ruid:      msg.Ruid,
			BitVector: []byte{},
		}
	} else {
		streamWantedHashes.Inc(1)
		wantedHashesMsg = WantedHashes{
			Ruid:      msg.Ruid,
			BitVector: want.Bytes(),
		}
	}

	p.logDebug("sending wanted hashes", "offered", lenHashes/HashSize, "want", ctr)
	if err := p.Send(ctx, wantedHashesMsg); err != nil {
		p.logError("error sending wanted hashes", "err", err)
		p.Drop()
		return
	}

	select {
	case err := <-errc:
		if err != nil {
			streamBatchFail.Inc(1)
			p.logError("got an error while sealing batch", "from", w.from, "to", w.to, "err", err)
			p.Drop()
			return
		}
		err = p.addInterval(w.stream, w.from, *w.to)
		if err != nil {
			p.logError("error persisting interval", "from", w.from, "to", w.to, "err", err)
			p.Drop()
			return
		}
		p.mtx.Lock()
		delete(p.openWants, msg.Ruid)
		p.mtx.Unlock()

		//TODO BATCH TIMEOUT?
	}
	cur, ok := p.getCursor(w.stream)
	if !ok {
		metrics.NewRegisteredCounter("network.stream.quit_unwanted.count", nil).Inc(1)
		p.logDebug("no longer interested in stream. quitting", "stream", w.stream)
		return
	}

	if w.head {
		if err := s.requestStreamHead(ctx, p, w.stream, msg.LastIndex+1); err != nil {
			streamRequestNextIntervalFail.Inc(1)
			p.logError("error requesting next interval from peer", "err", err)
			p.Drop()
			return
		}
	} else {
		if err := s.requestStreamRange(ctx, p, w.stream, cur); err != nil {
			streamRequestNextIntervalFail.Inc(1)
			p.logError("error requesting next interval from peer", "err", err)
			p.Drop()
			return
		}
	}
}

// handleWantedHashes is handled on the SERVER side and is dependent on a preceding OfferedHashes message
// the method is to ensure that all chunks in the requested batch is sent to the client
func (s *SlipStream) handleWantedHashes(ctx context.Context, p *Peer, msg *WantedHashes) {
	p.logDebug("peer.handleWantedHashes", "ruid", msg.Ruid)
	// Get the length of the original offer from state
	// get the offered hashes themselves
	p.mtx.Lock()
	offer, ok := p.openOffers[msg.Ruid]
	p.mtx.Unlock()
	if !ok {
		p.logError("ruid does not exist. dropping peer", "ruid", msg.Ruid)
		p.Drop()
		return
	}

	provider, ok := p.providers[offer.stream.Name]
	if !ok {
		p.logError("no provider found for stream, dropping peer", "stream", offer.stream)
		p.Drop()
		return
	}

	l := len(offer.hashes) / HashSize
	want, err := bv.NewFromBytes(msg.BitVector, l)
	if err != nil {
		p.logError("error initiaising bitvector", "l", l, "ll", len(offer.hashes), "err", err)
		panic("err")
		p.Drop()
		return
	}

	frameSize := 0
	const maxFrame = BatchSize
	cd := &ChunkDelivery{
		Ruid: msg.Ruid,
	}
	for i := 0; i < l; i++ {
		if want.Get(i) {
			metrics.GetOrRegisterCounter("peer.handlewantedhashesmsg.actualget", nil).Inc(1)
			hash := offer.hashes[i*HashSize : (i+1)*HashSize]
			data, err := provider.Get(ctx, hash)
			if err != nil {
				p.logError("handleWantedHashesMsg", "hash", hash, "err", err)
				p.Drop()
				return
			}

			chunkD := DeliveredChunk{
				Addr: hash,
				Data: data,
			}

			//collect the chunk into the batch
			frameSize++

			cd.Chunks = append(cd.Chunks, chunkD)
			if frameSize == maxFrame {
				//send the batch
				go func(cd *ChunkDelivery) {
					p.logDebug("sending chunk delivery")
					if err := p.Send(ctx, cd); err != nil {
						p.logError("error sending chunk delivery frame", "ruid", msg.Ruid, "error", err)
					}
				}(cd)
				frameSize = 0
				cd = &ChunkDelivery{
					Ruid: msg.Ruid,
				}
			}
		}
	}

	// send anything that we might have left in the batch
	if frameSize > 0 {
		if err := p.Send(ctx, cd); err != nil {
			p.logError("error sending chunk delivery frame", "ruid", msg.Ruid, "error", err)
		}
	}
}

func (s *SlipStream) handleChunkDelivery(ctx context.Context, p *Peer, msg *ChunkDelivery) {
	p.logDebug("peer.handleChunkDelivery", "chunks", len(msg.Chunks))
	processReceivedChunksCount.Inc(1)
	lastReceivedChunksMsg.Update(time.Now().UnixNano())

	p.mtx.RLock()
	w, ok := p.openWants[msg.Ruid]
	p.mtx.RUnlock()
	if !ok {
		p.logError("no open offers for for ruid", "ruid", msg.Ruid)
		p.Drop()
		return
	}

	p.logDebug("delivering chunks for peer", "chunks", len(msg.Chunks))
	for _, dc := range msg.Chunks {
		c := chunk.NewChunk(dc.Addr, dc.Data)
		p.logTrace("writing chunk to chunks channel", "caddr", c.Address())
		w.chunks <- c
	}
	p.logDebug("done writing batch to chunks channel")
}

func (s *SlipStream) sealBatch(p *Peer, provider StreamProvider, w *want) <-chan error {
	p.logDebug("stream.sealBatch", "ruid", w.ruid)
	errc := make(chan error)
	go func() {
		for {
			select {
			case c, ok := <-w.chunks:
				if !ok {
					p.logError("want chanks returned on !ok")
				}
				//p.mtx.Lock()
				//if wants, ok := want.hashes[c.Address().Hex()]; !ok || !wants {
				//log.Error("got an unwanted chunk from peer!", "peer", p.ID(), "caddr", c.Address)
				//}
				cc := chunk.NewChunk(c.Address(), c.Data())
				go func() {
					ctx := context.TODO()
					seen, err := provider.Put(ctx, cc.Address(), cc.Data())
					if err != nil {
						if err == storage.ErrChunkInvalid {
							p.Drop()
							return
						}
					}
					if seen {
						streamSeenChunkDelivery.Inc(1)
						p.logError("chunk already seen!", "caddr", c.Address()) //this is possible when the same chunk is asked from multiple peers
					}
					//p.mtx.Lock()
					//want.hashes[c.Address().Hex()] = false //todo: should by sync map
					//p.mtx.Unlock()
					v := atomic.AddUint64(&w.remaining, ^uint64(0))
					if v == 0 {
						close(errc)
						return
					}
				}()
			case <-p.quit:
				return
			}
		}
	}()
	return errc
}
func (s *SlipStream) collectBatch(ctx context.Context, p *Peer, provider StreamProvider, key interface{}, from, to uint64) (hashes []byte, f, t uint64, empty bool, err error) {
	p.logDebug("stream.collectBatch", "from", from, "to", to)
	batchStart := time.Now()

	descriptors, stop := provider.Subscribe(ctx, key, from, to)
	defer stop()

	const batchTimeout = 50 * time.Millisecond

	var (
		batch        []byte
		batchSize    int
		batchStartID *uint64
		batchEndID   uint64
		timer        *time.Timer
		timerC       <-chan time.Time
	)

	defer func(start time.Time) {
		metrics.GetOrRegisterResettingTimer("stream.collectBatch.total-time", nil).UpdateSince(start)
		metrics.GetOrRegisterCounter("stream.collectBatch.batch-size", nil).Inc(int64(batchSize))
		if timer != nil {
			timer.Stop()
		}
	}(batchStart)

	for iterate := true; iterate; {
		select {
		case d, ok := <-descriptors:
			if !ok {
				iterate = false
				break
			}
			batch = append(batch, d.Address[:]...)
			batchSize++
			if batchStartID == nil {
				// set batch start id only if
				// this is the first iteration
				batchStartID = &d.BinID
			}
			batchEndID = d.BinID
			if batchSize >= BatchSize {
				iterate = false
				metrics.GetOrRegisterCounter("stream.collectBatch.full-batch", nil).Inc(1)
				p.logTrace("pull subscription - batch size reached", "batchSize", batchSize, "batchStartID", *batchStartID, "batchEndID", batchEndID)
			}
			if timer == nil {
				timer = time.NewTimer(batchTimeout)
			} else {
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(batchTimeout)
			}
			timerC = timer.C
		case <-timerC:
			// return batch if new chunks are not received after some time
			iterate = false
			metrics.GetOrRegisterCounter("stream.collectBatch.timer-expire", nil).Inc(1)
			p.logTrace("pull subscription timer expired", "batchSize", batchSize, "batchStartID", batchStartID, "batchEndID", batchEndID)
		case <-p.quit:
			iterate = false
			p.logTrace("pull subscription - quit received", "batchSize", batchSize, "batchStartID", batchStartID, "batchEndID", batchEndID)
		case <-s.quit:
			iterate = false
			p.logTrace("pull subscription - shutting down")
		}
	}
	if batchStartID == nil {
		// if batch start id is not set, it means we timed out
		return nil, 0, 0, true, nil
	}
	return batch, *batchStartID, batchEndID, false, nil

}

func (s *SlipStream) Protocols() []p2p.Protocol {
	return []p2p.Protocol{
		{
			Name:    "bzz-stream",
			Version: 1,
			Length:  10 * 1024 * 1024,
			Run:     s.Run,
		},
	}
}

func (s *SlipStream) APIs() []rpc.API {
	return []rpc.API{
		{
			Namespace: "bzz-stream",
			Version:   "1.0",
			Service:   NewAPI(s),
			Public:    false,
		},
	}
}

// Additional public methods accessible through API for pss
type API struct {
	*SlipStream
}

func NewAPI(s *SlipStream) *API {
	return &API{SlipStream: s}
}

func (s *SlipStream) Start(server *p2p.Server) error {
	log.Debug("slip stream starting")

	return nil
}

func (s *SlipStream) Stop() error {
	log.Debug("slip stream stopping")
	s.mtx.Lock()
	defer s.mtx.Unlock()
	close(s.quit)
	for _, v := range s.providers {
		v.Close()
	}
	return nil
}