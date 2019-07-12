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

package retrieve

import "github.com/ethersphere/swarm/storage"

// RetrieveRequestMsg is the protocol msg for chunk retrieve requests
type RetrieveRequest struct {
	Addr storage.Address
}

//Chunk delivery always uses the same message type....
type ChunkDelivery struct {
	Addr  storage.Address
	SData []byte // the stored chunk Data (incl size)
	//peer  *Peer  // set in handleChunkDeliveryMsg
}

// ChunkDelivery delivers a frame of chunks in response to a WantedHashes message
//type ChunkDelivery struct {
//Ruid uint
//LastIndex uint
//Chunks []DeliveredChunk
//}

// DeliveredChunk encapsulates a particular chunk's underlying data within a ChunkDelivery message
//type DeliveredChunk struct {
//Addr storage.Address //chunk address
//Data []byte          //chunk data
//}
