// Copyright (C) 2020 Algorand, Inc.
// This file is part of go-algorand
//
// go-algorand is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// go-algorand is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with go-algorand.  If not, see <https://www.gnu.org/licenses/>.

package ledger

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/transactions"
)

// TestBuildDepGroupsAllDepend is a simple test where all txns share a common address and become one dependency group
func TestBuildDepGroupsAllDepend(t *testing.T) {
	//paysetgroups := make([][]transactions.SignedTxnWithAD, 0, 20)
	testtxns := make([]transactions.SignedTxnWithAD, 8)
	a1 := basics.Address{}
	a1[0] = 1
	testtxns[0].Txn.Sender = a1
	testtxns[1].Txn.Receiver = a1
	testtxns[2].Txn.CloseRemainderTo = a1
	testtxns[3].Txn.AssetReceiver = a1
	testtxns[4].Txn.AssetCloseTo = a1
	testtxns[5].Txn.AssetSender = a1
	testtxns[6].Txn.FreezeAccount = a1
	testtxns[7].Txn.Accounts = []basics.Address{a1}
	for i := range testtxns {
		testtxns[i].Txn.FirstValid = basics.Round(i)
	}

	txgroups := oneTxnGroups(testtxns)
	depgroups := buildDepGroups(txgroups)

	if len(depgroups) != 1 {
		for i, dg := range depgroups {
			t.Logf("dg[%d] len=%d", i, len(dg.paysetgroups))
		}
		t.Fail()
	}
}

// turn a list of txns into one-txn-groups
func oneTxnGroups(txns []transactions.SignedTxnWithAD) (txgroups [][]transactions.SignedTxnWithAD) {
	txgroups = make([][]transactions.SignedTxnWithAD, len(txns))
	for i, t := range txns {
		txgroups[i] = make([]transactions.SignedTxnWithAD, 1)
		txgroups[i][0] = t
	}
	return
}

// TestBuildDepGroupsPerAddr For a bunch of fake txns with one addr in each, one group per addr.
func TestBuildDepGroupsPerAddr(t *testing.T) {
	numtxns := 1000
	numaddrs := (numtxns / 10) + 2
	addrs := make([]basics.Address, numaddrs)
	for i := range addrs {
		binary.LittleEndian.PutUint64(addrs[i][:], uint64(i+1))
	}

	txns := make([]transactions.SignedTxnWithAD, numtxns)
	txgroups := make([][]transactions.SignedTxnWithAD, numtxns)
	for i := 0; i < numtxns; i++ {
		// TODO: set a random field instead of always Sender?
		txns[i].Txn.Sender = addrs[i%numaddrs]
		txgroups[i] = txns[i : i+1]
	}

	depgroups := buildDepGroups(txgroups)
	require.Equal(t, numaddrs, len(depgroups))
}

func BenchmarkBuildDepGroups(b *testing.B) {
	//debugLogf = b
	numtxns := b.N
	numaddrs := (numtxns / 10) + 2
	addrs := make([]basics.Address, numaddrs)
	for i := range addrs {
		binary.LittleEndian.PutUint64(addrs[i][:], uint64(i+1))
	}

	txns := make([]transactions.SignedTxnWithAD, numtxns)
	txgroups := make([][]transactions.SignedTxnWithAD, numtxns)
	for i := 0; i < numtxns; i++ {
		// TODO: set a random field instead of always Sender?
		txns[i].Txn.Sender = addrs[i%numaddrs]
		txgroups[i] = txns[i : i+1]
	}

	start := time.Now()
	b.ResetTimer()
	depgroups := buildDepGroups(txgroups)
	dt := time.Now().Sub(start)
	b.Logf("%d groups built from %d addrs in %d txns in %s", len(depgroups), numaddrs, numtxns, dt)
}
