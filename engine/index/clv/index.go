/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package clv

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
)

const (
	txPrefixPos = iota
	txPrefixTerm
	txPrefixDic
	txPrefixDicVersion
	txPrefixSid
	txPrefixId
	txSuffix = 9
)

const (
	qmin        = 1
	posFlag     = 1
	idFlag      = 2
	maxBuf      = 50000
	maxItemSize = 64 * 1024
)

type InvertState struct {
	timestamp int64
	pos       uint16
}

type InvertedIndex struct {
	iverteState []InvertState
}

func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		iverteState: make([]InvertState, 0),
	}
}

type TrieNode struct {
	children      map[string]*TrieNode
	invertedIndex map[uint64]*InvertedIndex
	ids           map[uint32]struct{}
}

func NewTrieNode() *TrieNode {
	return &TrieNode{
		children:      make(map[string]*TrieNode),
		invertedIndex: make(map[uint64]*InvertedIndex),
		ids:           make(map[uint32]struct{}),
	}
}

type Options struct {
	Path        string
	Measurement string
	Field       string
}

type TokenIndex struct {
	tb          *mergeset.Table
	root        *TrieNode
	analyzer    *Analyzer
	trieLock    sync.RWMutex
	closing     chan struct{}
	signal      chan struct{}
	path        string
	measurement string
	field       string
	docNum      uint32
}

func NewTokenIndex(opts *Options) (*TokenIndex, error) {
	idx := &TokenIndex{
		root:        NewTrieNode(),
		closing:     make(chan struct{}),
		signal:      make(chan struct{}),
		path:        opts.Path,
		measurement: opts.Measurement,
		field:       opts.Field,
	}

	// open token index
	err := idx.Open()
	if err != nil {
		return nil, err
	}

	// get dictionary version
	var version uint32
	version, err = idx.searchDicVersion()
	if err != nil {
		return nil, err
	}

	// get a analyzer
	dirs := strings.Split(opts.Path, "/")
	analyzerPath := ""
	for i := 0; i < len(dirs)-2; i++ {
		analyzerPath = analyzerPath + dirs[i] + "/"
	}
	analyzerPath = analyzerPath + "/directory"
	idx.analyzer, err = GetAnalyzer(analyzerPath, opts.Measurement, opts.Field, version)
	if err != nil {
		return nil, err
	}

	// write version to mergeset table
	if version == Blank {
		idx.writeDicVersion(idx.analyzer.Version())
	}

	return idx, nil
}

func (idx *TokenIndex) Open() error {
	tbPath := path.Join(idx.path, idx.measurement, idx.field)
	tb, err := mergeset.OpenTable(tbPath, nil, nil)
	if err != nil {
		return fmt.Errorf("cannot open text index:%s, err: %+v", tbPath, err)
	}
	idx.tb = tb

	// start a process routine
	go idx.process()

	return nil
}

func (idx *TokenIndex) Close() error {
	if idx.signal != nil {
		close(idx.signal)
	}
	if idx.closing != nil {
		idx.closing <- struct{}{}
		close(idx.closing)
	}
	return nil
}

func (idx *TokenIndex) insertInvertedIndex(node *TrieNode, tsid uint64, timestamp int64, position uint16) {
	invertedIndex, ok := node.invertedIndex[tsid]
	if !ok {
		invertedIndex = NewInvertedIndex()
		node.invertedIndex[tsid] = invertedIndex
	}

	state := InvertState{timestamp, position}
	invertedIndex.iverteState = append(invertedIndex.iverteState, state)
}

func (idx *TokenIndex) insertTrieNode(vtoken []string, tsid uint64, timestamp int64, position uint16) error {
	idx.trieLock.Lock()
	idx.docNum++
	node := idx.root
	for _, token := range vtoken {
		child, ok := node.children[token]
		if !ok {
			child = NewTrieNode()
			node.children[token] = child
		}
		node = child
	}
	idx.trieLock.Unlock()

	idx.insertInvertedIndex(node, tsid, timestamp, position)
	return nil
}

func (idx *TokenIndex) insertSuffixToTrie(vtoken []string, id uint32) error {
	idx.trieLock.Lock()
	node := idx.root
	for _, token := range vtoken {
		child, ok := node.children[token]
		if !ok {
			child = NewTrieNode()
			node.children[token] = child
		}
		node = child
	}

	if _, ok := node.ids[id]; !ok {
		node.ids[id] = struct{}{}
	}
	idx.trieLock.Unlock()

	return nil
}

func (idx *TokenIndex) AddDocument(log string, tsid uint64, timestamp int64) error {
	// tokenizer analyze
	tokens, err := idx.analyzer.Analyze(log)
	if err != nil {
		return err
	}
	for position, vtoken := range tokens {
		idx.insertTrieNode(vtoken.tokens, tsid, timestamp, position)
		if len(vtoken.tokens) <= qmin {
			continue
		}
		// v-token suffix insert
		oriVtoken := ""
		for i := 0; i < len(vtoken.tokens); i++ {
			oriVtoken += vtoken.tokens[i] + " "
		}

		for i := 1; i < len(vtoken.tokens); i++ {
			idx.insertSuffixToTrie(vtoken.tokens[i:], vtoken.id)
		}
	}

	if idx.docNum >= maxBuf {
		idx.signal <- struct{}{}
	}

	return nil
}

var idxItemsPool mergeindex.IndexItemsPool

func (idx *TokenIndex) writeDicVersion(version uint32) {
	return
}

func (idx *TokenIndex) marshalPosList(dst []byte, node *TrieNode) []byte {
	// len(sid)
	dst = encoding.MarshalUint32(dst, uint32(len(node.invertedIndex)))

	for sid, iindex := range node.invertedIndex {
		// prefixSid + sid + len(pos) + timelist + positionlist
		// sort by timestamp
		istate := iindex.iverteState
		sort.Slice(istate, func(i, j int) bool {
			return istate[i].timestamp < istate[j].timestamp
		})
		dst = append(dst, txPrefixSid)
		dst = encoding.MarshalVarUint64(dst, sid)
		dst = encoding.MarshalUint32(dst, uint32(len(istate)))
		posbuffer := make([]byte, 2*len(istate))
		for i := 0; i < len(istate); i++ {
			dst = encoding.MarshalInt64(dst, istate[i].timestamp)
			posbuffer = encoding.MarshalUint16(posbuffer, istate[i].pos)
		}
		dst = append(dst, posbuffer...)
	}
	return dst
}

func (idx *TokenIndex) marshalIdList(dst []byte, node *TrieNode) []byte {
	dst = append(dst, txPrefixId)
	ids := make([]uint32, len(node.ids))
	for id := range node.ids {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	for i := 0; i < len(ids); i++ {
		dst = encoding.MarshalUint32(dst, ids[i])
	}

	return dst
}

func (idx *TokenIndex) createIndex(vtoken string, flag uint8, node *TrieNode) error {
	ii := idxItemsPool.Get()
	defer idxItemsPool.Put(ii)

	// write txPrefixPos + vtokens + suffix + flag
	ii.B = append(ii.B, txPrefixPos)
	ii.B = append(ii.B, []byte(vtoken)...)
	ii.B = append(ii.B, txSuffix)
	ii.B = append(ii.B, flag)

	// write posList
	if flag&posFlag != 0 {
		ii.B = idx.marshalPosList(ii.B, node)
	}

	// write idlist
	if flag&idFlag != 0 {
		ii.B = idx.marshalIdList(ii.B, node)
	}
	ii.Next()
	// write to mergeset
	return idx.tb.AddItems(ii.Items)
}

func (idx *TokenIndex) writeDocumentIndex(vtoken string, node *TrieNode) {
	var flag uint8
	if len(node.invertedIndex) != 0 {
		flag = flag | posFlag
	}
	if len(node.ids) != 0 {
		flag = flag | idFlag
	}
	if flag != 0 {
		idx.createIndex(vtoken, flag, node)
		return
	}
	// traversal of trees.
	for token, child := range node.children {
		vtokens := vtoken + token + " "
		idx.writeDocumentIndex(vtokens, child)
	}
}

func (idx *TokenIndex) processDocument() {
	// replace the root node
	idx.trieLock.Lock()
	if len(idx.root.children) == 0 {
		idx.trieLock.Unlock()
		return
	}
	node := idx.root
	idx.docNum = 0
	idx.root = NewTrieNode()
	idx.trieLock.Unlock()

	// Deal the first level node of the tree.
	for token, child := range node.children {
		vtokens := token + " "
		idx.writeDocumentIndex(vtokens, child)
	}

}

func (idx *TokenIndex) process() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-idx.closing:
			return
		case <-idx.signal:
		case <-ticker.C:
			idx.processDocument()
		}
	}
}

var clvSearchPool sync.Pool

func (idx *TokenIndex) getClvSearch() *tokenSearch {
	v := clvSearchPool.Get()
	if v == nil {
		v = &tokenSearch{
			idx: idx,
		}
	}

	is := v.(*tokenSearch)
	is.ts.Init(idx.tb)
	is.idx = idx

	return is
}

func (idx *TokenIndex) putClvSearch(is *tokenSearch) {
	is.kb.Reset()
	is.ts.MustClose()
	is.idx = nil
	clvSearchPool.Put(is)
}

func (idx *TokenIndex) searchDicVersion() (uint32, error) {
	cs := idx.getClvSearch()
	dicVersion := cs.searchDicVersion()

	idx.putClvSearch(cs)
	return dicVersion, nil
}
