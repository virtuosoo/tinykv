package raft

import "log"

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

func (p *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if p.Match < n {
		p.Match = n
		updated = true
	}
	if p.Next < n+1 {
		p.Next = n + 1
	}
	return updated
}

func (p *Progress) maybeDecreTo(rejected, last uint64) bool {
	if p.Next-1 != rejected { //过期的消息
		return false
	}

	p.Next = min(rejected, last+1)

	if p.Next < 1 || p.Match >= p.Next {
		log.Panicf("next(%d), match(%d)", p.Next, p.Match)
	}
	return true
}
