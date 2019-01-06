package com.nebtrx.raft

// Entry should contain more log related data in order to handle log replication
case class Entry(term: Term, leaderId: MemberId)
