// Package gmaj implements the Chord distributed hash table.
//
// TODO
//
// 1. Port over tests from original code
//
// 2. Timeout connections and make an RPC function for disconnecting nodes to
// remove themselves from the connMap of other nodes
//
// 3. Make sure nil values in RPC methods are handled properly
//
// 4. Logging
//
// 5. Dockerize
//
// 6. Easy deployment
//
// 7. Write example app that just uses client
package gmaj
