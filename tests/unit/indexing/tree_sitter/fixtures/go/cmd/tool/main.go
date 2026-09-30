package main

import "example.com/app/internal/store"

func main() {
	db, _ := store.Open("x")
	defer db.Close()
	run(db)
}

func run(db *store.DB) {}

func (s *Server) Serve() { run(nil) }
