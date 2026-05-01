// Module for the stdlib-only note-backend-http. Deliberately
// separate go.mod: the backend imports nothing beyond Go's standard
// library, so isolating it from the rest of the repo's module graph
// makes the "stdlib-only" claim easy to verify (go mod tidy here
// should pull zero external dependencies).
module github.com/cheeseandcereal/aggexp/experiments/0026-http-json-backend-transport/backend-note

go 1.24
