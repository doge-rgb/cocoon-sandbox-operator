// Copyright 2026 The CocoonStack Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package execgw

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	sdk "github.com/cocoonstack/sandbox/sdk/go"
)

// The upstream agent-sandbox client addresses one router for the whole fleet
// and names its target in a header, so every handler below starts by resolving
// X-Sandbox-ID. That is also why the routes carry no sandbox in their path.
const sandboxIDHeader = "X-Sandbox-ID"

// ExecuteRequest is the body of POST /execute.
type ExecuteRequest struct {
	Command string            `json:"command"`
	Args    []string          `json:"args,omitempty"`
	Cwd     string            `json:"cwd,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	Stdin   string            `json:"stdin,omitempty"`
}

// ExecuteResponse is what the client reads back.
type ExecuteResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

// ListEntry describes one directory entry for GET /list/.
type ListEntry struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	IsDir bool   `json:"is_dir"`
	Size  int64  `json:"size"`
}

// AgentSandboxHandler serves the upstream data-plane routes. Mount it at the
// root of a listener the agent-sandbox SDK can reach — its tunnel strategy
// looks for a Service named sandbox-router-svc, and its DirectStrategy takes
// whatever URL the caller configures.
func (g *Gateway) AgentSandboxHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /execute", g.handleExecute)
	mux.HandleFunc("POST /upload", g.handleUpload)
	mux.HandleFunc("GET /download/{path...}", g.handleDownload)
	mux.HandleFunc("GET /list/{path...}", g.handleList)
	mux.HandleFunc("GET /exists/{path...}", g.handleExists)
	return mux
}

// open resolves the sandbox a request names, writing the error response itself
// when it cannot.
func (g *Gateway) open(w http.ResponseWriter, r *http.Request) (*sdk.Sandbox, bool) {
	id := r.Header.Get(sandboxIDHeader)
	sb, err := g.Open(r.Context(), id, bearer(r))
	if err == nil {
		return sb, true
	}
	code := http.StatusBadGateway
	switch {
	case errors.Is(err, ErrUnknownSandbox):
		code = http.StatusNotFound
	case id == "":
		code = http.StatusBadRequest
	case strings.Contains(err.Error(), "no credential"):
		code = http.StatusUnauthorized
	}
	writeErr(w, code, err.Error())
	return nil, false
}

func (g *Gateway) handleExecute(w http.ResponseWriter, r *http.Request) {
	var req ExecuteRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "malformed request body: "+err.Error())
		return
	}
	if req.Command == "" {
		writeErr(w, http.StatusBadRequest, "command is required")
		return
	}
	sb, ok := g.open(w, r)
	if !ok {
		return
	}
	ctx, cancel := withTimeout(r.Context())
	defer cancel()

	var stdout, stderr strings.Builder
	cmd := sdk.Cmd{
		Argv:   argvFor(req),
		Cwd:    req.Cwd,
		Env:    req.Env,
		Stdout: &stdout,
		Stderr: &stderr,
	}
	if req.Stdin != "" {
		cmd.Stdin = strings.NewReader(req.Stdin)
	}
	code, err := sb.Run(ctx, cmd)
	if err != nil {
		// A command that ran and failed is a result, not a transport error; only
		// a broken channel reaches here.
		writeErr(w, http.StatusBadGateway, "execute: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ExecuteResponse{
		Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: code,
	})
}

// argvFor turns an execute request into an argv. Upstream sends a whole shell
// command line in one string — its own docs use "cat /etc/hostname" — so a
// bare command has to reach a shell or the guest tries to exec a file whose
// name contains spaces and reports it missing. An explicit args list is this
// implementation's extension for callers that want no shell between them and
// the binary, and it is honored literally.
func argvFor(req ExecuteRequest) []string {
	if len(req.Args) > 0 {
		return append([]string{req.Command}, req.Args...)
	}
	return []string{"/bin/sh", "-lc", req.Command}
}

func (g *Gateway) handleUpload(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		writeErr(w, http.StatusBadRequest, "path query parameter is required")
		return
	}
	sb, ok := g.open(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	ctx, cancel := withTimeout(r.Context())
	defer cancel()
	if err := sb.WriteFile(ctx, path, body, nil); err != nil {
		writeErr(w, http.StatusBadGateway, "upload: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gateway) handleDownload(w http.ResponseWriter, r *http.Request) {
	sb, ok := g.open(w, r)
	if !ok {
		return
	}
	ctx, cancel := withTimeout(r.Context())
	defer cancel()
	data, err := sb.ReadFile(ctx, guestPath(r))
	if err != nil {
		writeErr(w, http.StatusNotFound, "download: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (g *Gateway) handleList(w http.ResponseWriter, r *http.Request) {
	sb, ok := g.open(w, r)
	if !ok {
		return
	}
	ctx, cancel := withTimeout(r.Context())
	defer cancel()
	dir := guestPath(r)
	entries, err := sb.ListDir(ctx, dir)
	if err != nil {
		writeErr(w, http.StatusNotFound, "list: "+err.Error())
		return
	}
	out := make([]ListEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, ListEntry{
			Name:  e.Name,
			Path:  joinGuest(dir, e.Name),
			IsDir: e.Kind == "dir",
			Size:  int64(e.Size),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (g *Gateway) handleExists(w http.ResponseWriter, r *http.Request) {
	sb, ok := g.open(w, r)
	if !ok {
		return
	}
	ctx, cancel := withTimeout(r.Context())
	defer cancel()
	_, err := sb.Stat(ctx, guestPath(r))
	writeJSON(w, http.StatusOK, map[string]bool{"exists": err == nil})
}

// guestPath recovers the in-guest path from a wildcard route. Go's pattern
// strips the leading slash from {path...}, and every path these clients send is
// absolute, so it goes back on.
func guestPath(r *http.Request) string {
	p := r.PathValue("path")
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

func joinGuest(dir, name string) string {
	return strings.TrimSuffix(dir, "/") + "/" + name
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"code": status, "message": msg})
}
