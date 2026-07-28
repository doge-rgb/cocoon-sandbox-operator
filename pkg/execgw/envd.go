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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"connectrpc.com/connect"
	sdk "github.com/cocoonstack/sandbox/sdk/go"

	fspb "github.com/doge-rgb/cocoon-sandbox-operator/pkg/execgw/envdspec/gen/filesystem"
	"github.com/doge-rgb/cocoon-sandbox-operator/pkg/execgw/envdspec/gen/filesystem/filesystemconnect"
	processpb "github.com/doge-rgb/cocoon-sandbox-operator/pkg/execgw/envdspec/gen/process"
	"github.com/doge-rgb/cocoon-sandbox-operator/pkg/execgw/envdspec/gen/process/processconnect"
)

// envdPort is the port an e2b client puts in the hostname it dials. The SDK
// builds "{port}-{sandboxID}.{domain}" and expects envd there, so the sandbox
// this request is for is carried in the Host header and nowhere else.
const envdPort = "49983"

// EnvdHandler serves the subset of envd's API the e2b SDK uses. Mount it on a
// listener that receives requests for "*.{domain}", because the only thing
// naming the target sandbox is the hostname.
func (g *Gateway) EnvdHandler() http.Handler {
	mux := http.NewServeMux()
	path, h := processconnect.NewProcessHandler(&processService{gw: g})
	mux.Handle(path, h)
	path, h = filesystemconnect.NewFilesystemHandler(&filesystemService{gw: g})
	mux.Handle(path, h)
	// The file routes are plain HTTP rather than RPC — that is how envd serves
	// them, and the SDK's read/write go here rather than through the service.
	mux.HandleFunc("GET /files", g.envdDownload)
	mux.HandleFunc("POST /files", g.envdUpload)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// sandboxFromHost recovers the sandbox id an e2b client encoded in the
// hostname. Everything before the first dot is "{port}-{sandboxID}"; the port
// prefix is what lets one sandbox expose several services, and only the envd
// one reaches here.
func sandboxFromHost(host string) string {
	if h, _, ok := strings.Cut(host, ":"); ok {
		host = h
	}
	label, _, _ := strings.Cut(host, ".")
	_, id, ok := strings.Cut(label, "-")
	if !ok {
		return ""
	}
	return id
}

// openEnvd binds the sandbox an envd request is for. The e2b client presents
// the token it was handed as envdAccessToken, so this surface needs nothing
// remembered — but it still accepts a remembered one, which is what makes a
// sandbox created through the Kubernetes API usable from an e2b client.
func (g *Gateway) openEnvd(ctx context.Context, host string, header http.Header) (*sdk.Sandbox, error) {
	r := &http.Request{Header: header}
	tok := bearer(r)
	id := sandboxFromHost(host)
	if id == "" {
		// A client pointed at a fixed gateway URL sends a host that names no
		// sandbox. Its credential still does.
		var ok bool
		if id, ok = g.sandboxForToken(tok); !ok {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("execgw: request host %q names no sandbox and its credential matches none; "+
					"address the gateway as {port}-{sandboxID}.{domain}", host))
		}
	}
	sb, err := g.Open(ctx, id, tok)
	if err != nil {
		if errors.Is(err, ErrUnknownSandbox) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return sb, nil
}

// --- process service ---------------------------------------------------------

type processService struct {
	processconnect.UnimplementedProcessHandler
	gw *Gateway
}

// streamWriter turns writes from the guest into stream events. The e2b client
// reads stdout and stderr as they arrive, so buffering the command to
// completion first would turn every interactive use into a silent wait.
type streamWriter struct {
	mu     sync.Mutex
	stream *connect.ServerStream[processpb.StartResponse]
	stderr bool
}

func (w *streamWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	data := &processpb.ProcessEvent_DataEvent{}
	if w.stderr {
		data.Output = &processpb.ProcessEvent_DataEvent_Stderr{Stderr: append([]byte(nil), p...)}
	} else {
		data.Output = &processpb.ProcessEvent_DataEvent_Stdout{Stdout: append([]byte(nil), p...)}
	}
	err := w.stream.Send(&processpb.StartResponse{Event: &processpb.ProcessEvent{
		Event: &processpb.ProcessEvent_Data{Data: data},
	}})
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *processService) Start(
	ctx context.Context,
	req *connect.Request[processpb.StartRequest],
	stream *connect.ServerStream[processpb.StartResponse],
) error {
	sb, err := s.gw.openEnvd(ctx, req.Header().Get("Host"), req.Header())
	if err != nil {
		return err
	}
	cfg := req.Msg.GetProcess()
	if cfg == nil || cfg.GetCmd() == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("execgw: no command in request"))
	}

	// The client waits for a start event before it will read anything else, and
	// it wants a pid. The guest reports one only when the process has actually
	// been spawned, which is after this point in the wire protocol, so the
	// stream opens with a placeholder: the id is the client's handle for kill
	// and stdin, both of which this surface routes by sandbox rather than pid.
	if err := stream.Send(&processpb.StartResponse{Event: &processpb.ProcessEvent{
		Event: &processpb.ProcessEvent_Start{Start: &processpb.ProcessEvent_StartEvent{Pid: 1}},
	}}); err != nil {
		return err
	}

	ctx, cancel := withTimeout(ctx)
	defer cancel()
	code, runErr := sb.Run(ctx, sdk.Cmd{
		Argv:   append([]string{cfg.GetCmd()}, cfg.GetArgs()...),
		Cwd:    cfg.GetCwd(),
		Env:    cfg.GetEnvs(),
		Stdout: &streamWriter{stream: stream},
		Stderr: &streamWriter{stream: stream, stderr: true},
	})
	end := &processpb.ProcessEvent_EndEvent{ExitCode: int32(code), Exited: true, Status: "exited"}
	if runErr != nil {
		// A command that exits non-zero is a result; only a broken channel is an
		// error, and the client needs to tell them apart to decide about retry.
		msg := runErr.Error()
		end.Error = &msg
		end.Exited = false
		end.Status = "error"
	}
	return stream.Send(&processpb.StartResponse{Event: &processpb.ProcessEvent{
		Event: &processpb.ProcessEvent_End{End: end},
	}})
}

// --- filesystem service ------------------------------------------------------

type filesystemService struct {
	filesystemconnect.UnimplementedFilesystemHandler
	gw *Gateway
}

func (s *filesystemService) Stat(
	ctx context.Context, req *connect.Request[fspb.StatRequest],
) (*connect.Response[fspb.StatResponse], error) {
	sb, err := s.gw.openEnvd(ctx, req.Header().Get("Host"), req.Header())
	if err != nil {
		return nil, err
	}
	info, err := sb.Stat(ctx, req.Msg.GetPath())
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}
	return connect.NewResponse(&fspb.StatResponse{Entry: &fspb.EntryInfo{
		Name: req.Msg.GetPath(),
		Type: entryType(info.Kind),
		Path: req.Msg.GetPath(),
		Size: int64(info.Size),
		Mode: info.Mode,
	}}), nil
}

func (s *filesystemService) ListDir(
	ctx context.Context, req *connect.Request[fspb.ListDirRequest],
) (*connect.Response[fspb.ListDirResponse], error) {
	sb, err := s.gw.openEnvd(ctx, req.Header().Get("Host"), req.Header())
	if err != nil {
		return nil, err
	}
	dir := req.Msg.GetPath()
	entries, err := sb.ListDir(ctx, dir)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}
	out := make([]*fspb.EntryInfo, 0, len(entries))
	for _, e := range entries {
		out = append(out, &fspb.EntryInfo{
			Name: e.Name,
			Type: entryType(e.Kind),
			Path: joinGuest(dir, e.Name),
			Size: int64(e.Size),
		})
	}
	return connect.NewResponse(&fspb.ListDirResponse{Entries: out}), nil
}

func (s *filesystemService) MakeDir(
	ctx context.Context, req *connect.Request[fspb.MakeDirRequest],
) (*connect.Response[fspb.MakeDirResponse], error) {
	sb, err := s.gw.openEnvd(ctx, req.Header().Get("Host"), req.Header())
	if err != nil {
		return nil, err
	}
	// Parents, because the client's MakeDir is expected to behave like mkdir -p:
	// its own filesystem helper creates nested paths in one call.
	if err := sb.Mkdir(ctx, req.Msg.GetPath(), true); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&fspb.MakeDirResponse{Entry: &fspb.EntryInfo{
		Name: req.Msg.GetPath(), Type: fspb.FileType_FILE_TYPE_DIRECTORY, Path: req.Msg.GetPath(),
	}}), nil
}

func (s *filesystemService) Remove(
	ctx context.Context, req *connect.Request[fspb.RemoveRequest],
) (*connect.Response[fspb.RemoveResponse], error) {
	sb, err := s.gw.openEnvd(ctx, req.Header().Get("Host"), req.Header())
	if err != nil {
		return nil, err
	}
	// Recursive, matching the client's Remove, which deletes a directory tree
	// without asking the caller to walk it first.
	if err := sb.Remove(ctx, req.Msg.GetPath(), true); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&fspb.RemoveResponse{}), nil
}

func (s *filesystemService) Move(
	ctx context.Context, req *connect.Request[fspb.MoveRequest],
) (*connect.Response[fspb.MoveResponse], error) {
	sb, err := s.gw.openEnvd(ctx, req.Header().Get("Host"), req.Header())
	if err != nil {
		return nil, err
	}
	if err := sb.Rename(ctx, req.Msg.GetSource(), req.Msg.GetDestination()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&fspb.MoveResponse{Entry: &fspb.EntryInfo{
		Name: req.Msg.GetDestination(), Path: req.Msg.GetDestination(),
	}}), nil
}

func entryType(kind string) fspb.FileType {
	if kind == "dir" {
		return fspb.FileType_FILE_TYPE_DIRECTORY
	}
	return fspb.FileType_FILE_TYPE_FILE
}

// --- /files ------------------------------------------------------------------

// envdSandboxID names the sandbox a plain-HTTP envd request is for, by hostname
// when the client was given a per-sandbox one and by credential otherwise.
func (g *Gateway) envdSandboxID(r *http.Request) string {
	if id := sandboxFromHost(r.Host); id != "" {
		return id
	}
	id, _ := g.sandboxForToken(bearer(r))
	return id
}

func (g *Gateway) envdDownload(w http.ResponseWriter, r *http.Request) {
	sb, err := g.Open(r.Context(), g.envdSandboxID(r), bearer(r))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	ctx, cancel := withTimeout(r.Context())
	defer cancel()
	data, err := sb.ReadFile(ctx, r.URL.Query().Get("path"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(data)
}

func (g *Gateway) envdUpload(w http.ResponseWriter, r *http.Request) {
	sb, err := g.Open(r.Context(), g.envdSandboxID(r), bearer(r))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	path := r.URL.Query().Get("path")
	body, err := uploadBody(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := withTimeout(r.Context())
	defer cancel()
	if err := sb.WriteFile(ctx, path, body, nil); err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, []map[string]string{{"path": path, "name": path}})
}

// uploadBody reads the file content out of a write request. The e2b client
// sends multipart, but a raw body is accepted too so the surface is usable with
// nothing more than curl.
func uploadBody(r *http.Request) ([]byte, error) {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
		return io.ReadAll(r.Body)
	}
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, fmt.Errorf("read upload: %w", err)
	}
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("upload carried no file part")
		}
		if err != nil {
			return nil, fmt.Errorf("read upload part: %w", err)
		}
		if part.FileName() != "" {
			return io.ReadAll(part)
		}
	}
}
