package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type RpcService struct {
	httpClient *http.Client
}

func NewRpcService() *RpcService {
	return &RpcService{
		httpClient: &http.Client{},
	}
}

type RequestOptions struct {
	ID             int
	JSONRPCVersion string
	Timeout        time.Duration
	Headers        map[string]string
}

func DefaultRequestOptions() *RequestOptions {
	return &RequestOptions{
		ID:             1,
		JSONRPCVersion: "2.0",
		Timeout:        60 * time.Second,
		Headers:        map[string]string{},
	}
}

func (o *RequestOptions) orDefault() *RequestOptions {
	if o == nil {
		return DefaultRequestOptions()
	}
	filled := *o
	if filled.ID == 0 {
		filled.ID = 1
	}
	if filled.JSONRPCVersion == "" {
		filled.JSONRPCVersion = "2.0"
	}
	if filled.Timeout == 0 {
		filled.Timeout = 60 * time.Second
	}
	if filled.Headers == nil {
		filled.Headers = map[string]string{}
	}
	return &filled
}

type jsonRPCRequest struct {
	Method  string      `json:"method"`
	JSONRPC string      `json:"jsonrpc"`
	Params  interface{} `json:"params,omitempty"`
	ID      int         `json:"id"`
}

func (c *RpcService) SendRequest(ctx context.Context, rawURL, method string, params interface{}, opts *RequestOptions) (json.RawMessage, error) {
	opts = opts.orDefault()

	reqBody := jsonRPCRequest{
		Method:  method,
		JSONRPC: opts.JSONRPCVersion,
		Params:  params,
		ID:      opts.ID,
	}

	buf, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("solrpc: marshal request: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, rawURL, bytes.NewReader(buf))
	if err != nil {
		return nil, fmt.Errorf("solrpc: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for k, v := range opts.Headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("solrpc: read response body: %w", err)
	}
	if len(respBytes) == 0 {
		return nil, fmt.Errorf("body is null")
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(respBytes, &body); err != nil {
		return nil, fmt.Errorf("solrpc: unmarshal response body: %w", err)
	}

	if errRaw, ok := body["error"]; ok && string(errRaw) != "null" {
		return nil, fmt.Errorf("%s", errRaw)
	}

	idRaw, ok := body["id"]
	if !ok {
		return nil, fmt.Errorf("id not found in body, %s", respBytes)
	}
	if _, ok := body["jsonrpc"]; !ok {
		return nil, fmt.Errorf("jsonrpc not found in body, %s", respBytes)
	}

	var respID int
	if err := json.Unmarshal(idRaw, &respID); err != nil {
		return nil, fmt.Errorf("id response not match id request, %s", respBytes)
	}
	if respID != opts.ID {
		return nil, fmt.Errorf("id response not match id request, %s", respBytes)
	}

	return body["result"], nil
}
