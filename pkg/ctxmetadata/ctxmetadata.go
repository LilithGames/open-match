package ctxmetadata

import (
	"context"
	"fmt"
	"strings"
)

// MD metadata
type MD map[string]string

func (md MD) GetString(key string) string {
	if val, ok := md[key]; ok {
		return val
	}
	return ""
}

type mdKey struct{}

// internal keys of MD
const (
	KeyNonBlocking = "_md_nb"
)

func NonBlocking() MD {
	return New(map[string]string{
		KeyNonBlocking: "t",
	})
}

func IsNonBlocking(ctx context.Context) bool {
	md := FromContext(ctx)
	if v, ok := md[KeyNonBlocking]; ok && v != "" {
		return true
	}
	return false
}

// New returns a md
func New(m map[string]string) MD {
	md := MD{}
	for k, c := range m {
		md[k] = c
	}
	return md
}

// FromContext get md from context
func FromContext(ctx context.Context) MD {
	md, ok := ctx.Value(mdKey{}).(MD)
	if ok {
		return md
	}
	return MD{}
}

// EFromContext get md from context may error
func EFromContext(ctx context.Context) (MD, bool) {
	md, ok := ctx.Value(mdKey{}).(MD)
	if ok {
		return md, true
	}
	return MD{}, false
}

// ToContext inject md from context
func ToContext(parent context.Context, md MD) context.Context {
	return context.WithValue(parent, mdKey{}, md)
}

// AppendContext append new md to md in context
func AppendContext(ctx context.Context, md MD) context.Context {
	return ToContext(ctx, Joins(FromContext(ctx), md))
}

// Pairs returns an MD formed by the mapping of key, value ...
func Pairs(kv ...string) MD {
	if len(kv)%2 == 1 {
		panic(fmt.Sprintf("metadata: Pairs got the odd number of input pairs for metadata: %d", len(kv)))
	}
	md := MD{}
	var key string
	for i, s := range kv {
		if i%2 == 0 {
			key = s
			continue
		}
		md[key] = s
	}
	return md
}

// Joins joins any number of mds into a single MD.
func Joins(mds ...MD) MD {
	out := MD{}
	for _, md := range mds {
		for k, v := range md {
			out[k] = v
		}
	}
	return out
}

// Value grpc HTTP2 spec all header keys should be lowercase
func Value(rpcCtx context.Context, key string) (value string, ok bool) {
	md := FromContext(rpcCtx)
	// grpc HTTP2 spec all header keys should be lowercase
	keys := []string{key, strings.ToLower(key)}
	for _, key := range keys {
		if val, ok := md[key]; ok {
			return val, true
		}
	}

	return "", false
}
