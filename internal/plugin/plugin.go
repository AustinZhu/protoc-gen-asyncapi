// Package plugin implements the protoc plugin protocol: it turns a
// CodeGeneratorRequest into a CodeGeneratorResponse holding one AsyncAPI
// document.
package plugin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/pluginpb"
	"gopkg.in/yaml.v3"

	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/jsonschema"
	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/options"
)

// Name is the plugin's name, as used in generated-file headers.
const Name = "protoc-gen-temporal-asyncapi"

// Version is the plugin version, overridden at release build time.
var Version = "dev"

// Run handles one request. Problems with the input (bad parameters, invalid
// annotations) are reported in the response's error field, which protoc and
// buf surface as a generation failure; no file is emitted in that case.
func Run(req *pluginpb.CodeGeneratorRequest) *pluginpb.CodeGeneratorResponse {
	resp := &pluginpb.CodeGeneratorResponse{
		SupportedFeatures: proto64(uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL | pluginpb.CodeGeneratorResponse_FEATURE_SUPPORTS_EDITIONS)),
		MinimumEdition:    proto32(int32(descriptorpb.Edition_EDITION_PROTO2)),
		MaximumEdition:    proto32(int32(descriptorpb.Edition_EDITION_2023)),
	}
	file, err := generate(req)
	if err != nil {
		msg := err.Error()
		resp.Error = &msg
		return resp
	}
	resp.File = []*pluginpb.CodeGeneratorResponse_File{file}
	return resp
}

func generate(req *pluginpb.CodeGeneratorRequest) (*pluginpb.CodeGeneratorResponse_File, error) {
	p, err := ParseParams(req.GetParameter())
	if err != nil {
		return nil, err
	}
	registry, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: req.GetProtoFile()})
	if err != nil {
		return nil, fmt.Errorf("reading descriptors: %w", err)
	}
	files, err := selectFiles(req, registry, p.Services)
	if err != nil {
		return nil, err
	}
	ops, err := options.Discover(files, registry)
	if err != nil {
		return nil, err
	}

	convOpts := jsonschema.Options{UseProtoNames: p.UseProtoNames}
	if p.WithProtovalidate {
		convOpts.Protovalidate = dynamicpb.NewTypes(registry)
	}
	conv := jsonschema.NewConverter(convOpts)

	pkg := firstPackage(ops, files)
	cfg := asyncapi.Config{
		Title:       p.Title,
		Version:     p.Version,
		Description: p.Description,
		ID:          p.ID,
		ServerURL:   p.ServerURL,
		Namespace:   p.Namespace,
		Perspective: p.Perspective,
	}
	if cfg.Title == "" {
		cfg.Title = pkg
		if cfg.Title == "" {
			cfg.Title = "Temporal"
		}
	}
	if cfg.ID == "" && pkg != "" {
		cfg.ID = "urn:temporal:" + pkg
	}
	if !p.TrimUnusedSchemas {
		cfg.Extra = declaredTypes(files)
	}
	doc, err := asyncapi.Build(ops, conv, cfg)
	if err != nil {
		return nil, err
	}
	content, err := marshal(doc, p.Format, files)
	if err != nil {
		return nil, err
	}
	return &pluginpb.CodeGeneratorResponse_File{Name: &p.Path, Content: &content}, nil
}

// selectFiles returns the files to scan: those protoc/buf asked to generate,
// or, when package globs are given, every file in the request whose package
// matches one (so dependencies can be documented too).
func selectFiles(req *pluginpb.CodeGeneratorRequest, registry *protoregistry.Files, globs []string) ([]protoreflect.FileDescriptor, error) {
	var files []protoreflect.FileDescriptor
	if len(globs) == 0 {
		for _, name := range req.GetFileToGenerate() {
			fd, err := registry.FindFileByPath(name)
			if err != nil {
				return nil, fmt.Errorf("file to generate %s: %w", name, err)
			}
			files = append(files, fd)
		}
		return files, nil
	}
	for _, fdp := range req.GetProtoFile() {
		for _, g := range globs {
			if matchPackage(g, fdp.GetPackage()) {
				fd, err := registry.FindFileByPath(fdp.GetName())
				if err != nil {
					return nil, err
				}
				files = append(files, fd)
				break
			}
		}
	}
	return files, nil
}

func firstPackage(ops []*options.Operation, files []protoreflect.FileDescriptor) string {
	if len(ops) > 0 {
		return ops[0].Package()
	}
	for _, f := range files {
		if f.Package() != "" {
			return string(f.Package())
		}
	}
	return ""
}

// declaredTypes lists every message and enum declared in files.
func declaredTypes(files []protoreflect.FileDescriptor) []protoreflect.Descriptor {
	var out []protoreflect.Descriptor
	var walk func(protoreflect.MessageDescriptors, protoreflect.EnumDescriptors)
	walk = func(msgs protoreflect.MessageDescriptors, enums protoreflect.EnumDescriptors) {
		for i := 0; i < enums.Len(); i++ {
			out = append(out, enums.Get(i))
		}
		for i := 0; i < msgs.Len(); i++ {
			md := msgs.Get(i)
			if md.IsMapEntry() {
				continue
			}
			out = append(out, md)
			walk(md.Messages(), md.Enums())
		}
	}
	for _, f := range files {
		walk(f.Messages(), f.Enums())
	}
	return out
}

func marshal(doc *asyncapi.Document, format string, files []protoreflect.FileDescriptor) (string, error) {
	var buf bytes.Buffer
	if format == "json" {
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		if err := enc.Encode(doc); err != nil {
			return "", err
		}
		return buf.String(), nil
	}
	fmt.Fprintf(&buf, "# Code generated by %s. DO NOT EDIT.\n", Name)
	if len(files) > 0 {
		names := make([]string, len(files))
		for i, f := range files {
			names[i] = f.Path()
		}
		fmt.Fprintf(&buf, "# source: %s\n", strings.Join(names, ", "))
	}
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return "", err
	}
	if err := enc.Close(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func proto64(v uint64) *uint64 { return &v }
func proto32(v int32) *int32   { return &v }
