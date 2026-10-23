package server

import (
	"bytes"
	"encoding/json"
	"io"
)

// TOKEN models keep one tool profile whichever upstream serves the request.
// These tools describe client-executed functions; the commercial tool grants
// belong to the CREDIT axis and cannot authorize work on a shared credential.
func fenceTokenTools(params map[string]json.RawMessage) *apiError {
	names := make(map[string]bool)
	if raw := params[toolsField]; !nullOrAbsent(raw) {
		var tools []json.RawMessage
		if json.Unmarshal(raw, &tools) != nil {
			return invalidTokenToolField(toolsField)
		}
		for _, rawTool := range tools {
			name, ok := tokenFunctionTool(rawTool, false)
			if !ok {
				return invalidTokenToolField(toolsField)
			}
			names[name] = true
		}
	}
	raw := params[toolChoiceField]
	if nullOrAbsent(raw) {
		return nil
	}
	var mode string
	if json.Unmarshal(raw, &mode) == nil {
		if mode == "auto" || mode == "none" || mode == "required" {
			return nil
		}
		return invalidTokenToolField(toolChoiceField)
	}
	choice, ok := tokenToolObject(raw)
	if !ok {
		return invalidTokenToolField(toolChoiceField)
	}
	var kind string
	if json.Unmarshal(choice["type"], &kind) != nil {
		return invalidTokenToolField(toolChoiceField)
	}
	if kind == "function" {
		name, valid := tokenFunctionTool(raw, true)
		if !valid || !names[name] {
			return invalidTokenToolField(toolChoiceField)
		}
		return nil
	}
	// Chat nests allowed_tools below its type, unlike the Responses shape.
	// Every entry is a reference to an already declared client function.
	if kind != allowedToolsChoice || !tokenToolFields(choice, "type", allowedToolsChoice) {
		return invalidTokenToolField(toolChoiceField)
	}
	allowed, ok := tokenToolObject(choice[allowedToolsChoice])
	if !ok || !tokenToolFields(allowed, "mode", toolsField) {
		return invalidTokenToolField(toolChoiceField)
	}
	if json.Unmarshal(allowed["mode"], &mode) != nil || (mode != "auto" && mode != "required") {
		return invalidTokenToolField(toolChoiceField)
	}
	var refs []json.RawMessage
	if json.Unmarshal(allowed[toolsField], &refs) != nil || len(refs) == 0 {
		return invalidTokenToolField(toolChoiceField)
	}
	for _, ref := range refs {
		name, valid := tokenFunctionTool(ref, true)
		if !valid || !names[name] {
			return invalidTokenToolField(toolChoiceField)
		}
	}
	return nil
}

func invalidTokenToolField(field string) *apiError {
	e := errInvalidParamValue(field)
	e.message = field + "에는 클라이언트가 실행하는 함수 도구(type: function)와 올바른 함수 선택만 사용할 수 있습니다. 서버 도구나 확장 필드를 제거하고 함수 정의를 확인해주세요."
	return &e
}

func nullOrAbsent(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// Only the protocol objects are inspected. Function parameters are JSON Schema
// data, so names such as model/provider/tools and examples remain untouched.
func tokenFunctionTool(raw json.RawMessage, reference bool) (string, bool) {
	tool, ok := tokenToolObject(raw)
	if !ok || !tokenToolFields(tool, "type", "function") {
		return "", false
	}
	var kind string
	if json.Unmarshal(tool["type"], &kind) != nil || kind != "function" {
		return "", false
	}
	fn, ok := tokenToolObject(tool["function"])
	if !ok {
		return "", false
	}
	if reference {
		if !tokenToolFields(fn, "name") {
			return "", false
		}
	} else if !tokenToolFields(fn, "name", "description", "parameters", "strict") {
		return "", false
	}
	var name string
	if json.Unmarshal(fn["name"], &name) != nil || !tokenFunctionName(name) {
		return "", false
	}
	if raw, present := fn["description"]; present {
		var description string
		if json.Unmarshal(raw, &description) != nil {
			return "", false
		}
	}
	if raw := fn["parameters"]; !nullOrAbsent(raw) {
		// The enclosing decoder already validated the complete JSON value.
		// Inspect its outer type without interpreting schema properties.
		if bytes.TrimSpace(raw)[0] != '{' {
			return "", false
		}
	}
	if raw, present := fn["strict"]; present {
		switch string(bytes.TrimSpace(raw)) {
		case "true", "false", "null":
		default:
			return "", false
		}
	}
	return name, true
}

func tokenFunctionName(name string) bool {
	if len(name) == 0 || len(name) > 64 {
		return false
	}
	for _, c := range []byte(name) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func tokenToolFields(object map[string]json.RawMessage, allowed ...string) bool {
	for name := range object {
		found := false
		for _, candidate := range allowed {
			if name == candidate {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// The request retains nested RawMessages when it is forwarded. Reject duplicate
// protocol keys, including escaped spellings, rather than validating one value
// while an upstream could read another. This does not walk schema data.
func tokenToolObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	d := json.NewDecoder(bytes.NewReader(raw))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return nil, false
	}
	object := make(map[string]json.RawMessage)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return nil, false
		}
		name, ok := token.(string)
		if !ok {
			return nil, false
		}
		if _, exists := object[name]; exists {
			return nil, false
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return nil, false
		}
		object[name] = value
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') {
		return nil, false
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, false
	}
	return object, true
}
