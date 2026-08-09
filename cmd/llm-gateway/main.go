// Command llm-gateway is the campus LLM API gateway: it terminates
// OpenAI-compatible requests from student code, authenticates API keys,
// enforces usage limits, and forwards to the configured upstream model
// server.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "llm-gateway: gateway is not implemented yet")
	os.Exit(1)
}
