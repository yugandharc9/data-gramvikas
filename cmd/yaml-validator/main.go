package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func main() {
	if len(os.Args) < 2 {
		return
	}

	hasError := false

	for _, file := range os.Args[1:] {
		data, err := os.ReadFile(file)
		if err != nil {
			fmt.Printf("❌ %s: %v\n", file, err)
			hasError = true
			continue
		}

		var node yaml.Node
		if err := yaml.Unmarshal(data, &node); err != nil {
			fmt.Printf("❌ %s: invalid YAML (%v)\n", file, err)
			hasError = true
		} else {
			fmt.Printf("✅ %s valid YAML\n", file)
		}
	}

	if hasError {
		os.Exit(1)
	}
}
