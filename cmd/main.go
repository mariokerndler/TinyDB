package main

import (
	"TinySQL/internal/db" // Assuming TinySQL/internal/db is the correct path to your database package
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/chzyer/readline" // Import the readline library
)

func main() {
	// Initialize your database engine
	engine := db.NewEngine("data.log")

	fmt.Println("Welcome to TinySQL! Type 'QUIT' or 'EXIT' to exit.")

	// Configure readline
	// HistoryFile can be set to store command history across sessions.
	// AutoComplete can be used for command suggestions, but is not implemented here.
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "tinysql> ",
		HistoryFile:     "/tmp/tinysql_history.txt", // Store history in a temporary file
		InterruptPrompt: "^C",                       // Text shown when Ctrl+C is pressed
		EOFPrompt:       "exit",                     // Text shown when Ctrl+D is pressed
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize readline: %v\n", err)
		os.Exit(1)
	}
	defer rl.Close() // Ensure readline resources are cleaned up on exit

	for {
		// Readline handles the prompt and input, including arrow key history
		line, err := rl.Readline()

		if err == io.EOF { // Ctrl+D pressed
			fmt.Println("Bye!")
			break
		}
		if err == readline.ErrInterrupt { // Ctrl+C pressed
			// Clear the current line and continue to the next prompt, or exit if pressed again
			if line == "" { // If Ctrl+C is pressed on an empty line, exit
				fmt.Println("Bye!")
				break
			} else { // If Ctrl+C is pressed with text, clear the text but stay in loop
				continue
			}
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			break
		}

		input := strings.TrimSpace(line) // Trim whitespace from the input

		if input == "" { // Don't execute empty commands
			continue
		}

		// Handle exit commands
		if strings.EqualFold(input, "QUIT") || strings.EqualFold(input, "EXIT") {
			fmt.Println("Bye!")
			break
		}

		// Execute the command using your engine
		result := engine.Execute(input)
		fmt.Println(result)
	}
}
