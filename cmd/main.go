package main

import (
	"TinySQL/internal/db"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	engine := db.NewEngine("data.log")

	fmt.Println("Welcome to TinySQL! Type 'QUIT' or 'EXIT' to exit.")
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("tinysql> ")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()

		if strings.EqualFold(input, "QUIT") || strings.EqualFold(input, "EXIT") {
			fmt.Println("Bye!")
			break
		}

		result := engine.Execute(input)
		fmt.Println(result)
	}
}
