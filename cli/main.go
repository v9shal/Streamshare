package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http" // Add this!
	"os"
)

func main() {
	// 1. Check if it's a keyboard (Your existing code)
	fi, err := os.Stdin.Stat()
	if err != nil {
		fmt.Println(err)
	}
	if (fi.Mode() & os.ModeCharDevice) != 0 {
		fmt.Println("Error: Please pipe data into this command. Example: echo 'hello' | streamshare")
		os.Exit(1)
	}

	// ==========================================
	// 2. NEW: Get a Room from the Server!
	// ==========================================
	// a. Use http.Get("http://localhost:8080/create")
	// b. Check for error (if err != nil)
	// c. Don't forget to defer closing the body! (defer resp.Body.Close())
	// d. Read the body using io.ReadAll(resp.Body)
	// e. Print the body out to the user nicely so they know their link!

	resp, err := http.Get("http://localhost:8080/create")
	if err != nil {
		fmt.Println("error wile fetching the room code", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	link, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("error wile parsing  the response", err)
		os.Exit(1)
	}
	fmt.Println(string(link))

	// ==========================================
	// 3. Scoop up the Logs (Your existing code)
	// ==========================================
	reader := bufio.NewReader(os.Stdin)
	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Println("Error reading:", err)
			break
		}
		fmt.Print(string(buf[:n]))
	}
}
