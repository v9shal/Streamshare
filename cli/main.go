package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
)

func main() {
	fi, err := os.Stdin.Stat()
	if err != nil {
		fmt.Println(err)
	}
	if (fi.Mode() & os.ModeCharDevice) != 0 {
		fmt.Println("Error: Please pipe data into this command. Example: echo 'hello' | streamshare")
		os.Exit(1)
	}
	host := os.Getenv("STREAMSHARE_HOST")
	if host == "" {
		host = "localhost:8080"
	}
	httpURL := fmt.Sprintf("http://%s/create", host)
	resp, err := http.Get(httpURL)
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
	roomID := string(link)

	fmt.Printf("\nLive Link: http://localhost:8080/?room=%s\n", roomID)
	fmt.Println("--- STREAMING LOGS TO THE WEB ---")
	wsURL := fmt.Sprintf("ws://%s/stream?room=%s", host, roomID)
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		fmt.Println("Error connecting to server stream:", err)
		os.Exit(1)
	}
	defer wsConn.Close() // Hang up when the program exits

	// 3. The Pipe Loop
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

		err = wsConn.WriteMessage(websocket.TextMessage, buf[:n])
		if err != nil {
			fmt.Println("\nServer disconnected.")
			break
		}
	}
}
