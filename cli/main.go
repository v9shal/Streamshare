package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultHost  = "localhost:8080"
	httpTimeout  = 10 * time.Second
	writeWait    = 10 * time.Second
	stdinBufSize = 4096
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "streamshare:", err)
		os.Exit(1)
	}
}

func run() error {
	if err := requirePipedStdin(); err != nil {
		return err
	}

	host := os.Getenv("STREAMSHARE_HOST")
	if host == "" {
		host = defaultHost
	}

	roomID, err := createRoom(host)
	if err != nil {
		return fmt.Errorf("creating room: %w", err)
	}

	fmt.Printf("Live link: http://%s/?room=%s\n", host, roomID)
	fmt.Println("--- streaming stdin to the web (Ctrl+C to stop) ---")

	conn, err := dialStream(host, roomID)
	if err != nil {
		return fmt.Errorf("connecting stream: %w", err)
	}
	defer conn.Close()

	return pipeStdinToSocket(conn)
}
func requirePipedStdin() error {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return fmt.Errorf("checking stdin: %w", err)
	}
	if fi.Mode()&os.ModeCharDevice != 0 {
		return errors.New("no piped input detected; example: echo 'hello' | streamshare")
	}
	return nil
}
func createRoom(host string) (string, error) {
	client := &http.Client{Timeout: httpTimeout}
	resp, err := client.Get(fmt.Sprintf("http://%s/create", host))
	if err != nil {
		return "", fmt.Errorf("reaching server at %s: %w", host, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	roomID := strings.TrimSpace(string(body))
	if roomID == "" {
		return "", errors.New("server returned an empty room id")
	}
	return roomID, nil
}

func dialStream(host, roomID string) (*websocket.Conn, error) {
	wsURL := fmt.Sprintf("ws://%s/stream?room=%s", host, roomID)
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("%w (http status %s)", err, resp.Status)
		}
		return nil, err
	}
	return conn, nil
}

func pipeStdinToSocket(conn *websocket.Conn) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	done := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(os.Stdin)
		buf := make([]byte, stdinBufSize)
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				conn.SetWriteDeadline(time.Now().Add(writeWait))
				if werr := conn.WriteMessage(websocket.TextMessage, buf[:n]); werr != nil {
					done <- fmt.Errorf("server disconnected: %w", werr)
					return
				}
			}
			if errors.Is(err, io.EOF) {
				done <- nil
				return
			}
			if err != nil {
				done <- fmt.Errorf("reading stdin: %w", err)
				return
			}
		}
	}()

	select {
	case err := <-done:
		closeGracefully(conn)
		return err
	case <-ctx.Done():
		fmt.Println("\ninterrupted, closing stream...")
		closeGracefully(conn)
		return nil
	}
}

func closeGracefully(conn *websocket.Conn) {
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(writeWait),
	)
}
