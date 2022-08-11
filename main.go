package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	f, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	cids := bytes.Split(f, []byte("\n"))
	if err = get(context.Background(), cids); err != nil {
		fmt.Println(err)
		return
	}
}

func get(ctx context.Context, cids [][]byte) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(100)

	errf, err := os.Create("cid.failed")
	if err != nil {
		return fmt.Errorf("failed to create failed file: %w", err)
	}

	for i := range cids {
		if len(cids[i]) == 0 {
			continue
		}
		cid := string(cids[i])
		g.Go(func() error {
			cid := cid
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			if out, err := exec.CommandContext(ctx, "ipfs", "pin", "add", cid).CombinedOutput(); err != nil {
				errf.Write([]byte(fmt.Sprintln(cid)))
				if exiterr, ok := err.(*exec.ExitError); ok {
					return fmt.Errorf("ipfs pin add error: %w; out: %s; cid: %s", exiterr.Stderr, out, cid)
				}
				return fmt.Errorf("ipfs pin add error: %w", err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("errgroup wait failed: %w", err)
	}
	return nil
}
