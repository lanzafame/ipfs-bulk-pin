package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	f, err := os.OpenFile(os.Args[1], os.O_RDWR, 0755)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer f.Close()

	start, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("start value couldn't be parsed")
		return
	}
	end, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println("end value couldn't be parsed")
		return
	}

	seclen := (end - start) * 47
	section := make([]byte, seclen)
	n, err := f.ReadAt(section, int64(start*47))
	if err != nil {
		fmt.Println("failed to readat: %v", err)
		return
	}
	fmt.Printf("read %v bytes; read %v lines\n", n, n/47)

	cids := bytes.Split(section, []byte("\n"))
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

			if err := exec.CommandContext(ctx, "ipfs", "pin", "add", cid).Run(); err != nil {
				if _, err = errf.Write([]byte(fmt.Sprintln(cid))); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("errgroup wait failed: %w", err)
	}
	return nil
}
