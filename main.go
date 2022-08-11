package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"golang.org/x/sync/errgroup"
)

func main() {
	goroutines, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("goroutines value couldn't be parsed")
		return
	}

	f, err := os.OpenFile(os.Args[2], os.O_RDWR, 0755)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer f.Close()

	start, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println("start value couldn't be parsed")
		return
	}
	end, err := strconv.Atoi(os.Args[4])
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
	if err = get(context.Background(), goroutines, cids); err != nil {
		fmt.Println(err)
		return
	}
}

func get(ctx context.Context, goroutines int, cids [][]byte) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(goroutines)

	errf, err := os.OpenFile("cid.failed", os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return fmt.Errorf("failed to create failed file: %w", err)
	}

	for i := range cids {
		if i%500 == 0 {
			fmt.Println("up to cid ", i)
		}
		if len(cids[i]) == 0 {
			continue
		}
		cid := string(cids[i])
		g.Go(func() error {
			cid := cid
			if err := exec.CommandContext(ctx, "timeout", "5", "ipfs", "pin", "add", cid).Run(); err != nil {
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
