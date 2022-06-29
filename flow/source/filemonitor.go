package source

import (
	"fmt"
	"goflow/flow"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)
type FileEntry struct {
	path string
	ts time.Time
}

type FileMonitor[R any] struct {
	root          string
	pattern       string
	recursive     bool
	readerFactory func(path string) flow.Reader[R]
	checkTicker   *time.Ticker
	snapshot map[string]bool  // map[path]true
	files chan string
	activeReader flow.Reader[R]
}

func NewFileMonitor[R any](path string, globPattern string, recursive bool, interval time.Duration, readerFactory func(path string) flow.Reader[R]) *FileMonitor[R] {
	fi, err := os.Stat(path)
	if err != nil {
		log.Fatal(err)
	}
	if !fi.IsDir() {
		log.Fatalf("root: %s is not a directory", path)
	}
	if _, err := filepath.Match(globPattern, ""); err != nil {
		log.Fatalf("Invalid pattern: %s (%v)", globPattern, err)
	}
	fm := FileMonitor[R]{root: path,
		pattern:     globPattern,
		recursive:   recursive,
		readerFactory: readerFactory,
		checkTicker: time.NewTicker(interval),
		snapshot: make(map[string]bool),
		files: make(chan string),
	}
	go func() {
		for t := range fm.checkTicker.C {
			fm.scan(t)
		}
	}()
	return &fm
}

func (fm *FileMonitor[R]) scan(t time.Time) {
	var newFiles []FileEntry
	newSnapshot := make(map[string]bool)
	filepath.Walk(fm.root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Println("Error when scanning for files:", err)
			return err
		}
		if !fm.recursive && info.IsDir() {
			return filepath.SkipDir
		}
		if info.Mode().IsRegular() {
			if match, _ := filepath.Match(fm.pattern, info.Name()); match {
				newSnapshot[path] = true
				if !fm.snapshot[path] {
					newFiles = append(newFiles, FileEntry{path, info.ModTime()})
				}
			}
		}
		return nil
	})
	// sort new file by modification time
	sort.Slice(newFiles, func(i, j int) bool {
		return newFiles[i].ts.Before(newFiles[j].ts)
	})
	for _, fi := range newFiles {
		log.Printf("Found new file: %s\n", fi.path)
		fm.files <- fi.path
	}
	fmt.Println("-- scan finished --")
	fm.snapshot = newSnapshot
}

func (fm *FileMonitor[R]) Read(ctx *flow.Context) (R, bool) {
	
	if fm.activeReader == nil {
		// get new reader, block until new file available
		path := <-fm.files
		log.Printf("Opening reader for file: %s", path)
		fm.activeReader = fm.readerFactory(path)
	}
	record, ok := fm.activeReader.Read(ctx)
	if ok {
		return record, true
	} else {
		// if no more records for this reader then get a new one
		log.Println("Closing active reader.")
		fm.activeReader.Close()
		fm.activeReader = nil
		return fm.Read(ctx)
	}
}
func (fm *FileMonitor[R]) Close() {
	fm.activeReader.Close()
}