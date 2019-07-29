package ipfs

import (
	"bytes"
	"fmt"
	"io"
	"log"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	shell "github.com/ipfs/go-ipfs-api"
)

type ipfsWriter struct {
	shell   *shell.Shell
	append  bool
	subPath string
	closed    bool
	committed bool
	cancelled bool
	offset    int64
	stream    *bytes.Buffer
	resp      *shell.Response
	writer    io.Writer
}

func newIpfsWriter(shell *shell.Shell, subPath string, append bool) (*ipfsWriter, error) {

	log.Printf("Opened Writer for " + subPath)

	writer := &ipfsWriter{
		shell:   shell,
		subPath: subPath,
		append:  append,
		offset:  0,
	}

	// Make sure the file exists
	_, err := writer.Write(make([]byte, 0))

	if err != nil {
		return nil, err
	}

	// If we are appending, find the end.
	if append {

		writer.offset = writer.Size()
	}

	return writer, nil
}

func (fw *ipfsWriter) Write(p []byte) (int, error) {

	if fw.closed {
		return 0, fmt.Errorf("already closed")
	} else if fw.committed {
		return 0, fmt.Errorf("already committed")
	} else if fw.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	bytesWritten, err := filesWrite(fw.shell, fw.subPath, p, false, fw.offset, false)

	fw.offset += int64(bytesWritten)

	return bytesWritten, err
}

func (fw *ipfsWriter) Size() int64 {

	file, err := filesStat(fw.shell, fw.subPath)

	if err != nil {

		switch err.(type) {

		case storagedriver.PathNotFoundError:
			return 0
		default:
			return -1
		}
	}

	return int64(file.Size)
}

func (fw *ipfsWriter) Close() error {

	if fw.closed {
		return fmt.Errorf("already closed")
	}

	log.Printf("Closing and flushing %s", fw.subPath)

	filesFlush(fw.shell, fw.subPath)

	fw.closed = true

	return nil
}

func (fw *ipfsWriter) Cancel() error {

	if fw.closed {
		return fmt.Errorf("already closed")
	}

	err := fw.resp.Close()

	if err != nil {
		return err
	}

	filesFlush(fw.shell, fw.subPath)

	fw.cancelled = true

	return nil
}

func (fw *ipfsWriter) Commit() error {

	if fw.closed {
		return fmt.Errorf("already closed")
	} else if fw.committed {
		return fmt.Errorf("already committed")
	} else if fw.cancelled {
		return fmt.Errorf("already cancelled")
	}

	filesFlush(fw.shell, fw.subPath)

	fw.committed = true

	return nil
}
