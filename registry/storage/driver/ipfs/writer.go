package ipfs

import (
	"fmt"
	"log"

	shell "github.com/ipfs/go-ipfs-api"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

type ipfsWriter struct {
	shell      *shell.Shell
	append    bool
	rootPath string
	subPath		string
	// bw        *bufio.Writer
	closed    bool
	committed bool
	cancelled bool
}

func newIpfsWriter(shell *shell.Shell, rootPath string, subPath string, append bool) *ipfsWriter {

	log.Printf("Opened Writer for " + subPath);

	return &ipfsWriter{
		shell: shell,
		rootPath: rootPath,
		subPath: subPath,
		append: append,
	}
}

func (fw *ipfsWriter) Write(p []byte) (int, error) {

	if fw.closed {
		return 0, fmt.Errorf("already closed")
	} else if fw.committed {
		return 0, fmt.Errorf("already committed")
	} else if fw.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	l, err := filesWrite(fw.shell, fw.subPath, p, true);

	return l, err
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

	return int64(file.Size);
}

func (fw *ipfsWriter) Close() error {

	if fw.closed {
		return fmt.Errorf("already closed")
	}

	fw.closed = true

	return nil
}

func (fw *ipfsWriter) Cancel() error {

	if fw.closed {
		return fmt.Errorf("already closed")
	}

	fw.cancelled = true
	
	return nil;
}

func (fw *ipfsWriter) Commit() error {

	if fw.closed {
		return fmt.Errorf("already closed")
	} else if fw.committed {
		return fmt.Errorf("already committed")
	} else if fw.cancelled {
		return fmt.Errorf("already cancelled")
	}

	fw.committed = true

	return nil
}
