package ipfs

import (
	"context"
	"fmt"
	"io"
	"path"
	"bytes"
	"io/ioutil"
	"strings"
	"net"
	"encoding/json"
	"log"

	files "github.com/ipfs/go-ipfs-files"
	shell "github.com/ipfs/go-ipfs-api"
	storagedriver "github.com/docker/distribution/registry/storage/driver"

	dcontext "github.com/docker/distribution/context"

	"github.com/docker/distribution/reference"

	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const (
	driverName           = "ipfs"
	defaultRootDirectory = "/var/lib/registry"
	defaultMaxThreads    = uint64(100)

	// minThreads is the minimum value for the maxthreads configuration
	// parameter. If the driver's parameters are less than this we set
	// the parameters to minThreads
	minThreads = uint64(25)
)

// DriverParameters represents all configuration options available for the
// filesystem driver
type DriverParameters struct {
	RootDirectory string
	MaxThreads    uint64
	Direction     string
	Address string
}

func init() {
	factory.Register(driverName, &filesystemDriverFactory{})
}

// filesystemDriverFactory implements the factory.StorageDriverFactory interface
type filesystemDriverFactory struct{}

func (factory *filesystemDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {

	return FromParameters(parameters)
}

type driver struct {
	rootDirectory string
	address string
	direction string
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by a local
// filesystem. All provided paths will be subpaths of the RootDirectory.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Optional Parameters:
// - rootdirectory
// - maxthreads
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	params, err := fromParametersImpl(parameters)

	if err != nil || params == nil {
		return nil, err
	}

	return New(*params), nil
}

func fromParametersImpl(parameters map[string]interface{}) (*DriverParameters, error) {
	var (
		err           error
		maxThreads    = defaultMaxThreads
		rootDirectory = defaultRootDirectory
		direction = "none"
		address = ""
	)

	if parameters != nil {
		if rootDir, ok := parameters["rootdirectory"]; ok {
			rootDirectory = fmt.Sprint(rootDir)
		}

		if dir, ok := parameters["direction"]; ok {
			direction = fmt.Sprint(dir)
		}

		if addr, ok := parameters["address"]; ok {
			address = fmt.Sprint(addr)
		}

		maxThreads, err = base.GetLimitFromParameter(parameters["maxthreads"], minThreads, defaultMaxThreads)
		if err != nil {
			return nil, fmt.Errorf("maxthreads config error: %s", err.Error())
		}
	}

	params := &DriverParameters{
		RootDirectory: rootDirectory,
		MaxThreads:    maxThreads,
		Direction: direction,
		Address: address,
	}
	return params, nil
}

// New constructs a new Driver with a given rootDirectory
func New(params DriverParameters) *Driver {

	fsDriver := &driver {
		rootDirectory: params.RootDirectory,
		direction: params.Direction,
		address: params.Address,
	}

	log.Printf("Using IPFS Address: '%s'", fsDriver.address);

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: base.NewRegulator(fsDriver, params.MaxThreads),
			},
		},
	}
}

// Implement the storagedriver.StorageDriver interface
func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, subPath string) ([]byte, error) {

	getLogger(ctx).Infof("GetContent: '%s'", subPath);

	domain, err := getDomain(ctx)

	if err != nil {
		return nil, err
	}

	localPath := path.Join("/", domain, subPath);

	sh, err := d.getShell();

	if err != nil {
		return nil, err
	}

	switch d.direction {
		case "in":
			ipfsPath, subErr := d.getIpfsAddressFromDomain(domain);

			if subErr != nil {
				return nil, subErr;
			}

			pp := path.Join(ipfsPath, subPath)
			getLogger(ctx).Infof("Requesting: '%s'", pp);
			p, subErr := ipfsCat(sh, pp);

			if subErr != nil {

				switch subErr.(type) {

					case storagedriver.PathNotFoundError:
						return nil, storagedriver.PathNotFoundError{
							DriverName: "ipfs",
							Path: localPath,
						}
					default:
						return nil, err;
				}
			}

			return p, nil;

		case "out":
			p, err := filesRead(sh, localPath);

			if err != nil {
				return nil, err
			}

			return p, err
		default:
			return nil, nil;
	}
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, subPath string, contents []byte) error {
	
	getLogger(ctx).Infof("Putting %s", subPath);

	domain, err := getDomain(ctx)

	if err != nil {
		return err
	}

	localPath := path.Join("/", domain, subPath);

	sh, err := d.getShell();
	
	if err != nil {
		return err
	}

	_, err = filesWrite(sh, localPath, contents, false)

	return err;
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, subPath string, offset int64) (io.ReadCloser, error) {
	
	getLogger(ctx).Infof("Creating Reader: '%s'", subPath);
	
	sh, err := d.getShell()

	domain, err := getDomain(ctx)

	if err != nil {
		return nil, err
	}

	localPath := path.Join("/", domain, subPath);

	switch d.direction {
		case "in":
			ipfsPath, err := d.getIpfsAddressFromDomain(domain);

			if err != nil {
				return nil, err;
			}

			pp := path.Join(ipfsPath, subPath)

			getLogger(ctx).Infof("Requesting: '%s'", pp);

			p, err := ipfsCatWithOffset(sh, pp, offset)
			
			if err != nil {
				return nil, err
			}

			closer := ioutil.NopCloser(bytes.NewReader(p));
			
			return closer, nil;

		case "out":
			p, err := filesReadWithOffset(sh, localPath, offset);

			if err != nil {
				return nil, err
			}

			closer := ioutil.NopCloser(bytes.NewReader(p));

			return closer, nil

		default:
			return nil, nil
	}
}

func (d *driver) Writer(ctx context.Context, subPath string, append bool) (storagedriver.FileWriter, error) {
	
	domain, err := getDomain(ctx)

	if err != nil {
		return nil, err
	}

	localPath := path.Join("/", domain, subPath);

	sh, err := d.getShell();

	if err != nil {
		return nil, err;
	}

	return newIpfsWriter(sh, localPath, append), nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {
	
	getLogger(ctx).Infof("Stating: '%s'", subPath);

	if subPath == "/" {
		return storagedriver.FileInfoInternal {
			FileInfoFields: storagedriver.FileInfoFields{
				Path: subPath,
				Size: 0,
				IsDir: true,
			},
		}, nil
	}

	domain, err := getDomain(ctx)

	if err != nil {
		return nil, err
	}

	localPath := path.Join("/", domain, subPath);

	sh, _ := d.getShell();

	if err != nil {
		return nil, err
	}

	var file shell.LsLink

	switch d.direction {
		case "in":
			ipfsPath, err := d.getIpfsAddressFromDomain(domain);

			if err != nil {
				return nil, err;
			}

			s, err := sh.ObjectStat(path.Join(ipfsPath, subPath));
			
			if err != nil {
				e := err.Error()
				
				if strings.HasPrefix(e, "object/stat: no link named") {
					return nil, storagedriver.PathNotFoundError {
						Path: localPath,
					}
				}

				return nil, err;
			}

			file = shell.LsLink{
				Hash: s.Hash,
			}
			
		case "out":
			file, err = filesStat(sh, localPath);

			if err != nil {
				return nil, err
			}
		default:
			return nil, nil;
	}

	isDir := false;

	if (file.Type == 1) {
		isDir = true;
	}

	return storagedriver.FileInfoInternal {
		FileInfoFields: storagedriver.FileInfoFields{
			Path: subPath,
			Size: int64(file.Size),
			IsDir: isDir,
		},
	}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
	
	getLogger(ctx).Infof("Listing %s", subPath);

	domain, err := getDomain(ctx)

	if err != nil {
		return nil, err
	}

	localPath := path.Join("/", domain, subPath);

	sh, err := d.getShell();

	if err != nil {
		return nil, err;
	}

	files, err := filesLs(sh, localPath);

	keys := make([]string, 0, len(files))

	for _, fileName := range files {

		keys = append(keys, path.Join(subPath, fileName.Name))
	}

	return keys, nil;
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	
	domain, err := getDomain(ctx)

	if err != nil {
		return err
	}

	localSourcePath := path.Join("/", domain, sourcePath);
	localDestPath := path.Join("/", domain, destPath);

	getLogger(ctx).Infof("Moving %s to %s", localSourcePath, localDestPath);

	sh, err := d.getShell()

	if err != nil {
		return err;
	}

	return filesMove(sh, localSourcePath, localDestPath);
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, subPath string) error {

	getLogger(ctx).Infof("Deleting %s", subPath);
	
	domain, err := getDomain(ctx)

	if err != nil {
		return err
	}

	localPath := path.Join("/", domain, subPath);

	sh, err := d.getShell()

	if err != nil {
		return err;
	}

	return filesRm(sh, localPath);
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod{}
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, subPath string, f storagedriver.WalkFn) error {
	
	domain, err := getDomain(ctx)

	if err != nil {
		return err
	}

	localPath := path.Join("/", domain, subPath);

	return storagedriver.WalkFallback(ctx, d, localPath, f)
}

type lsLinks struct {
	Hash string
	Name string
	Size uint64
	Type string
}

func filesMkdir(sh *shell.Shell, dir string) (error) {

	resp, err := sh.Request("files/mkdir", dir).Option("parents", true).Send(context.Background());

	if err != nil {
		return err
	}

	defer resp.Close()

	if (resp.Error != nil) {

		return resp.Error
	}

	return err;
}

func ipfsCat(sh *shell.Shell, path string) ([]byte, error) {

	return ipfsCatWithOffset(sh, path, 0);
}

func ipfsCatWithOffset(sh *shell.Shell, path string, offset int64) ([]byte, error) {

	closer, err := sh.Cat(path);

	if err != nil {

		e := err.Error()

		if strings.HasPrefix(e, "cat: no link named") {

			return nil, storagedriver.PathNotFoundError {
				DriverName: "ipfs",
				Path: path,
			}
		} else if strings.HasPrefix(e, "error resolving upload: blob upload unknown") {

			return nil, err;
		}

		return nil, err;
	}

	p, err := ioutil.ReadAll(closer);

	return p, err;
}


func filesRead(sh *shell.Shell, path string) ([]byte, error) {
	
	return filesReadWithOffset(sh, path, 0);
}

func filesReadWithOffset(sh *shell.Shell, path string, offset int64) ([]byte, error) {

	resp, err := sh.Request("files/read", path).Option("offset", offset).Send(context.Background());

	if err != nil {
		return nil, err
	}

	defer resp.Close()

	if (resp.Error != nil) {

		if resp.Error.Message == "file does not exist" {

			return nil, storagedriver.PathNotFoundError {
				DriverName: "ipfs",
				Path: path,
			}
		}

		return nil, resp.Error
	}

	p, err := ioutil.ReadAll(resp.Output);

	if (err != nil) {
		return nil, err;
	}

	return p, err;
}

func getLogger(ctx context.Context) dcontext.Logger {

	return dcontext.GetLoggerWithFields(ctx, map[interface{}]interface{}{
		"driver": "ipfs",
	});
}


func (d* driver) getIpfsAddressFromDomain(domain string) (string, error) {

	txtrecords, err := net.LookupTXT(domain)

	if err != nil {
		return "", err
	}

	hasHash := false;

	for _, txt := range txtrecords {

		if strings.HasPrefix(txt, "ipfsnode") {

			var out []string

			txt = strings.Replace(txt, "ipfsnode=", "", 1)

			json.Unmarshal([]byte(txt), &out);

			sh, err := d.getShell();

			if err != nil {
				return "", nil
			}

			for _, address := range out {

				err = swarmConnect(sh, address)

				if err != nil {
					return "", err
				}
			}
		}

		if strings.HasPrefix(txt, "dnslink") {
			hasHash = true
		}
	}

	if hasHash == false {
		return "", nil
	}

	return "/ipns/" + domain, nil;
}

func getDomain(ctx context.Context) (string, error) {

	name := dcontext.GetStringValue(ctx, "name");

	if (name == "") {

		return "", nil;
	}

	ref, err := reference.ParseNamed(name)

	if err != nil {

		return "", err;
	}

	domain := reference.Domain(ref);

	return domain, nil;
}

func (d* driver) getShell() (*shell.Shell, error) {

	sh := shell.NewShell(d.address);

	return sh, nil;
}

func doesFileExistLocally(sh *shell.Shell, subPath string) (bool, error) {

	// If this doesn't exist, we move into IPFS mode
	_, err := filesStat(sh, subPath);

	if err != nil {
		switch err.(type) {

			case storagedriver.PathNotFoundError:
				return false, nil;
			default:
				return false, err;
		}
	}

	return true, nil;
}

// FilesStat gets file info
func filesStat(sh *shell.Shell, path string) (shell.LsLink, error) {

	var out struct { lsLinks }

	err := sh.Request("files/stat", path).Exec(context.Background(), &out)

	if err != nil {

		e := err.Error();

		if e == "files/stat: file does not exist" {
			return shell.LsLink{}, storagedriver.PathNotFoundError {
				Path: path,
			}
		}

		return shell.LsLink{}, err
	}

	t := 0;

	if out.Type == "directory" {
		t = 1;
	}

	return shell.LsLink{
		Hash: out.Hash,
		Size: out.Size,
		Name: out.Name,
		Type: t,
	}, nil;
}

// filesRm removes a file
func filesRm(sh *shell.Shell, path string) (error) {

	resp, err := sh.Request("files/rm", path).Option("recursive", true).Send(context.Background());

	
	if err != nil {
		return err
	}

	defer resp.Close()

	return nil;
}

// filesMove moves a file
func filesMove(sh *shell.Shell, source string, destination string) (error) {

	err := filesMkdir(sh, path.Dir(destination));

	if err != nil {
		return err;
	}

	resp, err := sh.Request("files/mv", source, destination).Send(context.Background());

	if err != nil {
		return err
	}

	if (resp.Error != nil) {

		if resp.Error.Message == "file does not exist" {

			return storagedriver.PathNotFoundError {
				Path: source,
			} 
		}
	}

	defer resp.Close()

	return nil;
}

// FilesWrite writes to a file
func filesWrite(sh *shell.Shell, path string, p []byte, append bool) (int, error) {

	var offset uint64

	if (append) {

		size, err := filesStat(sh, path);

		if err != nil {

			switch err := err.(type) {

				case storagedriver.PathNotFoundError:
					
				default:
					return -1, err
			}
		}

		offset = size.Size;
	}

	fileReader := files.NewReaderFile(bytes.NewReader(p));

	sliceDirectory := files.NewSliceDirectory([]files.DirEntry{files.FileEntry("", fileReader)})

	multiFileReader := files.NewMultiFileReader(sliceDirectory, true)

	resp, err := sh.Request("files/write", path).Option("create", true).Option("offset", offset).Option("parents", true).Body(multiFileReader).Send(context.Background());

	if err != nil {
		return -1, err
	}

	defer resp.Close()

	return len(p), err;
}

// filesLs gets a list of files in a directory
func filesLs(sh *shell.Shell, path string) ([]shell.LsLink, error) {

	var out struct { Entries []shell.LsLink }

	err := sh.Request("files/ls", path).Exec(context.Background(), &out)

	if err != nil {
		return nil, err
	}

	return out.Entries, nil
}

func swarmConnect(sh *shell.Shell, address string) (error) {

	var out struct { Strings []string }

	err := sh.Request("swarm/connect", address).Exec(context.Background(), &out)

	if err != nil {
		return err
	}

	return nil
}